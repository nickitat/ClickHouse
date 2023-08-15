#include <atomic>
#include <memory>
#include <mutex>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>

#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3/copyS3File.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>

#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/MultiVersion.h>
#include <Common/Macros.h>

#include <IO/ReadBufferFromString.h>


namespace ProfileEvents
{
    extern const Event S3DeleteObjects;
    extern const Event S3ListObjects;
    extern const Event DiskS3DeleteObjects;
    extern const Event DiskS3ListObjects;
}

namespace CurrentMetrics
{
    extern const Metric ObjectStorageS3Threads;
    extern const Metric ObjectStorageS3ThreadsActive;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename Result, typename Error>
void throwIfError(const Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw S3Exception(fmt::format("{} (Code: {})", err.GetMessage(), static_cast<size_t>(err.GetErrorType())), err.GetErrorType());
    }
}

template <typename Result, typename Error>
void throwIfUnexpectedError(const Aws::Utils::Outcome<Result, Error> & response, bool if_exists)
{
    /// In this case even if absence of key may be ok for us,
    /// the log will be polluted with error messages from aws sdk.
    /// Looks like there is no way to suppress them.

    if (!response.IsSuccess() && (!if_exists || !S3::isNotFoundError(response.GetError().GetErrorType())))
    {
        const auto & err = response.GetError();
        throw S3Exception(err.GetErrorType(), "{} (Code: {})", err.GetMessage(), static_cast<size_t>(err.GetErrorType()));
    }
}

template <typename Result, typename Error>
void logIfError(const Aws::Utils::Outcome<Result, Error> & response, std::function<String()> && msg)
{
    try
    {
        throwIfError(response);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, msg());
    }
}

}

namespace
{

class S3IteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    S3IteratorAsync(
        const std::string & bucket,
        const std::string & path_prefix,
        std::shared_ptr<const S3::Client> client_,
        size_t max_list_size)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageS3Threads,
            CurrentMetrics::ObjectStorageS3ThreadsActive,
            "ListObjectS3")
        , client(client_)
    {
        request.SetBucket(bucket);
        request.SetPrefix(path_prefix);
        request.SetMaxKeys(static_cast<int>(max_list_size));
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);

        bool result = false;
        auto outcome = client->ListObjectsV2(request);
        /// Outcome failure will be handled on the caller side.
        if (outcome.IsSuccess())
        {
            auto objects = outcome.GetResult().GetContents();

            result = !objects.empty();

            for (const auto & object : objects)
                batch.emplace_back(object.GetKey(), ObjectMetadata{static_cast<uint64_t>(object.GetSize()), Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()), {}});

            if (result)
                request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());

            return result;
        }

        throw Exception(ErrorCodes::S3_ERROR, "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                quoteString(request.GetBucket()), quoteString(request.GetPrefix()),
                backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
    }

    std::shared_ptr<const S3::Client> client;
    S3::ListObjectsV2Request request;
};

}

bool S3ObjectStorage::exists(const StoredObject & object) const
{
    auto settings_ptr = s3_settings.get();
    return S3::objectExists(*clients.get()->client, bucket, object.remote_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
}

thread_local bool flag = false;

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t> file_size) const
{
    ReadSettings disk_read_settings = patchSettings(read_settings);
    auto global_context = Context::getGlobalContextInstance();

    auto settings_ptr = s3_settings.get();

    auto read_buffer_creator =
        [this, settings_ptr, disk_read_settings]
        (const std::string & path, size_t read_until_position) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromS3>(
            clients.get()->client,
            bucket,
            path,
            version_id,
            settings_ptr->request_settings,
            disk_read_settings,
            /* use_external_buffer */true,
            /* offset */0,
            read_until_position,
            /* restricted_seek */true);
    };

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
        return std::make_unique<ReadBufferFromRemoteFSGather>(
            std::move(read_buffer_creator),
            objects,
            disk_read_settings,
            global_context->getFilesystemCacheLog(),
            /* use_external_buffer */ flag,
            file_size);
        }
        case RemoteFSReadMethod::threadpool:
        {
        auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
            std::move(read_buffer_creator),
            objects,
            disk_read_settings,
            global_context->getFilesystemCacheLog(),
            /* use_external_buffer */ true,
            file_size);

        auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
        return std::make_unique<AsynchronousBoundedReadBuffer>(
            std::move(impl),
            reader,
            disk_read_settings,
            global_context->getAsyncReadCounters(),
            global_context->getFilesystemReadPrefetchesLog());
        }
    }
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    auto settings_ptr = s3_settings.get();
    return std::make_unique<ReadBufferFromS3>(
        clients.get()->client,
        bucket,
        object.remote_path,
        version_id,
        settings_ptr->request_settings,
        patchSettings(read_settings));
}

std::unique_ptr<WriteBufferFromFileBase> S3ObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode, // S3 doesn't support append, only rewrite
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    WriteSettings disk_write_settings = IObjectStorage::patchSettings(write_settings);

    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "S3 doesn't support append to files");

    auto settings_ptr = s3_settings.get();
    ThreadPoolCallbackRunner<void> scheduler;
    if (write_settings.s3_allow_parallel_part_upload)
        scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "VFSWrite");

    auto clients_ = clients.get();
    return std::make_unique<WriteBufferFromS3>(
        clients_->client,
        clients_->client_with_long_timeout,
        bucket,
        object.remote_path,
        buf_size,
        settings_ptr->request_settings,
        attributes,
        std::move(scheduler),
        disk_write_settings);
}


ObjectStorageIteratorPtr S3ObjectStorage::iterate(const std::string & path_prefix) const
{
    auto settings_ptr = s3_settings.get();
    auto client_ptr = clients.get()->client;

    return std::make_shared<S3IteratorAsync>(bucket, path_prefix, client_ptr, settings_ptr->list_object_keys_size);
}

void S3ObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, int max_keys) const
{
    auto settings_ptr = s3_settings.get();
    auto client_ptr = clients.get()->client;

    S3::ListObjectsV2Request request;
    request.SetBucket(bucket);
    request.SetPrefix(path);
    if (max_keys)
        request.SetMaxKeys(max_keys);
    else
        request.SetMaxKeys(settings_ptr->list_object_keys_size);

    Aws::S3::Model::ListObjectsV2Outcome outcome;
    do
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);
        outcome = client_ptr->ListObjectsV2(request);
        throwIfError(outcome);

        auto result = outcome.GetResult();
        auto objects = result.GetContents();

        if (objects.empty())
            break;

        for (const auto & object : objects)
            children.emplace_back(object.GetKey(), ObjectMetadata{static_cast<uint64_t>(object.GetSize()), Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()), {}});

        if (max_keys)
        {
            int keys_left = max_keys - static_cast<int>(children.size());
            if (keys_left <= 0)
                break;
            request.SetMaxKeys(keys_left);
        }

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void S3ObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    auto client_ptr = clients.get()->client;

    ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
    ProfileEvents::increment(ProfileEvents::DiskS3DeleteObjects);
    S3::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(object.remote_path);
    auto outcome = client_ptr->DeleteObject(request);

    throwIfUnexpectedError(outcome, if_exists);

    LOG_TRACE(log, "Object with path {} was removed from S3", object.remote_path);
}

void S3ObjectStorage::removeObjectsImpl(const StoredObjects & objects, bool if_exists)
{
    if (objects.empty())
        return;

    if (!s3_capabilities.support_batch_delete)
    {
        for (const auto & object : objects)
            removeObjectImpl(object, if_exists);
    }
    else
    {
        auto client_ptr = clients.get()->client;
        auto settings_ptr = s3_settings.get();

        size_t chunk_size_limit = settings_ptr->objects_chunk_size_to_delete;
        size_t current_position = 0;

        while (current_position < objects.size())
        {
            std::vector<Aws::S3::Model::ObjectIdentifier> current_chunk;
            String keys;
            for (; current_position < objects.size() && current_chunk.size() < chunk_size_limit; ++current_position)
            {
                Aws::S3::Model::ObjectIdentifier obj;
                obj.SetKey(objects[current_position].remote_path);
                current_chunk.push_back(obj);

                if (!keys.empty())
                    keys += ", ";
                keys += objects[current_position].remote_path;
            }

            Aws::S3::Model::Delete delkeys;
            delkeys.SetObjects(current_chunk);

            ProfileEvents::increment(ProfileEvents::S3DeleteObjects);
            ProfileEvents::increment(ProfileEvents::DiskS3DeleteObjects);
            S3::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);
            auto outcome = client_ptr->DeleteObjects(request);

            throwIfUnexpectedError(outcome, if_exists);

            LOG_TRACE(log, "Objects with paths [{}] were removed from S3", keys);
        }
    }
}

void S3ObjectStorage::removeObject(const StoredObject & object)
{
    removeObjectImpl(object, false);
}

void S3ObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectImpl(object, true);
}

void S3ObjectStorage::removeObjects(const StoredObjects & objects)
{
    removeObjectsImpl(objects, false);
}

void S3ObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    removeObjectsImpl(objects, true);
}

std::optional<ObjectMetadata> S3ObjectStorage::tryGetObjectMetadata(const std::string & path) const
{
    auto settings_ptr = s3_settings.get();
    auto object_info = S3::getObjectInfo(*clients.get()->client, bucket, path, {}, settings_ptr->request_settings, /* with_metadata= */ true, /* for_disk_s3= */ true, /* throw_on_error= */ false);

    if (object_info.size == 0 && object_info.last_modification_time == 0 && object_info.metadata.empty())
        return {};

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.last_modified = object_info.last_modification_time;
    result.attributes = object_info.metadata;

    return result;
}

ObjectMetadata S3ObjectStorage::getObjectMetadata(const std::string & path) const
{
    auto settings_ptr = s3_settings.get();
    auto object_info = S3::getObjectInfo(*clients.get()->client, bucket, path, {}, settings_ptr->request_settings, /* with_metadata= */ true, /* for_disk_s3= */ true);

    ObjectMetadata result;
    result.size_bytes = object_info.size;
    result.last_modified = object_info.last_modification_time;
    result.attributes = object_info.metadata;

    return result;
}

void S3ObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    /// Shortcut for S3
    if (auto * dest_s3 = dynamic_cast<S3ObjectStorage * >(&object_storage_to); dest_s3 != nullptr)
    {
        auto client_ptr = clients.get()->client;
        auto settings_ptr = s3_settings.get();
        auto size = S3::getObjectSize(*client_ptr, bucket, object_from.remote_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
        auto scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "S3ObjStor_copy");
        copyS3File(client_ptr, bucket, object_from.remote_path, 0, size, dest_s3->bucket, object_to.remote_path,
                   settings_ptr->request_settings, object_to_attributes, scheduler, /* for_disk_s3= */ true);
    }
    else
    {
        IObjectStorage::copyObjectToAnotherObjectStorage(object_from, object_to, object_storage_to, object_to_attributes);
    }
}

void S3ObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from, const StoredObject & object_to, std::optional<ObjectAttributes> object_to_attributes)
{
    auto client_ptr = clients.get()->client;
    auto settings_ptr = s3_settings.get();
    auto size = S3::getObjectSize(*client_ptr, bucket, object_from.remote_path, {}, settings_ptr->request_settings, /* for_disk_s3= */ true);
    auto scheduler = threadPoolCallbackRunner<void>(getThreadPoolWriter(), "S3ObjStor_copy");
    copyS3File(client_ptr, bucket, object_from.remote_path, 0, size, bucket, object_to.remote_path,
               settings_ptr->request_settings, object_to_attributes, scheduler, /* for_disk_s3= */ true);
}

void S3ObjectStorage::setNewSettings(std::unique_ptr<S3ObjectStorageSettings> && s3_settings_)
{
    s3_settings.set(std::move(s3_settings_));
}

void S3ObjectStorage::shutdown()
{
    auto clients_ptr = clients.get();
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    const_cast<S3::Client &>(*clients_ptr->client).DisableRequestProcessing();
    const_cast<S3::Client &>(*clients_ptr->client_with_long_timeout).DisableRequestProcessing();
}

void S3ObjectStorage::startup()
{
    auto clients_ptr = clients.get();

    /// Need to be enabled if it was disabled during shutdown() call.
    const_cast<S3::Client &>(*clients_ptr->client).EnableRequestProcessing();
    const_cast<S3::Client &>(*clients_ptr->client_with_long_timeout).EnableRequestProcessing();
}

void S3ObjectStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto new_s3_settings = getSettings(config, config_prefix, context);
    auto new_client = getClient(config, config_prefix, context, *new_s3_settings);
    auto new_clients = std::make_unique<Clients>(std::move(new_client), *new_s3_settings);
    s3_settings.set(std::move(new_s3_settings));
    clients.set(std::move(new_clients));
}

std::unique_ptr<IObjectStorage> S3ObjectStorage::cloneObjectStorage(
    const std::string & new_namespace, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto new_s3_settings = getSettings(config, config_prefix, context);
    auto new_client = getClient(config, config_prefix, context, *new_s3_settings);
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    return std::make_unique<S3ObjectStorage>(
        std::move(new_client), std::move(new_s3_settings),
        version_id, s3_capabilities, new_namespace,
        endpoint);
}

S3ObjectStorage::Clients::Clients(std::shared_ptr<S3::Client> client_, const S3ObjectStorageSettings & settings)
    : client(std::move(client_)), client_with_long_timeout(client->clone(std::nullopt, settings.request_settings.long_request_timeout_ms)) {}

class S3PlainObjectStorageForCache::SuperWriteBufferFromFile
{
    static std::string describe(BufferBase & buf)
    {
        auto addr_to_str = [](const char * ptr) { return static_cast<const void *>(ptr); };
        auto desc = [&](BufferBase::Buffer & b)
        { return fmt::format("addr_to_str(b.begin())={}, b.size()={}", addr_to_str(b.begin()), b.size()); };

        return fmt::format(
            "\nbuf.available()={}, buf.count()={}, buf.offset()={}, buf.position()={},\n\tdesc(buf.buffer())={},\n\t"
            "desc(buf.internalBuffer()));={}",
            buf.available(),
            buf.count(),
            buf.offset(),
            addr_to_str(buf.position()),
            desc(buf.buffer()),
            desc(buf.internalBuffer()));
    }

    class ReadBuffer : public ReadBufferFromFileBase
    {
    public:
        ReadBuffer(
            std::shared_ptr<std::string> data_,
            const std::string & remote_path_,
            std::shared_ptr<S3PlainObjectStorageForCache::SuperWriteBufferFromFile> in_flight_buffers_)
            : data(data_), memory_reader(*data), path(remote_path_), in_flight_buffers(std::move(in_flight_buffers_))
        {
            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(*this), */
            /* describe(memory_reader)); */

            if (data->empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Data buffer cannot be empty");

            updateBuffer();

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data->size(), */
            /* describe(*this), */
            /* describe(memory_reader)); */

            read_until_position = memory_reader.buffer().size();

            /* LOG_DEBUG(&Poco::Logger::get("debug"), "read_until_position={}", read_until_position); */
        }

        off_t getPosition() override { return memory_reader.getPosition(); }

        size_t getFileOffsetOfBufferEnd() const override
        {
            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(const_cast<ReadBuffer &>(*this)), */
            /* describe(memory_reader)); */

            return memory_reader.offset();
        }

        void setReadUntilPosition(size_t position) override
        {
            if (position > data->size())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "setReadUntilPosition: position={}, read_until_position={}, data.size()={}, describe(this)={}, "
                    "describe(memory_reader)={}",
                    position,
                    read_until_position,
                    data->size(),
                    describe(*this),
                    describe(memory_reader));

            read_until_position = position;
            /* LOG_DEBUG(&Poco::Logger::get("debug"), "setReadUntilPosition(): read_until_position={}", read_until_position); */
        }

        off_t seek(off_t offset, int whence) override
        {
            /* LOG_DEBUG(&Poco::Logger::get("debug"), "offset={}, whence={}", offset, whence); */

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(const_cast<ReadBuffer &>(*this)), */
            /* describe(memory_reader)); */

            const auto ret = memory_reader.seek(offset, whence);

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(const_cast<ReadBuffer &>(*this)), */
            /* describe(memory_reader)); */

            updateBuffer();

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(const_cast<ReadBuffer &>(*this)), */
            /* describe(memory_reader)); */

            return ret;
        }

        std::string getFileName() const override { return path; }

        size_t getFileSize() override { return read_until_position; }

        bool nextImpl() override
        {
            if (!memory_reader.available())
                memory_reader.next();

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(const_cast<ReadBuffer &>(*this)), */
            /* describe(memory_reader)); */

            const size_t to_read = read_until_position - memory_reader.offset();
            internalBuffer().resize(std::min(to_read, internalBuffer().size()));
            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "read_until_position={}, memory_reader.count()={}", */
            /* read_until_position, */
            /* memory_reader.count()); */

            const auto read = memory_reader.read(internalBuffer().begin(), internalBuffer().size());
            /// This is how you interact with CachedOnDiskReadBufferFromFile - it sets `internalBuffer` to point to the external buffer
            /// (see `use_external_buffer` setting in CachedOnDiskReadBufferFromFile) and data should be read in this memory region.
            BufferBase::set(internalBuffer().begin(), read, 0);

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, path={}, data.size()={}, describe(*this)={}, describe(memory_reader)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* path, */
            /* data.size(), */
            /* describe(const_cast<ReadBuffer &>(*this)), */
            /* describe(memory_reader)); */

            return read > 0;
        }

        /* ~ReadBuffer() override { LOG_DEBUG(&Poco::Logger::get("debug"), "~ReadBuffer path={}", path); } */

    private:
        void updateBuffer()
        {
            const auto & buf = memory_reader.buffer();
            BufferBase::set(buf.begin(), buf.size(), memory_reader.offset());
        }

        std::shared_ptr<std::string> data;

        mutable ReadBufferFromString memory_reader;
        const std::string path;
        size_t read_until_position;

        std::shared_ptr<S3PlainObjectStorageForCache::SuperWriteBufferFromFile> in_flight_buffers;
    };

    class WriteBuffer : public WriteBufferFromFileBase
    {
    public:
        WriteBuffer(
            std::shared_ptr<std::string> data_,
            std::unique_ptr<WriteBufferFromFileBase> impl_,
            std::shared_ptr<S3PlainObjectStorageForCache::SuperWriteBufferFromFile> in_flight_buffers_)
            : WriteBufferFromFileBase(0, nullptr, 0)
            , data(data_)
            , memory_writer(*data)
            , remote_writer(std::move(impl_))
            , in_flight_buffers(std::move(in_flight_buffers_))
        {
            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data.size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */

            updateBuffer();

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data.size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */
        }

        void sync() override { remote_writer->sync(); }

        std::string getFileName() const override { return remote_writer->getFileName(); }

        void nextImpl() override
        {
            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data.size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */

            BufferBase::set(position(), available(), 0);

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data.size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */
        }

        void finalizeImpl() override
        {
            /* LOG_DEBUG(&Poco::Logger::get("debug"), "s={}", data); */

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data->size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */

            /// Don't call finalize, because
            /// 1. `memory_writer` doesn't buffer anything, because it doesn't have it's own buffer
            /// 2. actually, if called - it will reset `data` to an empty string

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data.size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */

            memory_writer.set(data->data(), count(), count());

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, __LINE__={}, data.size()={}, describe(*this)={}, describe(memory_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* __LINE__, */
            /* data->size(), */
            /* describe(*this), */
            /* describe(memory_writer)); */

            remote_writer->write(data->data(), memory_writer.count());
            remote_writer->finalize();
            /* remote_writer->sync(); */

            /* LOG_DEBUG( */
            /* &Poco::Logger::get("debug"), */
            /* "__PRETTY_FUNCTION__={}, describe(*this)={}, describe(memory_writer)={}, describe(*remote_writer)={}", */
            /* __PRETTY_FUNCTION__, */
            /* describe(*this), */
            /* describe(memory_writer), */
            /* describe(*remote_writer)); */
        }

        /* ~WriteBuffer() override { LOG_DEBUG(&Poco::Logger::get("debug"), "~WriteBuffer path={}", remote_writer->getFileName()); } */

    private:
        void updateBuffer()
        {
            const auto & buf = memory_writer.buffer();
            BufferBase::set(buf.begin(), buf.size(), memory_writer.offset());
        }

        std::shared_ptr<std::string> data;

        WriteBufferFromString memory_writer;
        std::unique_ptr<WriteBufferFromFileBase> remote_writer;

        std::shared_ptr<S3PlainObjectStorageForCache::SuperWriteBufferFromFile> in_flight_buffers;
    };

public:
    explicit SuperWriteBufferFromFile(const std::string & remote_path_)
        : remote_path(remote_path_), data(std::make_shared<std::string>(FILECACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE, '\0'))
    {
    }

    ~SuperWriteBufferFromFile() { LOG_DEBUG(&Poco::Logger::get("debug"), "~SuperWriteBufferFromFile {}", remote_path); }

    std::unique_ptr<ReadBufferFromFileBase>
    getReadBuffer(std::shared_ptr<S3PlainObjectStorageForCache::SuperWriteBufferFromFile> in_flight_buffers_)
    {
        return std::make_unique<ReadBuffer>(data, remote_path, std::move(in_flight_buffers_));
    }

    std::unique_ptr<WriteBufferFromFileBase> getWriteBuffer(
        std::unique_ptr<WriteBufferFromFileBase> impl,
        std::shared_ptr<S3PlainObjectStorageForCache::SuperWriteBufferFromFile> in_flight_buffers_)
    {
        auto ret = std::make_unique<WriteBuffer>(data, std::move(impl), std::move(in_flight_buffers_));
        return ret;
    }

    const std::string remote_path;

private:
    /// Read/WriteBuffer dying will remove SuperWriteBufferFromFile from `in_flight_buffers`.
    /// And if it was WriteBuffer, then in dtor it will resize `data`, so it should outlive `SuperWriteBufferFromFile`.
    std::shared_ptr<std::string> data;
};

S3PlainObjectStorageForCache::~S3PlainObjectStorageForCache() = default;

std::unique_ptr<ReadBufferFromFileBase> S3PlainObjectStorageForCache::readObjects( /// NOLINT
    const StoredObjects & objects_,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t>) const
{
    StoredObjects objects = objects_;

    if (objects.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected only single object in arguments");

    {
        std::lock_guard lock(in_flight_buffers->m);

        /* LOG_DEBUG( */
        /* &Poco::Logger::get("debug"), */
        /* "readObjects: objects.front().local_path={}, objects.front().remote_path={}", */
        /* objects.front().local_path, */
        /* objects.front().remote_path); */

        /* std::string paths; */
        /* for (const auto & [path, _] : in_flight_buffers->map) */
        /* paths += path + ","; */
        /* LOG_DEBUG(&Poco::Logger::get("debug"), "readObjects paths={}", paths); */

        const auto & path = objects.front().remote_path;
        if (auto it = in_flight_buffers->map.find(path); it != in_flight_buffers->map.end())
            if (auto ifb = it->second.lock())
                return ifb->getReadBuffer(ifb);
    }

    /// There is a race between requesting objects metadata from s3 for in-flight cache segment and actual reading from this segment in this function.
    /// If object currently in-flight there is still no (accessible) metadata for it on s3. So we will have `bytes_size` == 0. And it is ok as far as
    /// this object remains in-flight until this call is finished (because in this case we will use `S3PlainObjectStorageForCache::SuperWriteBufferFromFile::ReadBuffer`
    /// that doesn't case about s3 metadata). But if the segment was finished after requesting metadata but before we found that it is already removed from
    /// `in_flight_buffers` - we will end up with `ReadBufferFromRemoteFSGather` with incorrect state. So we just re-requesting object sizes here.
    if (getTotalSize(objects) == 0)
        for (auto & obj : objects)
            obj.bytes_size = getObjectMetadata(obj.remote_path).size_bytes;

    flag = true;
    auto ret = Base::readObjects(objects, read_settings, read_hint);
    flag = false;
    return ret;
}

std::unique_ptr<WriteBufferFromFileBase> S3PlainObjectStorageForCache::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    std::lock_guard lock(in_flight_buffers->m);

    /* LOG_DEBUG(&Poco::Logger::get("debug"), "writeObject: object.remote_path={}", object.remote_path); */

    const auto & path = object.remote_path;
    if (auto it = in_flight_buffers->map.find(path); it != in_flight_buffers->map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There should be only one write buffer instance for each path at every point in time");

    /* std::string paths; */
    /* for (const auto & [p, _] : in_flight_buffers->map) */
    /* paths += p + ","; */
    /* LOG_DEBUG(&Poco::Logger::get("debug"), "writeObjects paths={}", paths); */

    auto impl_buffer = Base::writeObject(object, mode, attributes, buf_size, write_settings);
    auto super_buffer = std::shared_ptr<SuperWriteBufferFromFile>(
        new SuperWriteBufferFromFile{path},
        [this](auto ptr)
        {
            {
                std::lock_guard l(in_flight_buffers->m);
                in_flight_buffers->map.erase(ptr->remote_path);
            }

            delete ptr;
        });
    auto ret = super_buffer->getWriteBuffer(std::move(impl_buffer), super_buffer);
    in_flight_buffers->map.emplace(path, super_buffer);
    return ret;
}
}

#endif
