clickhouse_add_executable (compression_bm lz4.cpp)
target_link_libraries (compression_bm PRIVATE
    ch_contrib::gbenchmark_all
    dbms
)
