import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=False,
    image="yandex/clickhouse-server",
    tag="21.1",
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance("node2", with_zookeeper=False)
node3 = cluster.add_instance(
    "node3", user_configs=["configs/users.d/config.xml"], with_zookeeper=False
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def run_query_with_settings(node, query, user_settings={}):
    defaults = {
        "enable_memory_bound_merging_of_aggregation_results": 1,
        "group_by_two_level_threshold": 1000,
        "max_block_size": 500,
        "max_bytes_before_external_group_by": 1,
    }
    return node.query(query, settings={**defaults, **user_settings})


# def test_backward_compatability(start_cluster):
# node1.query("create table t (a UInt64) engine = MergeTree order by a")
# node2.query("create table t (a UInt64) engine = MergeTree order by a")
# node3.query("create table t (a UInt64) engine = MergeTree order by a")
#
# node1.query("insert into t select number % 100000 from numbers_mt(1000000)")
# node2.query("insert into t select number % 100000 from numbers_mt(1000000)")
# node3.query("insert into t select number % 100000 from numbers_mt(1000000)")
#
# assert (
# node1.query(
# """
# select count()
# from remote('node{1,2,3}', currentDatabase(), t)
# group by a
# limit 1 offset 12345
# settings enable_memory_bound_merging_of_aggregation_results = 1, group_by_two_level_threshold = 1000, group_by_two_level_threshold_bytes = 1000
# """
# )
# == "30\n"
# )
#
# assert (
# node2.query(
# """
# select count()
# from remote('node{1,2,3}', currentDatabase(), t)
# group by a
# limit 1 offset 12345
# settings enable_memory_bound_merging_of_aggregation_results = 1, group_by_two_level_threshold = 1000, group_by_two_level_threshold_bytes = 1000
# """
# )
# == "30\n"
# )
#
# node1.query("drop table t")
# node2.query("drop table t")
# node3.query("drop table t")


def test_remote_node_sends_multiple_single_level_tables_from_ordinary_aggregation(
    start_cluster,
):
    node2.query("create table t (a UInt64) engine = MergeTree order by tuple()")
    node3.query("create table t (a UInt64) engine = MergeTree order by tuple()")

    node2.query("insert into t select number % 100000 from numbers_mt(1000000)")
    node3.query("insert into t select number from numbers(100000)")

    assert node3.query("select getSetting('max_threads')") == "1\n"

    assert (
        run_query_with_settings(
            node2,
            """
            select throwIf(count() != 11)
            from remote('node{2,3}', currentDatabase(), t)
            group by a
            format Null
        """,
        )
        == ""
    )

    node2.query("drop table t")
    node3.query("drop table t")


def test_remote_node_sends_multiple_single_level_tables_from_aggregation_in_order(
    start_cluster,
):
    node2.query("create table t (a UInt64) engine = MergeTree order by tuple()")
    node3.query("create table t (a UInt64) engine = MergeTree order by a")

    node2.query("insert into t select number from numbers_mt(1000000)")
    node3.query("insert into t select number from numbers_mt(1000000)")

    assert (
        run_query_with_settings(
            node2,
            """
            select throwIf(count() != 2)
            from remote('node{2,3}', currentDatabase(), t)
            group by a
            order by a
            format Null
        """,
            user_settings={
                "aggregation_in_order_max_block_bytes": 1000,
                "optimize_aggregation_in_order": 1,
            },
        )
        == ""
    )

    node2.query("drop table t")
    node3.query("drop table t")


def test_old_initiator_and_new_remote_node(start_cluster):
    node1.query("create table t (a UInt64) engine = MergeTree order by tuple()")
    node2.query("create table t (a UInt64) engine = MergeTree order by tuple()")

    node1.query("insert into t select number % 100000 from numbers_mt(1000000)")
    node2.query("insert into t select number % 100000 from numbers_mt(1000000)")

    assert (
        run_query_with_settings(
            node1,
            """
            select throwIf(count() != 20)
            from remote('node{1,2}', currentDatabase(), t)
            group by a
            format Null
        """,
        )
        == ""
    )

    node1.query("drop table t")
    node2.query("drop table t")


def test_new_initiator_and_old_remote_node(start_cluster):
    node1.query("create table t (a UInt64) engine = MergeTree order by tuple()")
    node2.query("create table t (a UInt64) engine = MergeTree order by tuple()")

    node1.query("insert into t select number % 100000 from numbers_mt(1000000)")
    node2.query("insert into t select number % 100000 from numbers_mt(1000000)")

    assert (
        run_query_with_settings(
            node2,
            """
            select throwIf(count() != 20)
            from remote('node{1,2}', currentDatabase(), t)
            group by a
            format Null
        """,
        )
        == ""
    )

    node1.query("drop table t")
    node2.query("drop table t")
