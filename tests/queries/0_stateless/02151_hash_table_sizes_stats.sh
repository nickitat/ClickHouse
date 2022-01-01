#!/usr/bin/env bash
# Tags: long

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


table_size=10000
max_threads=5


prepare_table() {
  table_name="t_hash_table_sizes_stats_$RANDOM$RANDOM"
  $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table_name;"
  if [ -z "$1" ]; then
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $table_name(number UInt64) Engine=MergeTree() ORDER BY tuple();"
  else
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $table_name(number UInt64) Engine=MergeTree() ORDER BY $1;"
  fi
  $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES $table_name;"
  for ((i = 1; i <= max_threads; i++)); do
    cnt=$((table_size / max_threads))
    from=$(((i - 1) * cnt))
    $CLICKHOUSE_CLIENT -q "INSERT INTO $table_name SELECT * FROM numbers($from, $cnt);"
  done
}

prepare_table_with_sorting_key() {
  prepare_table "$1"
}

run_query() {
  query_id="${CLICKHOUSE_DATABASE}_hash_table_sizes_stats_$RANDOM$RANDOM"
  $CLICKHOUSE_CLIENT --query_id="$query_id" --multiquery -q \
    "SET max_block_size = $((table_size / 10));
     SET merge_tree_min_rows_for_concurrent_read = 1;
     SET max_untracked_memory = 0;
     $query"
}

check_number_of_string_occurrences_satisfies_condition() {
  $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
  $CLICKHOUSE_CLIENT                                                                               \
    --param_query_id="$query_id"                                                                   \
    -q "WITH (                                                                                     \
          SELECT COUNT(message)                                                                    \
            FROM system.text_log                                                                   \
           WHERE event_date >= yesterday() AND query_id = {query_id:String} AND message ILIKE '$1' \
        ) AS res                                                                                   \
        SELECT res $2"
}

check_logs_for_new_size_hint() {
  check_number_of_string_occurrences_satisfies_condition "%new size_hint=$expected_size_hint" "= 1"
}

check_logs_for_no_new_size_hint() {
  check_number_of_string_occurrences_satisfies_condition "%new size_hint%" "= 0"
}

check_logs_for_preallocate_singlethreaded() {
  check_number_of_string_occurrences_satisfies_condition "%preallocate $1 elements%" "= 1"
}

check_logs_for_preallocate_multithreaded() {
  # rows may be distributed in any way including "everything goes to the one particular thread"
  check_number_of_string_occurrences_satisfies_condition "%preallocate $1 elements%" "BETWEEN 1 AND $max_threads"
}

check_logs_for_convertion_to_two_level() {
  # rows may be distributed in any way including "everything goes to the one particular thread"
  check_number_of_string_occurrences_satisfies_condition "%converting % to two-level%" "BETWEEN 1 AND $max_threads"
}

print_border() {
  echo "--"
}


test_aggregation_without_table() {
  query="
  -- some odd case, expected no size_hint --
    SELECT 1 AS number
  GROUP BY number
    FORMAT Null;"
  run_query
  check_logs_for_no_new_size_hint
  print_border
}


source "$CURDIR"/02151_hash_table_sizes_stats.testcases


test_aggregation_without_table
test_one_thread_no_group_by
test_one_thread_simple_group_by
test_one_thread_simple_group_by_with_limit
test_several_threads_simple_group_by_with_limit_single_level_ht
test_several_threads_simple_group_by_with_limit_two_level_ht
test_several_threads_simple_group_by_external_aggregation_two_level_ht
test_several_threads_simple_group_by_in_order_aggregation
test_several_threads_simple_group_by_with_limit_and_rollup_single_level_ht
test_several_threads_simple_group_by_with_limit_and_rollup_two_level_ht
test_several_threads_simple_group_by_with_limit_and_cube_single_level_ht
test_several_threads_simple_group_by_with_limit_and_cube_two_level_ht
