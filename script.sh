#!/usr/bin/env bash


TABLE_SIZE=10000000

run_query() {
    echo "Running query $1..."
    clickhouse-client -t -n -q "$1"
}

create_table() {
    run_query "
        CREATE TABLE IF NOT EXISTS tbl_slow_final
        (
            hash UInt64,
            key1 String,
            key2 String,
            version DateTime
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY (key1, key2)"
}

populate_table() {
    for ((i = 1; i <= 5; i++)); do
      run_query "
          INSERT INTO tbl_slow_final SELECT
              sipHash64(key1, key2) AS hash,
              key1,
              key2,
              version
          FROM
          (
              SELECT
              repeat(toString(number), 13) AS key1,
              repeat(toString(number), 42) AS key2,
              number + $i AS version
              FROM numbers($TABLE_SIZE)
          )"
    done
}


# create_table

# populate_table

run_query "
    SELECT *
    FROM tbl_slow_final
    FINAL
    FORMAT Null"

with_unions="
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE key1 < '1250000'
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE (key1 >= '1250000') AND (key1 < '2500000')
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE (key1 >= '2500000') AND (key1 < '3750000')
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE (key1 >= '3750000') AND (key1 < '5000000')
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE (key1 >= '5000000') AND (key1 < '6250000')
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE (key1 >= '6250000') AND (key1 < '7500000')
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE (key1 >= '7500000') AND (key1 < '8750000')
    UNION ALL
    SELECT *
    FROM tbl_slow_final
    FINAL
    WHERE key1 >= '8750000'
    FORMAT Null"

run_query "$with_unions"

run_query "set max_final_threads = 1; $with_unions"
