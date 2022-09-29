-- memory bound merging changes the way how data is splitted into blocks
SELECT k, finalizeAggregation(sum_state), runningAccumulate(sum_state) FROM (SELECT intDiv(number, 50000) AS k, sumState(number) AS sum_state FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY k ORDER BY k) SETTINGS enable_memory_bound_merging_of_aggregation_results = 0;
