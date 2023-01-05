create table t(a UInt32) engine=MergeTree order by tuple() partition by a % 16;

system stop merges t;

set query_plan_read_in_order = 0;

insert into t select number from numbers_mt(1e6);
insert into t select number from numbers_mt(1e6);

explain pipeline select a from t group by a settings max_threads=16;

select count() from (select a from t group by a) settings max_threads=16;

drop table t;
