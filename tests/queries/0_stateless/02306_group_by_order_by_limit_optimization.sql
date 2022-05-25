create table tbl_agg(a UInt64, b UInt64) engine MergeTree order by (a, b);

insert into tbl_agg select number, number from numbers_mt(100);

select a from tbl_agg group by a, b order by a limit 5;

select a from tbl_agg group by a, b order by a limit 5 offset 5;

select a, sipHash64(a) from tbl_agg group by a, b order by a limit 5;

select a from tbl_agg group by a, b order by sipHash64(a) limit 5;

drop table tbl_agg;
