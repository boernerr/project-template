create table sample_table as
with sample_cte as (
SELECT
column1,
column2,
join_key
FROM source_table1
),
source_2_cte as(
SELECT
*
FROM source_table2)
SELECT
aa.amount,
aa.date,
bb.column1,
bb.column2,
-- Below line may give issues because we don't have the explicit columns from cc.table! Need to test
cc.*
FROM FACT_TABLE aa
left join sample_cte bb on aa.join_key = bb.join_key
left join source_2_cte cc on aa.join_key = cc.join_key