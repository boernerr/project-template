import sqlparse

sql_with_cte = '''
create table sample_table as
with sample_cte as (
SELECT 
column1,
column2,
join_key
FROM source_table1
)
SELECT
aa.amount,
aa.date,
bb.column1,
bb.column2
FROM FACT_TABLE aa
left join sample_cte bb on aa.join_key = bb.join_key
;'''

sql_cte_ambiguous = '''
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
-- Below line DOES give issues when using sqlparse!
cc.*
FROM FACT_TABLE aa
left join sample_cte bb on aa.join_key = bb.join_key
left join source_2_cte cc on aa.join_key = cc.join_key;
'''

tokens = sqlparse.parse(sql_with_cte)[0].tokens



def find_selected_columns(query) -> list:
    tokens = sqlparse.parse(query)[0].tokens
    found_select = False
    for token in tokens:
        if found_select:
            if isinstance(token, sqlparse.sql.IdentifierList):
                return [
                    col.value.split(" ")[-1].strip("`").rpartition('.')[-1]
                    for col in token.tokens
                    if isinstance(col, sqlparse.sql.Identifier)
                ]
        else:
            found_select = token.match(sqlparse.tokens.Keyword.DML, ["select", "SELECT"])
    raise Exception("Could not find a select statement. Weired query :)")

find_selected_columns(sql_cte_ambiguous)