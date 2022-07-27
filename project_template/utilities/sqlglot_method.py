"""Use package sqlglot to parse out column names from .SQL files."""

import importlib
import os
import pandas as pd
import re
import sqlglot
import sqlglot.expressions as exp
import subprocess

from collections import defaultdict
from os import path

from project_template import SQL_QUERIES_PATH, DOCS_PATH, SCRIPT_PATH
from project_template.utilities.emergency_fund_sql_table import sql_em_fund, jdm_complaints
import project_template.utilities.emergency_fund_sql_table
importlib.reload(project_template.utilities.emergency_fund_sql_table)

sql_table_query = '''
CREATE TABLE jdm_cases as
SELECT distinct d.employeemetadataid, d.cmsfullname, d.cmsssan, e.ssn, d.cmslastname, d.cmsfirstname, d.cmsmiddlename, to_date(to_char(cast(d.cmsdob as date), 'dd-mon-yy')) as cmsdob,
to_date(to_char(cast(c.dateopened as date), 'dd-mon-yy')) as casedateopened, 
to_date(to_char(cast(c.datetransferredtoopr as date), 'dd-mon-yy')) as caseto_opr, to_date(to_char(cast(c.adjudicationcomplete as date), 'dd-mon-yy')) as caseadjcomplete, 
a.caseid
FROM javelin_disciplinary.casesubjectmetadata a
JOIN javelin_disciplinary.subject d on a.subjectid=d.id
JOIN javelin_disciplinary.case c on a.caseid=c.id
JOIN spear_user.person_search e on d.employeemetadataid = e.entity_id;
'''

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
-- Below line may give issues because we don't have the explicit columns from cc.table! Need to test
cc.*
FROM FACT_TABLE aa
left join sample_cte bb on aa.join_key = bb.join_key
left join source_2_cte cc on aa.join_key = cc.join_key;
'''

for expr in sqlglot.parse_one(sql_cte_ambiguous).find(exp.Select).args['expressions']:
    print(expr)

'this is a string sample.'.find('sample')

def column_name_finder(sql_string : str) -> list:
    """Input a SQL query as a string and output the column names from the query."""
    column_names = []
    for expression in sqlglot.parse_one(sql_string, read='oracle').find(exp.Select).args["expressions"]:
        if isinstance(expression, exp.Alias):
            column_names.append(expression.text("alias"))
        elif isinstance(expression, exp.Column):
            column_names.append(expression.text("this"))

    return column_names

sql_query_columns = column_name_finder(sql_table_query)

sql_cte = column_name_finder(sql_cte_ambiguous)

for i, name in enumerate(sql_query_columns):
    print(f'{i+1}: {name}')