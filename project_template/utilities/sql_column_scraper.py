"""Parses the columns names of respective tables created from SQL query files."""

import os
import pandas as pd
import re
import subprocess

from collections import defaultdict
from os import path


SCRIPT_PATH = path.normpath(path.join(path.dirname(path.realpath(__file__))))
DOCS_PATH = path.join(SCRIPT_PATH, '..', 'docs')
SQL_QUERIES_PATH = path.join(SCRIPT_PATH, '..', 'sql_queries')

def get_lines_from_file(input_file: str) -> list:
    with open(input_file, 'r') as f:
        return f.readlines()

def regex_find_table_name(sql_file):
    """Return the SQL table that is created by the sql_file script. """
    # group(1) returns just the match:
    return re.search('create table(.+?)as', sql_file, re.IGNORECASE)# .group(1)

def main():
    for sql_file in os.listdir(SQL_QUERIES_PATH)[:10]:
        file_read = get_lines_from_file(path.join(SQL_QUERIES_PATH, sql_file))
        created_table_name = None
        for line in file_read:
            if regex_find_table_name(line):
                created_table_name = regex_find_table_name(line).group(1)
        print(f'[{sql_file}] creates: [{created_table_name.strip()}]')

        # print(f'Sql file: {sql_file} contains {len(file_read)} rows') #

if __name__ == '__main__':
    main()