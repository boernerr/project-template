"""Parses the columns names of respective tables created from SQL query files."""

import os
import pandas as pd
import re
import subprocess
import importlib

from collections import defaultdict
from os import path

from project_template import SQL_QUERIES_PATH, DOCS_PATH, SCRIPT_PATH


def get_lines_from_file(input_file: str) -> list:
    with open(input_file, 'r') as f:
        return f.readlines()

def regex_find_table_name(sql_file):
    """Return the SQL table that is created by the sql_file script. """
    # group(1) returns just the match:
    return re.search('create table(.+?)as', sql_file, re.IGNORECASE)# .group(1)

def main():
    for sql_file in os.listdir(SQL_QUERIES_PATH)[:1]:
        # file_to_str = ''
        file_read = get_lines_from_file(path.join(SQL_QUERIES_PATH, sql_file))
        file_to_str = ''.join(i for i in file_read)
        print(f'len of sql file: {len(file_to_str)}')
        print(file_to_str)
        # print(file_read)
        # created_table_name = None
        # for line in file_read:
        #     if regex_find_table_name(line):
        #         # group(1) returns just the match:
        #         created_table_name = regex_find_table_name(line).group(1)
        # print(f'SQL file: [{sql_file}] creates: [DATA_SCIENCE.{created_table_name.strip()}]')


if __name__ == '__main__':
    main()