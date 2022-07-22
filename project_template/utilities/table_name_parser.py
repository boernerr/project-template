"""Parses the table names created from SQL query files.

Converts the output of table_name_parser.sh to a CSV of two columns: table
name, file name

The two modes are 'create' and 'use'. 'Create' is for feature tables we
generate, 'use' is for tables / data sources that are consumed (which may be
our own or from the RPT schema).

"""
import subprocess
from os import path
import pandas as pd
import re
from collections import defaultdict


SCRIPT_PATH = path.normpath(path.join(path.dirname(path.realpath(__file__))))
DOCS_PATH = path.join(SCRIPT_PATH, '..', 'docs')


def main():
    run_script_to_collect_names()
    file_params = [
        ('feature_tables_created.txt', 'create'),
        ('data_sources_used.txt', 'use')
        ]
    for file_name, mode in file_params:
        process_file(file_name=file_name, mode=mode)


def run_script_to_collect_names():
    bash_script_path = path.normpath(f'{SCRIPT_PATH}/table_name_getter.sh')
    commands = ['bash', bash_script_path]
    output = subprocess.run(commands, stdout=subprocess.PIPE, text=True).stdout.strip()
    print(output)


def process_file(file_name: str, mode: str):
    """Parse and write out CSV for a text file."""
    file_basename, file_extension = path.splitext(file_name)
    tables_created_path = path.join(SCRIPT_PATH, file_name)

    created_tables_text = get_lines_from_file(input_file=tables_created_path)
    created_tables_dict = create_dict_of_file_to_table(lines=created_tables_text)

    outpath = path.join(DOCS_PATH, f'{file_basename}.csv')
    colname = set_dynamic_colname(mode)
    write_dict_to_csv(file_table_dict=created_tables_dict, outpath=outpath, colname=colname)


class NotValidTableNameError(Exception):
    def __init__(self, message):
        self.message = message


def get_lines_from_file(input_file: str) -> list:
    with open(input_file, 'r') as f:
        return f.readlines()


def create_dict_of_file_to_table(lines: list) -> dict:
    file_table_dict = defaultdict(list)
    for line in lines:
        file_name, table_name = line.strip().split(':')
        try:
            schema, table_name = split_schema_from_table_name(table_name)
        except NotValidTableNameError:
            continue
        file_table_dict['schema'].append(schema)
        file_table_dict['table'].append(table_name)
        file_table_dict['filename'].append(file_name)
    return file_table_dict


def split_schema_from_table_name(table_name: str) -> tuple:
    """If a schema is specified, return it. Otherwise leave blank.

    The sign of a schema being present is a period separating two
    words in the table name.
    """
    schema_pattern = r'^(\w+\.)?(\w+)$'
    match = re.match(schema_pattern, table_name)
    try:
        schema, table = match.group(1, 2)
    except AttributeError:
        raise NotValidTableNameError(f'Could not match pattern in {table_name}')
    schema = schema.strip('.') if schema else ''
    return (i.upper() for i in (schema, table))


def set_dynamic_colname(mode: str) -> str:
    """Set the final column name depending on which list is being created.

    Args:
        mode: 'create' if inspecting feature tables created, 'use' if inspecting
            data sources used.

    Returns:
        Name of column describing the filename involved.

    """
    if mode == 'create':
        colname = 'created_by_filename'
    elif mode == 'use':
        colname = 'used_in_filename'
    else:
        raise ValueError(f"Parameter mode '{mode}' should be either 'create' or 'use'.")
    return colname


def write_dict_to_csv(file_table_dict: dict, outpath: str, colname: str):
    df = pd.DataFrame.from_dict(file_table_dict)
    df.columns = ['schema', 'table', colname]
    df_clean = drop_bad_rows(df)
    df_clean = df_clean.fillna('<none>')
    df_clean = df_clean.drop_duplicates()# ignore_index=True ######################################################## here
    df_clean.to_csv(outpath, index=False)


def drop_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are not relevant to the introspection.

    Bad rows include:
        - rows which come from a comment block, based on common stopwords
        - dual tables (which are ephemeral)

    """
    stopwords = ['sf',
       'into', 'dual', 'srsp', 'the',
        'any', 'payroll', 'there', 'a',
        'overtime', 'trial', 'wide', 'these',
        'fdp.', 'employee'
        ]
    stopwords_upper = [x.upper() for x in stopwords]
    stopword_matches = df.loc[df['table'].isin(stopwords_upper)].index
    df = df.drop(stopword_matches)

    dual_tables = df.loc[df['table'] == 'DUAL'].index
    df = df.drop(dual_tables)
    return df


if __name__ == '__main__':
    main()