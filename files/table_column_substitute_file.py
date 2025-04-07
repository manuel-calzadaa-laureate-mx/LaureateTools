import os

from tools.file_tools import read_csv_file

TABLES_COLUMN_SUBSTITUTE_FILE = "../input/table_column_substitute.csv"


def get_tables_column_substitute_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, TABLES_COLUMN_SUBSTITUTE_FILE)


def get_tables_column_substitute_file(table_name: str,
                                      original_column: str) -> str:
    """
    Retrieves the modified column name from the CSV file based on
    the table name and original column name.
    """
    tables = read_csv_file(get_tables_column_substitute_file_path())
    modified_column = None

    for row in tables:
        if row.get("table") == table_name and row.get("original_column") == original_column:
            modified_column = row.get("modified_column")
            break

    return modified_column if modified_column else original_column
