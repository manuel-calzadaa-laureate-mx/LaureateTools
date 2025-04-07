import os

from db.database_properties import DatabaseEnvironment
from tools.file_tools import read_csv_file

TABLES_FILE = "../input/new_tables.csv"


def get_tables_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, TABLES_FILE)


def get_tables_by_environment(database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> set[str]:
    tables = read_csv_file(get_tables_file_path())
    tables_by_environment = set()
    for table in tables:
        if table.get("ENVIRONMENT") == database_environment.value.upper():
            tables_by_environment.add(table.get("TABLE"))

    return tables_by_environment
