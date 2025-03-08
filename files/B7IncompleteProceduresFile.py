import logging
import os

from db.OracleDatabaseTools import OracleDBConnectionPool
from db.datasource.ProceduresDatasource import query_all_procedures_by_owner_and_package
from files.B7CompletedProceduresFile import get_completed_procedures_file_path
from tools.FileTools import read_csv_file, write_csv_file

INCOMPLETE_PROCEDURES_FILE_PATH = "../input/b7_incomplete_procedures.csv"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_incomplete_procedures_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, INCOMPLETE_PROCEDURES_FILE_PATH)


def get_incomplete_procedures() -> list[dict]:
    return read_csv_file(get_incomplete_procedures_file_path())


def find_missing_procedures_manager(db_pool: OracleDBConnectionPool):
    """This process takes the input csv file Incomplete_Procedures.csv
    and finds all the procedures and functions that belong to a package.
    Then it creates a new file called Complete_Procedures.csv with the full data"""
    logging.info("Starting: looking for missing procedures")
    csv_rows = read_csv_file(get_incomplete_procedures_file_path())
    processed_data = _process_missing_procedures(db_pool=db_pool, rows=csv_rows)
    write_csv_file(get_completed_procedures_file_path(), processed_data)
    logging.info("Ending: looking for missing procedures")


def _process_missing_procedures(db_pool: OracleDBConnectionPool, rows):
    """Process the data, querying missing procedures where needed."""
    processed_data = [['Owner', 'Package', 'Procedure', 'Function']]

    for row in rows:
        owner = row["Owner"].strip()
        package = row['Package'].strip() if row['Package'] else None
        procedure = row["Procedure"].strip() if row['Procedure'] else None

        if not procedure:
            procedures = query_all_procedures_by_owner_and_package(db_pool=db_pool, owner=owner, package=package)
            for proc in procedures:
                processed_data.append([owner, package, proc, ""])
        else:
            processed_data.append([owner, package, procedure, ""])

    return processed_data


if __name__ == "__main__":
    find_missing_procedures_manager()
