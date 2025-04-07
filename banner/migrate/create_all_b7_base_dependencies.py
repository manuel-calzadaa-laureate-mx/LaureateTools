import logging

from db.database_properties import DatabaseEnvironment
from db.oracle_database_tools import OracleDBConnectionPool
from files.b7_completed_procedures_file import create_source_code_manager
from files.b7_dependency_file import find_all_dependencies_manager
from files.b7_incomplete_procedures_file import find_missing_procedures_manager

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    db_pool_banner7 = OracleDBConnectionPool(DatabaseEnvironment.BANNER7)

    find_missing_procedures_manager(db_pool=db_pool_banner7)
    create_source_code_manager(db_pool=db_pool_banner7)
    find_all_dependencies_manager(db_pool=db_pool_banner7)

    db_pool_banner7.close_pool()
