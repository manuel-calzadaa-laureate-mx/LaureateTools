import logging

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool
from files.B7CompletedProceduresFile import create_source_code_manager
from files.B7DependencyFile import find_all_dependencies_manager
from files.B7IncompleteProceduresFile import find_missing_procedures_manager

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
