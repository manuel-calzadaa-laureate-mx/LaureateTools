import logging

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool
from files.B9CompletedProceduresFile import create_package_specification_source_code_manager, create_source_code_manager
from files.B9DependencyFile import find_all_dependencies_manager
from files.B9IncompleteProceduresFile import find_missing_procedures_manager

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    banner9_environment = DatabaseEnvironment.BANNER9
    db_pool_banner9 = OracleDBConnectionPool(database_name=banner9_environment)

    find_missing_procedures_manager(db_pool=db_pool_banner9)
    create_source_code_manager(db_pool=db_pool_banner9, database_environment=banner9_environment)
    find_all_dependencies_manager(db_pool=db_pool_banner9, database_environment=banner9_environment)

    # add full package specifications sources
    create_package_specification_source_code_manager(db_pool=db_pool_banner9, database_environment=banner9_environment)

    db_pool_banner9.close_pool()
