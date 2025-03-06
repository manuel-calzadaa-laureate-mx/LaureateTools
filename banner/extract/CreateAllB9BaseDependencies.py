import logging

from db.DatabaseProperties import DatabaseEnvironment
from files.B9CompletedProceduresFile import create_source_code_manager
from files.B9DependencyFile import find_all_dependencies_manager
from files.B9IncompleteProceduresFile import find_missing_procedures_manager

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    find_missing_procedures_manager()
    create_source_code_manager(database_environment=DatabaseEnvironment.BANNER9)
    find_all_dependencies_manager(database_environment=DatabaseEnvironment.BANNER9)
    # create_package_specification_source_code_manager(database_environment=DatabaseEnvironment.BANNER9)
