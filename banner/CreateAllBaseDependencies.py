import logging

from files.CompletedProceduresFile import create_source_code_manager
from files.DependencyFile import find_all_dependencies_manager
from files.IncompleteProceduresFile import find_missing_procedures_manager

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    find_missing_procedures_manager()
    create_source_code_manager()
    find_all_dependencies_manager()
