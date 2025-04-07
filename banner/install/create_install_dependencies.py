from db.database_properties import DatabaseEnvironment
from tools.install_dependency_ordered_tools import create_install_dependency_ordered_manager
from tools.install_dependency_tools import create_install_dependency_file_manager

if __name__ == "__main__":
    database_environment = DatabaseEnvironment.BANNER9
    create_install_dependency_file_manager(database_environment=database_environment)
    create_install_dependency_ordered_manager()
