from db.DatabaseProperties import DatabaseEnvironment
from tools.InstallDependencyOrderedTools import create_install_dependency_ordered_manager
from tools.InstallDependencyTools import create_install_dependency_file_manager

if __name__ == "__main__":
    database_environment = DatabaseEnvironment.BANNER9
    create_install_dependency_file_manager(database_environment=database_environment)
    create_install_dependency_ordered_manager()
