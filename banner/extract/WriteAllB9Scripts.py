from db.DatabaseProperties import DatabaseEnvironment
from files.B9SqlScriptFile import create_table_scripts_manager, create_sequence_scripts_manager

if __name__ == "__main__":
    database_environment = DatabaseEnvironment.BANNER9
    create_table_scripts_manager(database_environment=database_environment)
    create_sequence_scripts_manager(database_environment=database_environment)
    # create_packages_scripts_manager(database_environment=database_environment)
