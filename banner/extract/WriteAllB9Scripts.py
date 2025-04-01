from db.DatabaseProperties import DatabaseEnvironment
from files.B9SqlScriptFile import create_packages_scripts_manager, create_table_scripts_manager, \
    create_sequence_scripts_manager, create_trigger_scripts_manager, delete_table_scripts_manager, \
    delete_sequence_scripts_manager

if __name__ == "__main__":
    banner9_database_environment = DatabaseEnvironment.BANNER9
    create_table_scripts_manager(database_environment=banner9_database_environment)
    delete_table_scripts_manager(database_environment=banner9_database_environment)
    create_sequence_scripts_manager(database_environment=banner9_database_environment)
    delete_sequence_scripts_manager(database_environment=banner9_database_environment)
    create_trigger_scripts_manager(database_environment=banner9_database_environment)
    create_packages_scripts_manager(database_environment=banner9_database_environment)
