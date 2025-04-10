from db.database_properties import DatabaseEnvironment
from files.b9_sql_script_file import create_packages_scripts_manager, create_table_scripts_manager, \
    create_sequence_scripts_manager, create_trigger_scripts_manager, delete_table_scripts_manager, \
    delete_sequence_scripts_manager, delete_packages_scripts_manager, delete_trigger_scripts_manager, \
    create_setup_table_scripts_manager, create_setup_package_scripts_manager

if __name__ == "__main__":
    banner9_database_environment = DatabaseEnvironment.BANNER9

    create_table_scripts_manager(database_environment=banner9_database_environment)
    delete_table_scripts_manager(database_environment=banner9_database_environment)

    create_sequence_scripts_manager(database_environment=banner9_database_environment)
    delete_sequence_scripts_manager(database_environment=banner9_database_environment)

    create_trigger_scripts_manager(database_environment=banner9_database_environment)
    delete_trigger_scripts_manager(database_environment=banner9_database_environment)

    create_packages_scripts_manager(database_environment=banner9_database_environment)
    delete_packages_scripts_manager(database_environment=banner9_database_environment)

    ## CREATE BASE TABLE OBJECTS
    create_setup_table_scripts_manager(database_environment=banner9_database_environment)
    create_setup_package_scripts_manager(database_environment=banner9_database_environment)
