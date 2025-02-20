from db.DatabaseProperties import DatabaseEnvironment
from files.B7ObjectDataFile import create_object_base_manager, add_base_tables_manager, add_custom_sequences_manager, \
    add_custom_tables_manager, add_custom_triggers_manager

if __name__ == "__main__":
    create_object_base_manager()
    add_base_tables_manager()
    add_custom_sequences_manager()

    ## ADD CUSTOM BANNER 7 TABLES
    add_custom_tables_manager()

    ## ADD CUSTOM BANNER 9 TABLES
    add_custom_tables_manager(database_environment=DatabaseEnvironment.BANNER9)

    add_custom_triggers_manager()

