from db.DatabaseProperties import DatabaseEnvironment
from files.ObjectDataFile import migrate_packages_manager, migrate_sequences_manager, \
    migrate_tables_manager, migrate_addon_sequences_manager, migrate_addon_triggers_manager, \
    migrate_functions_manager, migrate_procedures_manager

if __name__ == "__main__":
    banner9_database_environment = DatabaseEnvironment.BANNER9
    migrate_tables_manager(database_environment=banner9_database_environment)
    migrate_sequences_manager(database_environment=banner9_database_environment)
    migrate_packages_manager(database_environment=banner9_database_environment)
    migrate_functions_manager(database_environment=banner9_database_environment)
    migrate_procedures_manager(database_environment=banner9_database_environment)
    migrate_addon_sequences_manager(database_environment=banner9_database_environment)
    migrate_addon_triggers_manager(database_environment=banner9_database_environment)
