from db.DatabaseProperties import DatabaseEnvironment
from files.B9ObjectDataFile import migrate_banner9_tables_manager, migrate_banner9_sequences_manager

if __name__ == "__main__":
    database_environment = DatabaseEnvironment.BANNER9
    migrate_banner9_tables_manager(database_environment=database_environment)
    migrate_banner9_sequences_manager(database_environment=database_environment)
    # migrate_banner9_package_manager()
