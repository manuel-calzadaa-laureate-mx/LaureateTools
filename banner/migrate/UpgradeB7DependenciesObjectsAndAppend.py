from db.DatabaseProperties import DatabaseEnvironment
from files.B7ObjectDataFile import migrate_banner7_tables_manager, migrate_banner9_tables_manager

if __name__ == "__main__":
    migrate_banner7_tables_manager(database_environment=DatabaseEnvironment.BANNER9)
    migrate_banner9_tables_manager(database_environment=DatabaseEnvironment.BANNER9)
