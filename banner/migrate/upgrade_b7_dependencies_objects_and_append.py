from db.database_properties import DatabaseEnvironment
from files.b7_object_data_file import migrate_banner7_tables_manager, migrate_banner9_tables_manager

if __name__ == "__main__":
    migrate_banner7_tables_manager(database_environment=DatabaseEnvironment.BANNER9)
    migrate_banner9_tables_manager(database_environment=DatabaseEnvironment.BANNER9)
