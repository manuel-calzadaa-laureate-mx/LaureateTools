from files.ObjectDataFile import migrate_banner7_tables_manager, migrate_banner9_tables_manager

if __name__ == "__main__":
    migrate_banner7_tables_manager()
    migrate_banner9_tables_manager()

