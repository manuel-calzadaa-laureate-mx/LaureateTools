from files.B9ObjectDataFile import migrate_banner9_tables_manager, migrate_banner9_sequences_manager

if __name__ == "__main__":
    migrate_banner9_tables_manager()
    migrate_banner9_sequences_manager()
