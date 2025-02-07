from files.ObjectDataFile import create_object_base_manager, add_base_tables_manager, add_custom_sequences_manager, \
    add_custom_tables_manager, add_custom_triggers_manager

if __name__ == "__main__":
    create_object_base_manager()
    add_base_tables_manager()
    add_custom_sequences_manager()
    add_custom_tables_manager()
    add_custom_triggers_manager()

