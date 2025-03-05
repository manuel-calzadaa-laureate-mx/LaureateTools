from files.B9SqlScriptFile import create_table_scripts_manager, create_sequence_scripts_manager, create_packages_scripts_manager

if __name__ == "__main__":
    create_table_scripts_manager()
    create_sequence_scripts_manager()
    create_packages_scripts_manager()