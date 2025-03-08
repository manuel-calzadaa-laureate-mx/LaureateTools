from db.DatabaseProperties import DatabaseEnvironment
from files.B7SqlScriptFile import create_table_scripts_manager

if __name__ == "__main__":
    create_table_scripts_manager(database_environment=DatabaseEnvironment.BANNER7)
    # create_sequence_scripts_manager()
