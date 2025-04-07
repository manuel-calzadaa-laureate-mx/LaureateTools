from db.database_properties import DatabaseEnvironment
from files.b7_sql_script_file import create_table_scripts_manager

if __name__ == "__main__":
    create_table_scripts_manager(database_environment=DatabaseEnvironment.BANNER9)
    # create_sequence_scripts_manager()
