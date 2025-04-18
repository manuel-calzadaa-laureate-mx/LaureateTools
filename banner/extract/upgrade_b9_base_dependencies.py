from db.database_properties import DatabaseEnvironment
from db.oracle_database_tools import OracleDBConnectionPool
from files.b9_dependency_file import complete_dependency_file
from files.object_data_file import create_object_base_manager, add_base_tables_manager, add_custom_sequences_manager, \
    add_custom_tables_manager, add_custom_triggers_manager

if __name__ == "__main__":
    db_pool_banner9 = OracleDBConnectionPool(database_name=DatabaseEnvironment.BANNER9)

    create_object_base_manager()
    complete_dependency_file()
    add_base_tables_manager(db_pool=db_pool_banner9, database_environment=DatabaseEnvironment.BANNER9)
    add_custom_sequences_manager(db_pool=db_pool_banner9, database_environment=DatabaseEnvironment.BANNER9)

    ## ADD CUSTOM BANNER 9 TABLES
    add_custom_tables_manager(db_pool=db_pool_banner9, database_environment=DatabaseEnvironment.BANNER9)
    add_custom_triggers_manager(db_pool=db_pool_banner9, database_environment=DatabaseEnvironment.BANNER9)

    db_pool_banner9.close_pool()
