from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool
from files.B7ObjectDataFile import create_object_base_manager, add_base_tables_manager, add_custom_sequences_manager, \
    add_custom_tables_manager, add_custom_triggers_manager

if __name__ == "__main__":
    banner7_environment = DatabaseEnvironment.BANNER7
    db_pool_banner7 = OracleDBConnectionPool(database_name=banner7_environment)

    banner9_environment = DatabaseEnvironment.BANNER9
    db_pool_banner9 = OracleDBConnectionPool(database_name=banner9_environment)

    create_object_base_manager()
    add_base_tables_manager(db_pool=db_pool_banner7, database_environment=banner7_environment)
    add_custom_sequences_manager(db_pool=db_pool_banner7, database_environment=banner7_environment)

    ## ADD CUSTOM BANNER 7 TABLES
    add_custom_tables_manager(db_pool=db_pool_banner7, database_environment=banner7_environment)

    ## ADD CUSTOM BANNER 9 TABLES
    add_custom_tables_manager(db_pool=db_pool_banner9, database_environment=banner9_environment)

    add_custom_triggers_manager(db_pool=db_pool_banner7)

    db_pool_banner7.close_all_pools()
