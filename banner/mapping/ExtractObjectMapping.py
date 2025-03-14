from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool
from files.MappingFile import write_mapping_file
from tools.MappingTools import build_mapping_data

if __name__ == "__main__":
    banner9_environment = DatabaseEnvironment.BANNER9
    db_pool_banner9 = OracleDBConnectionPool(database_name=banner9_environment)

    mapping_data = build_mapping_data(db_pool=db_pool_banner9)
    write_mapping_file(mapping_data=mapping_data)

    db_pool_banner9.close_pool()
