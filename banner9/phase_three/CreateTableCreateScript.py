from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_connection
from db.datasource.TablesDatasource import fetch_full_indexes_for_tables
from tools.SqlScriptTools import build_create_table_script

if __name__ == "__main__":
    object_data = "../../object_data.json"
    table_name = "TZRAPPL"
    environment = DatabaseEnvironment.BANNER7
    script = build_create_table_script(object_data_file=object_data, table_name=table_name, environment=environment)
    print(script)
    # db_connection = get_connection(config_file="../../db_config.json", database_name= DatabaseEnvironment.BANNER7)
    # indexes = fetch_full_indexes_for_tables(connection=db_connection, table_names=["TZRFACC"])
    # print(indexes)
    # db_connection.close()
