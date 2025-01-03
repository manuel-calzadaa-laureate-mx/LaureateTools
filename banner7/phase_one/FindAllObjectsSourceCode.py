from database.DatabaseProperties import DatabaseEnvironment
from database.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_source_code_from_procedures

if __name__ == "__main__":
    config_file = '../../db_config.json'  # JSON file containing database credentials

    # Load configuration and connect to the database
    connection = get_connection(config_file, DatabaseEnvironment.BANNER7)

    procedures_list = "../procedures.out"
    source_code_output = "src"

    # Extract source code of procedures
    extract_source_code_from_procedures(connection, procedures_list, source_code_output)

    connection.close()
