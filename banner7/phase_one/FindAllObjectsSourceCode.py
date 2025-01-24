from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection
from tools.ExtractTools import extract_source_code_from_procedures

if __name__ == "__main__":
    config_file = '../../config/db_config.json'  # JSON file containing db credentials

    # Load configuration and connect to the db
    connection = get_db_connection(DatabaseEnvironment.BANNER7)

    procedures_list = "../procedures.out"
    source_code_output = "src"

    # Extract source code of procedures
    extract_source_code_from_procedures(connection, procedures_list, source_code_output)

    connection.close()
