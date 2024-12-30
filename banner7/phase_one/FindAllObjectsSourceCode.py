from db.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_source_code_from_procedures

if __name__ == "__main__":
    config_file = '../../db_config.json'  # JSON file containing db credentials

    # Load configuration and connect to the db
    connection = get_connection(config_file, "banner7")

    procedures_list = "../procedures.out"
    source_code_output = "src"

    # Extract source code of procedures
    extract_source_code_from_procedures(connection, procedures_list, source_code_output)
