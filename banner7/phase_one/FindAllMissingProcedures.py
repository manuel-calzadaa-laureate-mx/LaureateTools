from db.OracleDatabaseTools import get_connection
from tools.ExtractTools import find_procedures_from_csv_file

if __name__ == "__main__":
    config_file = '../../db_config.json'  # JSON file containing db credentials

    input_csv = "procedures.csv"
    output_csv = "../procedures.out"

    # Load configuration and connect to the db
    connection = get_connection(config_file, "banner7")

    # Find the missing procedures
    find_procedures_from_csv_file(input_csv, output_csv, connection)

    connection.close()