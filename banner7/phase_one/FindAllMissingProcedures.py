from database.DatabaseProperties import DatabaseEnvironment
from database.OracleDatabaseTools import get_connection
from tools.ExtractTools import find_missing_procedures_from_csv_file

if __name__ == "__main__":
    config_file = '../../db_config.json'  # JSON file containing database credentials

    input_csv = "procedures.csv"
    output_csv = "../procedures.out"

    # Load configuration and connect to the database
    connection = get_connection(config_file, DatabaseEnvironment.BANNER7)

    # Find the missing procedures
    find_missing_procedures_from_csv_file(input_file=input_csv, output_file=output_csv, connection=connection)

    connection.close()
