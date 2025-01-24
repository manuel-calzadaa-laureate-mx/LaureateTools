from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection
from tools.ExtractTools import find_missing_procedures_from_csv_file

if __name__ == "__main__":
    config_file = '../../config/db_config.json'  # JSON file containing db credentials

    input_csv = "procedures.csv"
    output_csv = "../procedures.out"

    # Load configuration and connect to the db
    connection = get_db_connection(DatabaseEnvironment.BANNER7)

    # Find the missing procedures
    find_missing_procedures_from_csv_file(input_file=input_csv, output_file=output_csv, connection=connection)

    connection.close()
