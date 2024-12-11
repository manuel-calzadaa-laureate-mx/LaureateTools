import cx_Oracle
import json

def load_config(config_file):
    """Load database configuration from a JSON file."""
    with open(config_file, 'r') as file:
        return json.load(file)

def build_connection_string(config):
    """Construct the Oracle connection string from configuration."""
    return f"{config['username']}/{config['password']}@{config['host']}:{config['port']}/{config['service_name']}"

def list_oracle_packages(config_file):
    try:
        # Load configuration
        config = load_config(config_file)

        # Build the connection string
        connection_string = build_connection_string(config)

        # Establish a connection to the Oracle database
        with cx_Oracle.connect(connection_string) as connection:
            cursor = connection.cursor()

            # Set the schema if provided
            schema = config.get('schema', '')
            if schema:
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")

            # Query to list all packages in the database
            query = """
                SELECT OBJECT_NAME
                FROM ALL_OBJECTS
                WHERE OBJECT_TYPE = 'PACKAGE'
                ORDER BY OBJECT_NAME
            """

            cursor.execute(query)

            # Fetch all package names
            packages = cursor.fetchall()

            # Write packages to a file
            output_file = f"list_{schema}_packages.txt" if schema else "list_packages.txt"
            with open(output_file, 'w') as file:
                file.write("Packages in the database:\n")
                for package in packages:
                    file.write(f"{package[0]}\n")

            print(f"Package list written to {output_file}")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"Database error occurred: {error.message}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    # Path to the JSON configuration file
    config_file = "db_config.json"
    list_oracle_packages(config_file)
