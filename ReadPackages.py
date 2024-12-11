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

def check_packages_existence(config_file, input_file):
    try:
        # Load configuration
        config = load_config(config_file)

        # Build the connection string
        connection_string = build_connection_string(config)

        # Read input packages
        with open(input_file, 'r') as file:
            package_lines = file.readlines()

        package_status = []

        # Establish a connection to the Oracle database
        with cx_Oracle.connect(connection_string) as connection:
            cursor = connection.cursor()

            for line in package_lines:
                line = line.strip()
                if not line:
                    continue

                # Extract schema and package name
                schema, package_name = line.split('.')

                # Check if the package exists
                query = """
                    SELECT COUNT(*)
                    FROM ALL_OBJECTS
                    WHERE OBJECT_TYPE = 'PACKAGE' AND OWNER = :schema AND OBJECT_NAME = :package_name
                """
                cursor.execute(query, schema=schema, package_name=package_name)
                exists = cursor.fetchone()[0] > 0

                package_status.append((schema, package_name, exists))

        # Write the results to a file
        output_file = "package_status.txt"
        with open(output_file, 'w') as file:
            file.write("Schema,Package,Exists\n")
            for schema, package_name, exists in package_status:
                file.write(f"{schema},{package_name},{exists}\n")

        print(f"Package existence status written to {output_file}")

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(f"Database error occurred: {error.message}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    # Path to the JSON configuration file
    config_file = "db_config.json"
    # list_oracle_packages(config_file)

    # Path to the input file containing package list
    input_file = "packages.txt"
    check_packages_existence(config_file, input_file)
