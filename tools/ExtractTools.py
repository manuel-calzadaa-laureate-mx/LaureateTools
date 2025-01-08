import json
import json
import logging

import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_connection, is_oracle_built_in_object
from db.datasource.ProceduresDatasource import query_all_procedures_by_owner_and_package, extract_object_source_code
from db.datasource.TablesDatasource import fetch_table_columns_for_tables, fetch_table_attributes_for_tables, \
    fetch_column_comments_for_tables, fetch_indexes_for_tables
from tools.BusinessRulesTools import is_custom_table
from tools.PatternMatchingTools import extract_select_tables, extract_update_tables, extract_delete_tables, \
    extract_type_declarations, extract_insert_tables, extract_sequences, extract_local_functions, \
    extract_functions, extract_procedures
from tools.ScriptTools import clean_comments_and_whitespace
from collections import defaultdict
import os
import logging
import csv

logging.basicConfig(level=logging.INFO)


def find_missing_procedures_from_csv_file(input_file: str, output_file: str, connection: cx_Oracle.Connection):
    """
    Read the input CSV, query missing procedures, and generate a new CSV.
    """
    with open(input_file, mode='r') as infile, open(output_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(['Owner', 'Package', 'Procedure', 'Function'])  # Write header

        for row in reader:
            owner = row[0].strip()
            package = row[1].strip() if len(row) > 1 and row[1].strip() else None
            procedure = row[2].strip() if len(row) > 2 and row[2].strip() else None

            if not procedure:
                # Query missing procedures
                procedures = query_all_procedures_by_owner_and_package(connection, owner, package)
                for proc in procedures:
                    writer.writerow([owner, package, proc, ""])
            else:
                writer.writerow([owner, package, procedure, ""])

def extract_source_code_from_procedures(connection: cx_Oracle.Connection, input_file: str, output_dir: str):
    """
    Process the source code extraction for each row in the input CSV.

    Args:
        connection (cx_Oracle.Connection): Oracle DB connection.
        input_file (str): Path to the input CSV file.
        output_dir (str): Path to the directory where the output files will be saved.
    """
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)

    # Read input CSV file and group rows by package
    grouped_rows = defaultdict(list)
    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            package = row['Package'].strip() if row['Package'] else None
            grouped_rows[package].append(row)

    # Process each group
    for package, rows in grouped_rows.items():
        try:
            # Determine the owner (assuming all rows in the group share the same owner)
            owner = rows[0]['Owner'].strip()
            local_package = package if package else "NONE"
            logging.info(f"Processing package group: Owner={owner}, Package={local_package}")

            # Extract source code for the package if it exists
            package_source_code = None
            if package:
                package_source_code = extract_object_source_code(
                    connection=connection,
                    owner=owner,
                    package=package,
                    procedure=None,
                    function=None
                )

            # Process each row in the group
            for row in rows:
                procedure = row['Procedure'].strip()
                function = row['Function'].strip() if row['Function'] else None

                # Determine the output file path based on function or procedure
                if function:
                    output_file_path = os.path.join(output_dir, f"{owner}.{local_package}.{function}.sql")
                else:
                    output_file_path = os.path.join(output_dir, f"{owner}.{local_package}.{procedure}.sql")

                # Skip writing if the file already exists
                if os.path.exists(output_file_path):
                    logging.info(f"Skipping extraction for {output_file_path} as it already exists.")
                    continue

                # Extract source code for individual objects if package is None
                source_code_lines = package_source_code
                if not package:
                    logging.info(f"Extracting individual source code: Owner={owner}, Procedure={procedure}, Function={function}")
                    source_code_lines = extract_object_source_code(
                        connection=connection,
                        owner=owner,
                        package=None,
                        procedure=procedure,
                        function=function
                    )

                # Write the source code to the output file
                write_source_code_to_file(source_code_lines, package, procedure, function, output_file_path)

        except Exception as e:
            logging.error(f"Error processing package group: Owner={owner}, Package={local_package}, Error: {e}")


def write_source_code_to_file(source_code_lines: str, package: str = None, procedure: str = None, function: str = None,
                              output_file_path: str = None):
    """
    Writes the source code to the specified file, based on whether it's a procedure or function in a package or standalone.

    Args:
        source_code_lines (str): The source code lines to write.
        package (str): The package name, if applicable.
        procedure (str): The procedure name, if applicable.
        function (str): The function name, if applicable.
        output_file_path (str): The output file path.
    """
    if package:
        # If part of a package, extract specific code (function or procedure)
        if procedure:
            specific_source_code = extract_package_body_specific_object_from_source_code_data(source_code_lines,
                                                                                              procedure)
        elif function:
            specific_source_code = extract_package_body_specific_object_from_source_code_data(source_code_lines,
                                                                                              function)
        else:
            specific_source_code = source_code_lines  # If no specific procedure or function, use full code

    else:
        # Not part of a package, write the entire source code
        specific_source_code = source_code_lines

    # Write the source code to the file
    with open(output_file_path, 'w', encoding='utf-8') as outfile:
        outfile.writelines(specific_source_code)
        logging.info(f"Source code written to: {output_file_path}")


def extract_package_body_specific_object_from_source_code_data(source_code_lines: str, procedure_name: str):
    """
    Extract the source code for a specific procedure or function from the given package source.

    Args:
        source_code_lines (list): Lines of the package source code.
        procedure_name (str): The name of the procedure or function to extract.

    Returns:
        list: Lines of the source code for the specified procedure or function.
    """
    in_procedure = False
    procedure_code = []

    # Normalize procedure name for matching (remove spaces, case-insensitive comparison)
    procedure_name = procedure_name.strip().lower()

    for line in source_code_lines:
        normalized_line = line.strip().lower()

        # Check if the line contains the procedure or function definition
        if (f"procedure {procedure_name}" in normalized_line or
                f"function {procedure_name}" in normalized_line):
            in_procedure = True  # Start capturing lines
            procedure_code.append(line)
            continue

        # Stop capturing when we reach the `END` of the procedure or function
        if in_procedure:
            procedure_code.append(line)
            if normalized_line == "end;" or normalized_line.startswith(f"end {procedure_name}"):
                break

    return procedure_code


def extract_dependencies_from_one_source_code_data(source_code_lines: [str], object_name: str) -> dict:
    source_code = clean_comments_and_whitespace(source_code_lines)

    # Find all tables
    select_tables = extract_select_tables(source_code)
    insert_tables = extract_insert_tables(source_code)
    update_tables = extract_update_tables(source_code)
    delete_tables = extract_delete_tables(source_code)
    type_tables = extract_type_declarations(source_code)

    # Combine all unique table names
    all_tables = set(select_tables + insert_tables + update_tables + delete_tables + type_tables)
    user_defined_tables = {table.upper() for table in all_tables if not is_oracle_built_in_object(table)}

    # Find all global functions
    all_functions = extract_functions(source_code)
    local_functions = extract_local_functions(source_code)

    # Find all procedures
    procedures = extract_procedures(source_code)

    # Find all sequences
    sequences = extract_sequences(source_code)

    return {
        "TABLE": sorted(user_defined_tables),
        "FUNCTION": sorted(all_functions),
        "LOCAL_FUNCTION": sorted(local_functions),
        "SEQUENCE": sorted(sequences),
        "PROCEDURE": sorted(procedures)
    }


def extract_all_dependencies_from_source_file_folder(source_folder: str, output_csv: str):
    """
    Process all SQL files in a source folder, extract their dependencies, and write the results to a CSV file.

    Args:
        source_folder (str): Path to the folder containing SQL files.
        output_csv (str): Path to the output CSV file where dependencies will be saved.
    """
    # Create a CSV file for the output
    with open(output_csv, mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["OBJECT_OWNER", "OBJECT_TYPE", "OBJECT_PACKAGE", "OBJECT_NAME", "DEPENDENCY_TYPE",
                             "DEPENDENCY_NAME"])  # Header row

        # Iterate over each SQL file in the source folder
        for filename in os.listdir(source_folder):
            logging.info("Reading this source code file: %s", filename)
            if filename.endswith(".sql"):
                file_path = os.path.join(source_folder, filename)

                # Read the source code from the SQL file
                with open(file_path, mode='r', encoding='utf-8') as file:
                    source_code_lines = file.readlines()

                # Determine the object type (PROCEDURE/FUNCTION) and object name
                object_owner = filename.split('.')[0]
                object_package = filename.split('.')[1]
                object_name = filename.split('.')[2]  # Assuming the file name is the object name
                object_type = "PROCEDURE" if "PROCEDURE" in source_code_lines[0].upper() else "FUNCTION"
                # Extract dependencies for the object
                dependencies_map = extract_dependencies_from_one_source_code_data(source_code_lines, object_name)
                # Write the dependencies to the CSV
                for dep_type, dep_names in dependencies_map.items():
                    for dep_name in dep_names:
                        csv_writer.writerow(
                            [object_owner, object_type, object_package, object_name, dep_type, dep_name])

    print(f"Dependencies have been written to {output_csv}")


def convert_dependencies_file_to_json_object(input_filename: str) -> dict:
    """
    Extract data from a CSV file and format it into a JSON-compatible dictionary.

    Args:
        input_filename (str): The path to the input CSV file.

    Returns:
        dict: A JSON-compatible dictionary representing the extracted data.
    """
    # Initialize the structure for the JSON output
    json_data = {
        "root": [
            {
                "environment": "banner7",
                "objects": []
            }
        ]
    }

    # Read the CSV file
    with open(input_filename, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        # Temporary storage for objects
        objects_dict = {}

        # Process each row in the CSV
        for row in reader:
            obj_owner = row['OBJECT_OWNER']
            obj_type = row['OBJECT_TYPE']
            obj_package = row['OBJECT_PACKAGE']
            obj_name = row['OBJECT_NAME']
            dep_type = row['DEPENDENCY_TYPE']
            dep_name = row['DEPENDENCY_NAME']

            # Initialize the object if it doesn't exist
            if obj_name not in objects_dict:
                objects_dict[obj_name] = {
                    "type": obj_type,
                    "owner": obj_owner,
                    "package" : obj_package,
                    "name": obj_name,
                    "dependencies": {
                        "tables": [],
                        "functions": [],
                        "sequences": [],
                        "procedures": []
                    }
                }

            # Add the dependency name to the appropriate list
            if dep_type == "TABLE":
                if is_custom_table(dep_name):
                    objects_dict[obj_name]["dependencies"]["tables"].append({"name": dep_name, "custom": True})
                else:
                    objects_dict[obj_name]["dependencies"]["tables"].append({"name": dep_name, "custom": False})
            elif dep_type == "LOCAL_FUNCTION":
                objects_dict[obj_name]["dependencies"]["functions"].append({"name": dep_name, "local": True})
            elif dep_type == "FUNCTION":
                objects_dict[obj_name]["dependencies"]["functions"].append({"name": dep_name, "local": False})
            elif dep_type == "SEQUENCE":
                objects_dict[obj_name]["dependencies"]["sequences"].append({"name": dep_name})
            elif dep_type == "PROCEDURE":
                objects_dict[obj_name]["dependencies"]["procedures"].append({"name": dep_name})

        # Convert the dictionary to a list and add it to json_data
        json_data["root"][0]["objects"] = list(objects_dict.values())

    return json_data


def extract_table_metadata_from_database(connection: cx_Oracle.Connection, table_names: [str]):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param connection:
    :param table_names: List of table names (e.g., ["SZTBLAN", "ANOTHER_TABLE"])
    :param output_file: The output file to save the generated JSON
    """

    # Fetch metadata
    columns = fetch_table_columns_for_tables(connection, table_names)
    attributes = fetch_table_attributes_for_tables(connection, table_names)
    comments = fetch_column_comments_for_tables(connection, table_names)
    indexes = fetch_indexes_for_tables(connection, table_names)

    # Construct JSON structure
    table_metadata = []
    for schema in columns.keys():
        for table_name in columns[schema].keys():
            table_entry = {
                "name": table_name,
                "type": "TABLE",
                "owner": schema,
                "custom": is_custom_table(table_name),
                "columns": columns[schema].get(table_name, []),
                "attributes": attributes[schema].get(table_name, {}),
                "comments": comments[schema].get(table_name, {}),
                "indexes": indexes[schema].get(table_name, []),
                "sequences": []
            }
            table_metadata.append(table_entry)

    return json.dumps(table_metadata, indent=4)


if __name__ == "__main__":
    config_file = '../db_config.json'  # JSON file containing db credentials

    input_csv = "procedures.csv"
    output_csv = "procedures.out"

    # Load configuration and connect to the db
    connection = get_connection(config_file, DatabaseEnvironment.BANNER7)

    # Find the missing procedures
    find_missing_procedures_from_csv_file(input_csv, output_csv, connection)

    procedures_list = "procedures.out"
    source_code_output = "../src"

    # Extract source code of procedures
    extract_source_code_from_procedures(procedures_list, source_code_output)

    requirements_output = "requirements.out"

    # Find all the elements for the procedures
    output_csv = "dependencies.out"
    extract_all_dependencies_from_source_file_folder(source_code_output, output_csv)

    # Close the db connection
    connection.close()

    # Send the info to json file
