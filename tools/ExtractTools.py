import csv
import json
import os
import re

import cx_Oracle

from db.OracleDatabaseTools import get_connection, is_oracle_built_in_object
from db.ProceduresDatasource import query_all_procedures_by_owner_and_package, extract_object_source_code
from db.TablesDatasource import fetch_table_columns_for_tables, fetch_table_attributes_for_tables, \
    fetch_column_comments_for_tables, fetch_indexes_for_tables
from tools.BusinessRulesTools import is_custom_table
from tools.PatternMatchingTools import extract_select_tables, extract_update_tables, extract_delete_tables, \
    extract_type_declarations, extract_insert_tables, exclude_insert_into_not_functions, \
    exclude_procedure_not_functions, extract_sequences
from tools.ScriptTools import clean_comments_and_whitespace


def find_procedures_from_csv_file(input_file: str, output_file: str, connection: cx_Oracle.Connection):
    """
    Read the input CSV, query missing procedures, and generate a new CSV.
    """
    with open(input_file, mode='r') as infile, open(output_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(['Owner', 'Package', 'Procedure'])  # Write header

        for row in reader:
            owner = row[0].strip()
            package = row[1].strip() if len(row) > 1 and row[1].strip() else None
            procedure = row[2].strip() if len(row) > 2 and row[2].strip() else None

            if not procedure:
                # Query missing procedures
                procedures = query_all_procedures_by_owner_and_package(connection, owner, package)
                for proc in procedures:
                    writer.writerow([owner, package, proc])
            else:
                writer.writerow([owner, package, procedure])


def extract_source_code_from_procedures(connection: cx_Oracle.Connection, input_file: str, output_dir: str):
    """
    Process the source code extraction for each row in the input CSV.

    Args:
        input_file (str): Path to the input CSV file.
        connection (cx_Oracle.Connection): Oracle db connection.
        :param input_file:
        :param connection:
        :param output_dir:
    """
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            owner = row['Owner'].strip()
            package = row['Package'].strip() if row['Package'] else None
            procedure = row['Procedure'].strip()

            # Extract the source code
            source_code_lines = extract_object_source_code(connection, owner, package, procedure)
            print("Procesando: ", owner, package, procedure)

            # Prepare the output file path
            output_file_path = os.path.join(output_dir, f"{procedure}.sql")

            if package:
                # Find specific procedure or function source code
                specific_source_code = find_object_source_code(source_code_lines, procedure)
                with open(output_file_path, 'w', encoding='utf-8') as outfile:
                    outfile.writelines(specific_source_code)  # Write the procedure's code to file
            else:
                # If not a package, write the entire source code to the file
                with open(output_file_path, 'w', encoding='utf-8') as outfile:
                    outfile.writelines(source_code_lines)


def find_object_source_code(source_code_lines: str, procedure_name: str):
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


def extract_dependencies_from_source_code_by_whole_file(source_code_lines) -> dict:
    source_code = clean_comments_and_whitespace(source_code_lines)

    # Find all tables
    select_tables = extract_select_tables(source_code)
    insert_tables = extract_insert_tables(source_code)
    update_tables = extract_update_tables(source_code)
    delete_tables = extract_delete_tables(source_code)
    type_tables = extract_type_declarations(source_code)

    # Find all functions
    function_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    exclude_insert_tables = exclude_insert_into_not_functions(source_code)
    exclude_procedure_names = exclude_procedure_not_functions(source_code)

    functions = set(
        match.group(1) for match in function_pattern.finditer(source_code)
        if
        not is_oracle_built_in_object(match.group(1)) and match.group(1) not in exclude_insert_tables and match.group(
            1) not in exclude_procedure_names
    )

    # Find all sequences
    sequences = extract_sequences(source_code)

    # Combine all unique table names
    all_tables = set(select_tables + insert_tables + update_tables + delete_tables + type_tables)
    user_defined_tables = {table for table in all_tables if not is_oracle_built_in_object(table)}

    return {
        "TABLE": sorted(user_defined_tables),
        "FUNCTION": sorted(functions),
        "SEQUENCE": sorted(sequences)
    }


def process_source_folder(source_folder: str, output_csv: str):
    """
    Process all SQL files in a source folder, extract their dependencies, and write the results to a CSV file.

    Args:
        source_folder (str): Path to the folder containing SQL files.
        output_csv (str): Path to the output CSV file where dependencies will be saved.
    """
    # Create a CSV file for the output
    with open(output_csv, mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["OBJECT_TYPE", "OBJECT_NAME", "DEPENDENCY_TYPE", "DEPENDENCY_NAME"])  # Header row

        # Iterate over each SQL file in the source folder
        for filename in os.listdir(source_folder):
            if filename.endswith(".sql"):
                file_path = os.path.join(source_folder, filename)

                # Read the source code from the SQL file
                with open(file_path, mode='r', encoding='utf-8') as file:
                    source_code_lines = file.readlines()

                # Determine the object type (PROCEDURE/FUNCTION) and object name
                object_name = filename.split('.')[0]  # Assuming the file name is the object name
                object_type = "PROCEDURE" if "PROCEDURE" in source_code_lines[0].upper() else "FUNCTION"

                # Extract dependencies for the object
                dependencies_map = extract_dependencies_from_source_code_by_whole_file(source_code_lines)
                # Write the dependencies to the CSV
                for dep_type, dep_names in dependencies_map.items():
                    for dep_name in dep_names:
                        csv_writer.writerow([object_type, object_name, dep_type, dep_name])

    print(f"Dependencies have been written to {output_csv}")


def export_data_to_object_json(input_filename: str, output_filename: str) -> None:
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
            obj_name = row['OBJECT_NAME']
            obj_type = row['OBJECT_TYPE']
            dep_type = row['DEPENDENCY_TYPE']
            dep_name = row['DEPENDENCY_NAME']

            # Initialize the object if it doesn't exist
            if obj_name not in objects_dict:
                objects_dict[obj_name] = {
                    "name": obj_name,
                    "type": obj_type,
                    "dependencies": {
                        "tables": [],
                        "functions": [],
                        "sequences": []
                    }
                }

            # Add the dependency name to the appropriate list
            if dep_type == "TABLE":
                if is_custom_table(dep_name):
                    objects_dict[obj_name]["dependencies"]["tables"].append({"name": dep_name, "custom": True})
                else:
                    objects_dict[obj_name]["dependencies"]["tables"].append({"name": dep_name, "custom": False})
            elif dep_type == "FUNCTION":
                objects_dict[obj_name]["dependencies"]["functions"].append({"name": dep_name})
            elif dep_type == "SEQUENCE":
                objects_dict[obj_name]["dependencies"]["sequences"].append({"name": dep_name})

            # Convert the dictionary to a list and add it to json_data
        json_data["root"][0]["objects"] = list(objects_dict.values())

    # Write the JSON output to a file
    with open(output_filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(json_data, jsonfile, indent=4)

    print(f'Successfully converted {input_filename} to {output_filename}')

def extract_attributes_from_tables(connection: cx_Oracle.Connection, table_names: [str]):
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
                "columns": columns[schema].get(table_name, []),
                "attributes": attributes[schema].get(table_name, {}),
                "comments": comments[schema].get(table_name, {}),
                "indexes": indexes[schema].get(table_name, [])
            }
            table_metadata.append(table_entry)

    return json.dumps(table_metadata, indent=4)

if __name__ == "__main__":
    config_file = '../db_config.json'  # JSON file containing db credentials

    input_csv = "procedures.csv"
    output_csv = "procedures.out"

    # Load configuration and connect to the db
    connection = get_connection(config_file, "banner7")

    # Find the missing procedures
    find_procedures_from_csv_file(input_csv, output_csv, connection)

    procedures_list = "procedures.out"
    source_code_output = "../src"

    # Extract source code of procedures
    extract_source_code_from_procedures(procedures_list, source_code_output)

    requirements_output = "requirements.out"

    # Find all the elements for the procedures
    output_csv = "dependencies.out"
    process_source_folder(source_code_output, output_csv)

    # Close the db connection
    connection.close()

    # Send the info to json file
    export_data_to_object_json()
