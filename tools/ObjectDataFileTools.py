import json

import cx_Oracle

from database.DatabaseProperties import DatabaseEnvironment
from database.SequenceDatasource import fetch_attributes_for_sequences


def extract_unique_tables_from_procedure_and_function_dependencies(input_file_name: str, environment: DatabaseEnvironment, is_custom: bool = True) -> [str]:
    """
    Extracts all unique table names from a JSON file filtered by a specific environment.

    Parameters:
        input_file_name (str): The path to the JSON file.
        environment (str): The environment to filter by.

    Returns:
        list: A sorted list of unique table names.
        :param is_custom:
    """
    try:
        with open(input_file_name, 'r') as file:
            data = json.load(file)

        # Ensure root exists and is a list
        if 'root' not in data or not isinstance(data['root'], list):
            raise ValueError("Invalid JSON structure: 'root' key not found or not a list.")

        table_names = set()
        # Iterate through root objects and filter by environment
        for item in data['root']:
            if item.get('environment').upper() == environment.name:
                objects = item.get('objects', [])
                for obj in objects:
                    dependencies = obj.get('dependencies', {})
                    tables = dependencies.get('tables', [])
                    for table in tables:
                        if table.get('custom') == is_custom:
                            table_names.add(table.get('name').upper())

        return sorted(table_names)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")


def add_new_object_element_to_object_data_file(input_file: str, environment: DatabaseEnvironment, metadata_json):
    """
    Append metadata JSON to the specified environment in the input JSON file.

    :param input_file: Path to the input JSON file
    :param environment: Environment name to append the metadata to
    :param metadata_json: JSON string to append
    """
    with open(input_file, "r") as file:
        data = json.load(file)

    new_metadata = json.loads(metadata_json)

    # Find the correct environment and append metadata
    for env in data.get("root", []):
        if env.get("environment").upper() == environment.name:
            env_objects = env.get("objects", [])
            env_objects.extend(new_metadata)
            env["objects"] = env_objects
            break

    # Write back to the file
    with open(input_file, "w") as file:
        json.dump(data, file, indent=4)


def extract_unique_sequences(input_file_name: str, environment: DatabaseEnvironment):
    """
    Extracts all unique table names from a JSON file filtered by a specific environment.

    Parameters:
        input_file_name (str): The path to the JSON file.
        environment (str): The environment to filter by.

    Returns:
        list: A sorted list of unique table names.
    """
    try:
        with open(input_file_name, 'r') as file:
            data = json.load(file)

        # Ensure root exists and is a list
        if 'root' not in data or not isinstance(data['root'], list):
            raise ValueError("Invalid JSON structure: 'root' key not found or not a list.")

        sequence_names = set()

        # Iterate through root objects and filter by environment
        for item in data['root']:
            if item.get('environment').upper() == environment.name:
                objects = item.get('objects', [])
                for obj in objects:
                    object_type = obj.get("type")
                    if object_type in ["PROCEDURE", "FUNCTION"]:
                        dependencies = obj.get('dependencies', {})
                        sequences = dependencies.get('sequences', [])
                        for sequence in sequences:
                            sequence_names.add(sequence.get('name'))
        return sorted(sequence_names)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")


def extract_attributes_from_sequences(connection: cx_Oracle.Connection, unique_sequences: [str]):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param unique_sequences:
    :param connection:
    """


    # Fetch metadata
    sequences = fetch_attributes_for_sequences(connection, unique_sequences)

    # Construct JSON structure
    sequence_metadata = []
    for sequence in sequences:
        sequence_entry = {
            "owner": sequence.get("sequence_owner"),
            "name": sequence.get("sequence_name"),
            "type": "SEQUENCE",
            "min_value": sequence.get("min_value"),
            "max_value": sequence.get("max_value"),
            "increment_by": sequence.get("increment_by"),
            "cycle_flag": sequence.get("cycle_flag"),
            "order_flag": sequence.get("order_flag"),
            "cache_size": sequence.get("cache_size"),
            "last_number": sequence.get("last_number")
        }
        sequence_metadata.append(sequence_entry)

    # Return the constructed JSON structure
    return json.dumps(sequence_metadata, indent=4)
