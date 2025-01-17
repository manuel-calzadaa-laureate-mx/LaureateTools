import json
import os
from typing import Dict, List, Optional

import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment, DatabaseObject, TableObject
from db.datasource.SequenceDatasource import fetch_attributes_for_sequences
from db.datasource.TriggersDatasource import fetch_triggers_elements_from_database


def add_new_object_to_data_file(object_data_file: str, environment: DatabaseEnvironment, new_json_data: dict):
    """
    Append metadata JSON to the specified environment in the input JSON file.

    :param object_data_file: Path to the input JSON file
    :param environment: Environment name to append the metadata to
    :param new_json_data: JSON string to append
    """
    with open(object_data_file, "r") as file:
        data = json.load(file)

    new_metadata = json.loads(new_json_data)

    # Find the correct environment and append metadata
    for env in data.get("root", []):
        if env.get("environment").upper() == environment.name:
            env_objects = env.get("objects", [])
            env_objects.extend(new_metadata)
            env["objects"] = env_objects
            break

    # Write back to the file
    with open(object_data_file, "w") as file:
        json.dump(data, file, indent=4)


def extract_table_unique_dependencies_types_from_data_file(
        input_file_name: str,
        environment: DatabaseEnvironment,
        table_object_type: TableObject
) -> [str]:
    try:
        with open(input_file_name, 'r') as file:
            data = json.load(file)

        # Ensure root exists and is a list
        if 'root' not in data or not isinstance(data['root'], list):
            raise ValueError("Invalid JSON structure: 'root' key not found or not a list.")

        dependency_names = set()
        # Iterate through root objects and filter by environment
        for item in data['root']:
            if item.get('environment').upper() == environment.name:
                objects = item.get('objects', [])
                for obj in objects:
                    object_type = obj.get("type")
                    if object_type in ["TABLE"]:
                        dependencies = obj.get(table_object_type.value, {})
                        if dependencies:
                            for dependency in dependencies:
                                dependency_names.add(dependency.get('name').upper())

        return sorted(dependency_names)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")


def extract_unique_dependencies_types_from_data_file(
        input_file_name: str,
        environment: DatabaseEnvironment,
        database_object_type: DatabaseObject,
        is_custom: bool = True
) -> [str]:
    """
    Extracts all unique dependency names from a JSON file filtered by a specific environment and object type.

    Parameters:
        input_file_name (str): The path to the JSON file.
        environment (DatabaseEnvironment): The environment to filter by.
        database_object_type (DatabaseObject): The type of dependency to extract (e.g., tables, functions).
        is_custom (bool, optional): Filter for custom dependencies if the object type is 'TABLE'.

    Returns:
        list: A sorted list of unique dependency names.
    """
    try:
        with open(input_file_name, 'r') as file:
            data = json.load(file)

        # Ensure root exists and is a list
        if 'root' not in data or not isinstance(data['root'], list):
            raise ValueError("Invalid JSON structure: 'root' key not found or not a list.")

        dependency_names = set()
        # Iterate through root objects and filter by environment
        for item in data['root']:
            if item.get('environment').upper() == environment.name:
                objects = item.get('objects', [])
                for obj in objects:
                    object_type = obj.get("type")
                    if object_type in ["PROCEDURE", "FUNCTION"]:
                        dependencies = obj.get('dependencies', {})
                        dependency_objects = dependencies.get(database_object_type.value, [])
                        for dependency in dependency_objects:
                            # Apply custom filter if the object type is TABLE
                            if database_object_type.name.upper() == DatabaseObject.TABLE.name and is_custom:
                                if dependency.get('custom') == is_custom:
                                    dependency_names.add(dependency.get('name').upper())
                            else:
                                dependency_names.add(dependency.get('name').upper())

        return sorted(dependency_names)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")


def extract_triggers_from_database(connection: cx_Oracle.Connection, unique_triggers: [str]):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param unique_triggers:
    :param connection:
    """
    # Fetch metadata
    triggers = fetch_triggers_elements_from_database(connection, unique_triggers)

    # Construct JSON structure
    triggers_metadata = []
    for trigger in triggers:
        trigger_entry = {
            "owner": trigger.get("owner"),
            "name": trigger.get("trigger_name"),
            "type": "TRIGGER",
            "table_name": trigger.get("table_name"),
            "trigger_type": trigger.get("trigger_type"),
            "triggering_event": trigger.get("triggering_event"),
            "referencing_names": trigger.get("referencing_names"),
            "when_clause": trigger.get("when_clause"),
            "status": trigger.get("status"),
            "description": trigger.get("description"),
            "trigger_body": trigger.get("trigger_body"),
        }
        triggers_metadata.append(trigger_entry)

    # Return the constructed JSON structure
    return json.dumps(triggers_metadata, indent=4)


def extract_sequences_attributes_from_database(connection: cx_Oracle.Connection, unique_sequences: [str]):
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


def add_new_environment(file_path: str, new_environment: DatabaseEnvironment):
    """
    Reads a JSON file, checks if an environment exists, and adds it if it doesn't.

    :param file_path: Path to the JSON file.
    :param new_environment: The environment to add (e.g., "banner9").
    """
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
    else:
        # Initialize the structure if the file doesn't exist
        data = {"root": []}

    environment_exists = any(obj["environment"] == new_environment.value for obj in data["root"])
    if not environment_exists:
        data["root"].append({"environment": new_environment.value, "objects": []})

    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def extract_all_objects_from_data_file(input_file_name: str) -> List[Dict[str, Optional[str]]]:
    """
    Extracts a list of objects from the JSON data with the specified structure.

    Args:
        json_data (dict): The JSON data containing the objects to be extracted.

    Returns:
        List[Dict[str, Optional[str]]]: A list of dictionaries with the extracted data.
        :param input_file_name:
    """
    try:
        with open(input_file_name, 'r') as file:
            json_data = json.load(file)
        objects_list = []

        # Navigate through the JSON structure
        for root_entry in json_data.get("root", []):
            for obj in root_entry.get("objects", []):
                # Build the dictionary for each object
                obj_dict = {
                    "NAME": obj.get("name"),
                    "TYPE": obj.get("type"),
                    "PACKAGE": obj.get("package", None),  # Default to None if not present
                    "SCHEMA": obj.get("owner"),
                    "CUSTOM": obj.get("custom", None)  # Default to None if not present
                }
                objects_list.append(obj_dict)

        return objects_list

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")
