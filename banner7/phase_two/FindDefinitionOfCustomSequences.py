import json

import cx_Oracle

from db.OracleDatabaseTools import get_connection
from db.SequenceDatasource import fetch_attributes_for_sequences
from tools.ObjectDataTools import append_metadata_to_json


def extract_unique_sequences(input_file_name, environment):
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
                if item.get('environment') == environment:
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

    :param connection:
    :param table_names: List of table names (e.g., ["SZTBLAN", "ANOTHER_TABLE"])
    :param output_file: The output file to save the generated JSON
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



if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_sequences = extract_unique_sequences(object_data, "banner7")
    connection = get_connection("../../db_config.json", "banner7")

    json_attributes_from_sequences = extract_attributes_from_sequences(connection, unique_sequences)
    append_metadata_to_json(object_data, "banner7", json_attributes_from_sequences)

