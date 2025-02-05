import json
import os
from typing import Dict

import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment, DatabaseObject, TableObject
from db.OracleDatabaseTools import get_db_connection
from db.datasource.SequenceDatasource import fetch_attributes_for_sequences
from db.datasource.TablesDatasource import fetch_table_columns_for_tables_grouped_by_schema_and_table_name, \
    fetch_table_attributes_for_tables_grouped_by_schema_and_table_name, \
    fetch_column_comments_for_tables_grouped_by_schema_and_table_name, \
    fetch_full_indexes_for_tables_grouped_by_schema_and_table_name
from db.datasource.TriggersDatasource import fetch_triggers_elements_from_database, fetch_triggers_for_tables
from files.DependencyFile import get_dependencies_data
from files.MappingFile import get_mapping_data
from tools.BusinessRulesTools import is_custom_table
from tools.FileTools import read_json_file, write_json_file
from tools.MigrationTools import migrate_b7_table_to_b9

OBJECT_DATA_JSON = "../workfiles/object_data.json"


def create_object_base_manager():
    dependencies_data = get_dependencies_data()
    object_data = _convert_dependencies_file_to_json_object(dependencies_data=dependencies_data)
    write_json_file(json_data=object_data, output_filename=get_object_data_file_path())


def _convert_dependencies_file_to_json_object(dependencies_data: list[dict]) -> dict:
    # Initialize the structure for the JSON output
    json_data = {
        "root": [
            {
                "environment": "banner7",
                "objects": []
            }
        ]
    }

    # Temporary storage for objects
    objects_dict = {}

    # Process each row in the CSV
    for row in dependencies_data:
        obj_status = row['STATUS']

        if obj_status != "OK":
            continue

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
                "package": obj_package,
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
            objects_dict[obj_name]["dependencies"]["sequences"].append({"name": dep_name, "deployment": "external",
                                                                        })
        elif dep_type == "PROCEDURE":
            objects_dict[obj_name]["dependencies"]["procedures"].append({"name": dep_name})

    # Convert the dictionary to a list and add it to json_data
    json_data["root"][0]["objects"] = list(objects_dict.values())

    return json_data


def add_new_object_to_data_file(environment: DatabaseEnvironment, new_json_data: dict):
    """
    Append metadata JSON to the specified environment in the input JSON file.

    :param environment: Environment name to append the metadata to
    :param new_json_data: JSON string to append
    """
    object_data_file = get_object_data_file_path()
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


def add_or_update_object_in_data_file(environment: DatabaseEnvironment, new_object: Dict):
    """
    Add a new object to the specified environment in the JSON file, or update it if it already exists.

    :param environment: Environment name to append or update the object in
    :param new_object: The new object to add or update
    """
    add_new_environment(new_environment=environment)
    object_data_file = get_object_data_file_path()
    with open(object_data_file, "r") as file:
        data = json.load(file)

    # Find the target environment
    for env in data.get("root", []):
        if env.get("environment", "") == environment.value:
            env_objects = env.setdefault("objects", [])

            # Check if the object already exists by matching the "name" field
            for obj in env_objects:
                if obj.get("name") == new_object.get("name"):
                    # Update the existing object
                    obj.update(new_object)
                    break
            else:
                # If the object does not exist, add it
                env_objects.append(new_object)
            break
    else:
        # If the environment does not exist, add it
        data.setdefault("root", []).append({
            "environment": environment,
            "objects": [new_object]
        })

    # Write the updated data back to the file
    with open(object_data_file, "w") as file:
        json.dump(data, file, indent=4)


def extract_table_unique_dependencies_types_from_data_file(
        environment: DatabaseEnvironment,
        table_object_type: TableObject
) -> [str]:
    input_file_name = get_object_data_file_path()
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
    input_file_name = get_object_data_file_path()
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
            "deployment": "external",
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
            "deployment": "external",
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


def add_new_environment(new_environment: DatabaseEnvironment):
    """
    Reads a JSON file, checks if an environment exists, and adds it if it doesn't.

    :param file_path: Path to the JSON file.
    :param new_environment: The environment to add (e.g., "banner9").
    """
    file_path = get_object_data_file_path()
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


# def extract_all_objects_from_data_file(input_file_name: str) -> List[Dict[str, Optional[str]]]:
#     """
#     Extracts a list of objects from the JSON data with the specified structure.
#
#     Args:
#         json_data (dict): The JSON data containing the objects to be extracted.
#
#     Returns:
#         List[Dict[str, Optional[str]]]: A list of dictionaries with the extracted data.
#         :param input_file_name:
#     """
#     try:
#         with open(input_file_name, 'r') as file:
#             json_data = json.load(file)
#         objects_list = []
#
#         # Navigate through the JSON structure
#         for root_entry in json_data.get("root", []):
#             for obj in root_entry.get("objects", []):
#                 # Build the dictionary for each object
#                 obj_dict = {
#                     "NAME": obj.get("name"),
#                     "TYPE": obj.get("type"),
#                     "PACKAGE": obj.get("package", None),  # Default to None if not present
#                     "SCHEMA": obj.get("owner"),
#                     "CUSTOM": obj.get("custom", None)  # Default to None if not present
#                 }
#                 objects_list.append(obj_dict)
#
#         return objects_list
#
#     except FileNotFoundError:
#         raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
#     except json.JSONDecodeError:
#         raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")

def get_object_data_mapped_by_names_by_environment(
        database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> dict:
    object_data_dictionary = {}
    object_data = get_object_data()  # Fetch JSON data

    for one_root in object_data.get("root", []):  # Ensure "root" exists
        if one_root.get("environment") == database_environment.value:
            for one_object in one_root.get("objects", []):
                name = one_object.get("name")
                if name:
                    object_data_dictionary[name] = one_object

    return object_data_dictionary


def get_object_data_names_by_environment(
        database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7
) -> list[str]:
    object_data_names = []
    object_data = get_object_data()  # Fetch JSON data

    for one_root in object_data.get("root", []):  # Ensure "root" exists
        if one_root.get("environment") == database_environment.value:
            for one_object in one_root.get("objects", []):  # Ensure "objects" exists
                object_data_names.append(one_object.get("name", ""))  # Default to empty string if "name" is missing

    return object_data_names


def get_object_data() -> dict:
    config_file = get_object_data_file_path()
    return read_json_file(config_file)


def get_object_data_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, OBJECT_DATA_JSON)


def get_trigger_names_and_status(triggers: dict, schema: str, table_name: str):
    """
    Extract trigger names and their statuses for a given owner and table name.

    Args:
        triggers (dict): The nested dictionary of triggers grouped by owner and table name.
        schema (str): The schema/owner of the table.
        table_name (str): The name of the table.

    Returns:
        list: A list of dictionaries containing trigger names and statuses.
              Example: [{"trigger_name": "TRIGGER1", "status": "ENABLED"}, ...]
    """
    # Get the triggers for the specified owner and table name, defaulting to an empty list
    table_triggers = triggers.get(schema, {}).get(table_name, [])

    # Extract the trigger name and status for each trigger
    return [
        {"name": trigger["trigger_name"], "status": trigger["status"], "deployment": "external"}
        for trigger in table_triggers
    ]


def extract_table_metadata_from_database(connection: cx_Oracle.Connection, table_names: [str]):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param connection:
    :param table_names: List of table names (e.g., ["SZTBLAN", "ANOTHER_TABLE"])
    :param output_file: The output file to save the generated JSON
    """
    # Fetch metadata
    columns = fetch_table_columns_for_tables_grouped_by_schema_and_table_name(connection, table_names)
    attributes = fetch_table_attributes_for_tables_grouped_by_schema_and_table_name(connection, table_names)
    comments = fetch_column_comments_for_tables_grouped_by_schema_and_table_name(connection, table_names)
    indexes = fetch_full_indexes_for_tables_grouped_by_schema_and_table_name(connection, table_names)
    triggers = fetch_triggers_for_tables(connection, table_names)
    # sequences = fetch_sequences_names_for_tables(connection,table_names)

    # Construct JSON structure
    table_metadata = []
    for schema in columns.keys():
        for table_name in columns[schema].keys():
            raw_comments = comments[schema].get(table_name, {})
            transformed_comments = [
                {"name": column_name, "comment": comment}
                for column_name, comment in raw_comments.items()
            ]

            table_entry = {
                "name": table_name,
                "type": "TABLE",
                "owner": schema,
                "custom": is_custom_table(table_name),
                "columns": columns[schema].get(table_name, []),
                "attributes": attributes[schema].get(table_name, {}),
                "comments": transformed_comments,
                "indexes": indexes[schema].get(table_name, []),
                "sequences": [],
                "triggers": get_trigger_names_and_status(triggers=triggers, schema=schema, table_name=table_name)
            }
            table_metadata.append(table_entry)

    return json.dumps(table_metadata, indent=4)


def add_base_tables_manager():
    unique_tables = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.TABLE,
                                                                     environment=DatabaseEnvironment.BANNER7,
                                                                     is_custom=False)

    if unique_tables:
        connection = get_db_connection(DatabaseEnvironment.BANNER7)
        json_attributes_from_tables = extract_table_metadata_from_database(connection, unique_tables)
        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_tables)
    else:
        print("No unique base tables found. Skipping db operations.")


def add_custom_sequences_manager():
    unique_sequences = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.SEQUENCE,
                                                                        environment=DatabaseEnvironment.BANNER7)

    # Check if the list is not empty
    if unique_sequences:
        # Proceed only if unique_sequences is not empty
        connection = get_db_connection(DatabaseEnvironment.BANNER7)
        json_attributes_from_sequences = extract_sequences_attributes_from_database(connection, unique_sequences)
        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_sequences)
    else:
        print("No unique sequences found. Skipping db operations.")


def add_custom_tables_manager():
    unique_tables = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.TABLE,
                                                                     environment=DatabaseEnvironment.BANNER7,
                                                                     is_custom=True)

    if unique_tables:
        connection = get_db_connection(DatabaseEnvironment.BANNER7)
        json_attributes_from_tables = extract_table_metadata_from_database(connection, unique_tables)
        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_tables)
    else:
        print("No unique custom tables found. Skipping db operations.")


def add_custom_triggers_manager():
    unique_triggers = extract_table_unique_dependencies_types_from_data_file(table_object_type=TableObject.TRIGGER,
                                                                             environment=DatabaseEnvironment.BANNER7)
    # Check if the list is not empty
    if unique_triggers:
        # Proceed only if unique_triggers is not empty
        connection = get_db_connection(DatabaseEnvironment.BANNER7)
        json_attributes_from_triggers = extract_triggers_from_database(connection=connection,
                                                                       unique_triggers=unique_triggers)

        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_triggers)
    else:
        print("No unique triggers found. Skipping db operations.")


def migrate_table_manager():
    migration_data = get_mapping_data()
    for table_name_key in migration_data:
        b9_mapping_data = migration_data.get(table_name_key, {})
        b9_paquete = b9_mapping_data.get("B9_PAQUETE")
        b9_nombre = b9_mapping_data.get("B9_NOMBRE")
        b9_esquema = b9_mapping_data.get("B9_ESQUEMA")

        json_object_data = get_object_data()
        converted_table_data = migrate_b7_table_to_b9(json_data=json_object_data, b7_table_name=table_name_key,
                                                      b9_table_name=b9_nombre, b9_owner=b9_esquema)

        add_or_update_object_in_data_file(new_object=converted_table_data,
                                          environment=DatabaseEnvironment.BANNER9)
