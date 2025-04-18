import json
import logging
import os
from enum import Enum

from db.database_properties import DatabaseEnvironment, DatabaseObject, TableObject
from db.datasource.sequence_datasource import fetch_attributes_for_sequences
from db.datasource.tables_datasource import fetch_table_columns_for_tables_grouped_by_schema_and_table_name, \
    fetch_table_attributes_for_tables_grouped_by_schema_and_table_name, \
    fetch_column_comments_for_tables_grouped_by_schema_and_table_name, \
    fetch_full_indexes_for_tables_grouped_by_schema_and_table_name
from db.datasource.triggers_datasource import fetch_triggers_elements_from_database, fetch_triggers_for_tables
from db.oracle_database_tools import OracleDBConnectionPool
from files.b7_dependency_file import get_dependencies_data
from files.mapping_file import MappingFileTypes, \
    get_filtered_mapping_data_by_type_and_is_mapped_for_banner7, \
    get_filtered_mapping_data_by_type_and_is_mapped_for_banner9
from files.tables_file import get_tables_by_environment
from tools.business_rules_tools import is_custom_table
from tools.file_tools import read_json_file, write_json_file
from tools.migration_tools import migrate_b7_table_to_b9, migrate_b9_table_to_b9

OBJECT_DATA_JSON = "../workfiles/b7_output/object_data.json"
MIGRATED_OBJECT_DATA_JSON = "../workfiles/b7_output/migrated_object_data.json"


class ObjectDataTypes(Enum):
    TABLE = "TABLE"
    SEQUENCE = "SEQUENCE"
    TRIGGER = "TRIGGER"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"


class ObjectOriginType(Enum):
    MANUAL = 'MANUAL'  ## objects added by new_table.csv file
    ADDON = 'ADDON'  ## added by object_addon.json file
    DEPENDENCY = 'DEPENDENCY'  ## added by dependency analysis


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

    # Ensure file exists and has valid JSON
    if os.path.exists(object_data_file) and os.path.getsize(object_data_file) > 0:
        with open(object_data_file, "r", encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                data = {"root": []}
    else:
        data = {"root": []}

    new_metadata = new_json_data if isinstance(new_json_data, dict) else json.loads(new_json_data)

    # Find or create environment entry
    for env in data["root"]:
        if env.get("environment").upper() == environment.name:
            if "objects" not in env:
                env["objects"] = []
            env["objects"].extend(new_metadata if isinstance(new_metadata, list) else [new_metadata])
            break
    else:
        # Environment doesn't exist, add it
        data["root"].append({
            "environment": environment.value,
            "objects": new_metadata if isinstance(new_metadata, list) else [new_metadata]
        })

    # Write back to the file
    with open(object_data_file, "w", encoding='utf-8') as file:
        json.dump(data, file, indent=4)


def add_or_update_object_data_file(environment: DatabaseEnvironment, new_json_data: dict):
    """
    Add new metadata or update an existing object within the specified environment.

    :param environment: Environment name to append/update metadata
    :param new_json_data: JSON string to append/update
    """
    object_data_file = get_migrated_object_data_file_path()

    # Ensure file exists and has valid JSON
    if os.path.exists(object_data_file) and os.path.getsize(object_data_file) > 0:
        with open(object_data_file, "r", encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                data = {"root": []}
    else:
        data = {"root": []}

    new_metadata = new_json_data if isinstance(new_json_data, dict) else json.loads(new_json_data)
    object_name = new_metadata.get("name")  # Assuming objects have a unique "name" field

    for env in data["root"]:
        if env.get("environment").upper() == environment.name:
            if "objects" not in env:
                env["objects"] = []

            # Check if object already exists, update it
            for obj in env["objects"]:
                if obj.get("name") == object_name:
                    obj.update(new_metadata)
                    break
            else:
                env["objects"].append(new_metadata)  # If not found, add new object
            break
    else:
        # Environment doesn't exist, add it with the new object
        data["root"].append({
            "environment": environment.value,
            "objects": [new_metadata]
        })

    # Write back to the file
    with open(object_data_file, "w", encoding='utf-8') as file:
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
) -> set[str]:
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

        return dependency_names

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")


def extract_triggers_from_database(db_pool: OracleDBConnectionPool, unique_triggers: [str]):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param db_pool:
    :param unique_triggers:
    """
    # Fetch metadata
    triggers = fetch_triggers_elements_from_database(db_pool=db_pool, trigger_names=unique_triggers)

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


def extract_sequences_attributes_from_database(db_pool: OracleDBConnectionPool, unique_sequences: [str]) -> dict:
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param db_pool:
    :param unique_sequences:
    """
    # Fetch metadata
    sequences = fetch_attributes_for_sequences(db_pool=db_pool, sequence_names=unique_sequences)

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


def get_object_data_mapped_by_names_by_environment_and_type(database_environment: DatabaseEnvironment,
                                                            object_data_type: str = "table") -> dict:
    object_data_dictionary = {}
    object_data = get_migrated_object_data()  # Fetch JSON data

    for one_root in object_data.get("root", []):  # Ensure "root" exists
        if one_root.get("environment") == database_environment.value:
            for one_object in one_root.get("objects", []):
                if one_object.get("type") == object_data_type:
                    name = one_object.get("name")
                    if name:
                        object_data_dictionary[name] = one_object

    return object_data_dictionary


def get_object_data_mapped_by_names_by_environment(
        database_environment: DatabaseEnvironment) -> dict:
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


def get_migrated_object_data() -> dict:
    config_file = get_migrated_object_data_file_path()
    return read_json_file(config_file)


def get_object_data_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, OBJECT_DATA_JSON)


def get_migrated_object_data_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, MIGRATED_OBJECT_DATA_JSON)


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


def extract_table_metadata_from_database(db_pool: OracleDBConnectionPool, table_names: [str]):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param connection:
    :param table_names: List of table names (e.g., ["SZTBLAN", "ANOTHER_TABLE"])
    :param output_file: The output file to save the generated JSON
    """
    # Fetch metadata
    columns = fetch_table_columns_for_tables_grouped_by_schema_and_table_name(db_pool=db_pool, table_names=table_names)
    attributes = fetch_table_attributes_for_tables_grouped_by_schema_and_table_name(db_pool=db_pool,
                                                                                    table_names=table_names)
    comments = fetch_column_comments_for_tables_grouped_by_schema_and_table_name(db_pool=db_pool,
                                                                                 table_names=table_names)
    indexes = fetch_full_indexes_for_tables_grouped_by_schema_and_table_name(db_pool=db_pool, table_names=table_names)
    triggers = fetch_triggers_for_tables(db_pool=db_pool, table_names=table_names)
    # sequences = fetch_sequences_names_for_tables(db_pool=db_pool, table_names=table_names)

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


def add_base_tables_manager(db_pool: OracleDBConnectionPool, database_environment: DatabaseEnvironment):
    logging.info("Starting: add base tables to object data")
    unique_tables = extract_unique_dependencies_types_from_data_file(
        database_object_type=DatabaseObject.TABLE,
        environment=database_environment,
        is_custom=False)

    if unique_tables:
        json_attributes_from_tables = extract_table_metadata_from_database(db_pool=db_pool, table_names=unique_tables)
        add_new_object_to_data_file(environment=database_environment, new_json_data=
        json_attributes_from_tables)
        logging.info(f"Added {len(unique_tables)} base tables to object data")

    else:
        logging.info("No unique base tables found. Skipping db operations.")
    logging.info("Ending: add base tables to object data")


def add_custom_sequences_manager(db_pool: OracleDBConnectionPool, database_environment: DatabaseEnvironment):
    logging.info("Starting: add custom sequences to object data")

    unique_sequences = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.SEQUENCE,
                                                                        environment=database_environment)

    # Check if the list is not empty
    if unique_sequences:
        # Proceed only if unique_sequences is not empty
        json_attributes_from_sequences = extract_sequences_attributes_from_database(db_pool=db_pool,
                                                                                    unique_sequences=unique_sequences)
        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_sequences)
        logging.info(f"Added {len(unique_sequences)} custom sequences to object data")

    else:
        logging.info("No unique sequences found. Skipping db operations.")
    logging.info("Ending: add custom sequences to object data")


def add_custom_tables_manager(db_pool: OracleDBConnectionPool,
                              database_environment: DatabaseEnvironment):
    logging.info("Starting: add custom tables to object data")

    unique_tables = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.TABLE,
                                                                     environment=database_environment,
                                                                     is_custom=True)
    additional_tables = get_tables_by_environment(database_environment=database_environment)
    if additional_tables:
        unique_tables.update(additional_tables)

    if unique_tables:
        json_attributes_from_tables = extract_table_metadata_from_database(db_pool=db_pool, table_names=unique_tables)
        add_new_object_to_data_file(database_environment, new_json_data=
        json_attributes_from_tables)
    else:
        logging.info("No unique custom tables found. Skipping db operations.")
    logging.info("Ending: add custom tables to object data")


def add_custom_triggers_manager(db_pool: OracleDBConnectionPool):
    logging.info("Starting: add custom tables to object data")

    unique_triggers = extract_table_unique_dependencies_types_from_data_file(table_object_type=TableObject.TRIGGER,
                                                                             environment=DatabaseEnvironment.BANNER7)
    # Check if the list is not empty
    if unique_triggers:
        # Proceed only if unique_triggers is not empty
        json_attributes_from_triggers = extract_triggers_from_database(db_pool=db_pool,
                                                                       unique_triggers=unique_triggers)

        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_triggers)
    else:
        print("No unique triggers found. Skipping db operations.")
    logging.info("Ending: add custom tables to object data")


def migrate_banner7_tables_manager(database_environment: DatabaseEnvironment):
    filtered_migration_data = get_filtered_mapping_data_by_type_and_is_mapped_for_banner7(
        mapping_object_types=MappingFileTypes.TABLE)
    for one_migration_data in filtered_migration_data:
        b7_table_name = one_migration_data.get("B7_NOMBRE", '')
        b9_paquete = one_migration_data.get("B9_PAQUETE", '')
        b9_nombre = one_migration_data.get("B9_NOMBRE")
        b9_esquema = one_migration_data.get("B9_ESQUEMA")

        object_data = get_object_data()
        converted_table_data = migrate_b7_table_to_b9(json_data=object_data,
                                                      b7_table_name=b7_table_name,
                                                      b9_table_name=b9_nombre,
                                                      b9_owner=b9_esquema)

        add_or_update_object_data_file(new_json_data=converted_table_data,
                                       environment=database_environment)


def migrate_banner9_tables_manager(database_environment: DatabaseEnvironment):
    filtered_migration_data = get_filtered_mapping_data_by_type_and_is_mapped_for_banner9(
        mapping_object_types=MappingFileTypes.TABLE)

    for one_migration_data in filtered_migration_data:
        b9_nombre = one_migration_data.get("B9_NOMBRE")
        b9_esquema = one_migration_data.get("B9_ESQUEMA")

        object_data = get_object_data()
        converted_table_data = migrate_b9_table_to_b9(json_data=object_data,
                                                      b9_table_name=b9_nombre,
                                                      b9_owner=b9_esquema)

        add_or_update_object_data_file(new_json_data=converted_table_data,
                                       environment=database_environment)
