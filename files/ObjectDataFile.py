import json
import logging
import os
from enum import Enum

from db.DatabaseProperties import DatabaseEnvironment, DatabaseObject, TableObject
from db.OracleDatabaseTools import OracleDBConnectionPool
from db.datasource.SequenceDatasource import fetch_attributes_for_sequences
from db.datasource.TablesDatasource import fetch_table_columns_for_tables_grouped_by_schema_and_table_name, \
    fetch_table_attributes_for_tables_grouped_by_schema_and_table_name, \
    fetch_column_comments_for_tables_grouped_by_schema_and_table_name, \
    fetch_full_indexes_for_tables_grouped_by_schema_and_table_name
from db.datasource.TriggersDatasource import fetch_triggers_elements_from_database, fetch_triggers_for_tables
from files.B9DependencyFile import get_dependencies_data
from files.ObjectAddonsFile import read_custom_data, GrantType, ObjectAddonType
from files.TablesFile import get_tables_by_environment
from tools.BusinessRulesTools import is_custom_table
from tools.CommonTools import ObjectOriginType, ObjectTargetType
from tools.FileTools import read_json_file, write_json_file
from tools.MigrationTools import migrate_b9_table_to_b9, migrate_sequence_to_b9, migrate_trigger_to_b9

OBJECT_DATA_JSON = "../workfiles/b9_output/object_data.json"
MIGRATED_OBJECT_DATA_JSON = "../workfiles/b9_output/migrated_object_data.json"


class ObjectDataTypes(Enum):
    TABLE = "TABLE"
    SEQUENCE = "SEQUENCE"
    TRIGGER = "TRIGGER"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    PACKAGE = "PACKAGE"


def create_object_base_manager():
    dependencies_data = get_dependencies_data()
    object_data = _convert_dependencies_file_to_json_object(dependencies_data=dependencies_data)
    write_json_file(json_data=object_data, output_filename=get_object_data_file_path())


def _convert_dependencies_file_to_json_object(dependencies_data: list[dict]) -> dict:
    # Initialize the structure for the JSON output
    json_data = {
        "root": [
            {
                "environment": "banner9",
                "objects": []
            }
        ]
    }

    # Temporary storage for objects
    objects_dict = {}

    # Process each row in the CSV
    for row in dependencies_data:
        obj_status = row['STATUS']

        if obj_status == ObjectTargetType.MISSING:
            continue

        obj_owner = row['OBJECT_OWNER']
        obj_type = row['OBJECT_TYPE']
        obj_package = row['OBJECT_PACKAGE']
        obj_name = row['OBJECT_NAME']
        dep_type = row['DEPENDENCY_TYPE']
        dep_name = row['DEPENDENCY_NAME']
        dep_package = row['DEPENDENCY_PACKAGE']

        # Initialize the object if it doesn't exist
        if obj_name not in objects_dict:
            objects_dict[obj_name] = {
                "object_status": obj_status,
                "origin": ObjectOriginType.DEPENDENCY.value,
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
                objects_dict[obj_name]["dependencies"]["tables"].append(
                    {"type": "TABLE", "package": None, "name": dep_name, "custom": True, "local": None,
                     "deployment": None, "object_status": None})
            else:
                objects_dict[obj_name]["dependencies"]["tables"].append(
                    {"type": "TABLE", "package": None, "name": dep_name, "custom": False, "local": None,
                     "deployment": None, "object_status": None})
        elif dep_type == "LOCAL_FUNCTION":
            objects_dict[obj_name]["dependencies"]["functions"].append(
                {"type": "FUNCTION", "package": None, "name": dep_name, "custom": None, "local": True,
                 "deployment": None, "object_status": None})
        elif dep_type == "FUNCTION":
            objects_dict[obj_name]["dependencies"]["functions"].append(
                {"type": "FUNCTION", "package": dep_package, "name": dep_name, "custom": None, "local": False,
                 "deployment": None, "object_status": obj_status})
        elif dep_type == "SEQUENCE":
            objects_dict[obj_name]["dependencies"]["sequences"].append(
                {"type": "SEQUENCE", "package": None, "name": dep_name, "custom": None, "local": None,
                 "deployment": "external", "object_status": None})
        elif dep_type == "PROCEDURE":
            objects_dict[obj_name]["dependencies"]["procedures"].append(
                {"type": "PROCEDURE", "package": dep_package, "name": dep_name, "custom": None, "local": None,
                 "deployment": None, "object_status": obj_status})
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
    with open(object_data_file, "w") as file:
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


def extract_unique_object_types_from_data_file(
        environment: DatabaseEnvironment,
        database_object_type: DatabaseObject,
        is_custom: bool = True
) -> set[str]:
    """
    Extracts all unique object names from a JSON file filtered by a specific environment and object type.

    Parameters:
        environment (DatabaseEnvironment): The environment to filter by.
        is_custom (bool, optional): Filter for custom dependencies if the object type is 'TABLE'.

    Returns:
        list: A sorted list of unique dependency names.
        :param environment:
        :param is_custom:
        :param database_object_type:
    """

    data = get_full_object_data()

    # Ensure root exists and is a list
    if 'root' not in data or not isinstance(data['root'], list):
        raise ValueError("Invalid JSON structure: 'root' key not found or not a list.")

    object_names = set()
    # Iterate through root objects and filter by environment
    for item in data['root']:
        if item.get('environment').upper() == environment.name:
            objects = item.get('objects', [])
            for obj in objects:
                object_type = obj.get("type")
                if object_type == database_object_type.name and obj.get('custom') == is_custom:
                    object_names.add(obj.get('name').upper())

    return object_names


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

    data = get_full_object_data()

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


def extract_triggers_from_database(db_pool: OracleDBConnectionPool,
                                   unique_triggers: [str],
                                   object_origin: ObjectOriginType):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param object_origin:
    :param db_pool:
    :param unique_triggers:
    """
    # Fetch metadata
    triggers = fetch_triggers_elements_from_database(db_pool=db_pool, trigger_names=unique_triggers)

    # Construct JSON structure
    triggers_metadata = []
    for trigger in triggers:
        trigger_entry = {
            "object_status": ObjectTargetType.SKIP.value,
            "origin": object_origin.value,
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


def extract_sequences_attributes_from_database(db_pool: OracleDBConnectionPool,
                                               unique_sequences: [str],
                                               object_origin: ObjectOriginType) -> dict:
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param object_origin:
    :param db_pool:
    :param unique_sequences:
    """
    # Fetch metadata
    sequences = fetch_attributes_for_sequences(db_pool=db_pool, sequence_names=unique_sequences)

    # Construct JSON structure
    sequence_metadata = []
    for sequence in sequences:
        sequence_entry = {
            "status": ObjectTargetType.INSTALL.value,
            "origin": object_origin.value,
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


def _get_generic_object_data_mapped_by_names_by_environment_and_type(
        database_environment: DatabaseEnvironment,
        data_fetcher: object = None,
        object_data_type: str = "TABLE"  # Function to fetch the JSON data
) -> dict:
    if data_fetcher is None:
        raise ValueError("A data fetcher function must be provided.")

    object_data_dictionary = {}
    object_data = data_fetcher  # Fetch JSON data using the provided function

    for one_root in object_data.get("root", []):  # Ensure "root" exists
        if one_root.get("environment") == database_environment.value:
            for one_object in one_root.get("objects", []):
                if one_object.get("type") == object_data_type:
                    name = one_object.get("name")
                    if name:
                        object_data_dictionary[name] = one_object

    return object_data_dictionary


def get_object_data_mapped_by_names_by_environment_and_type(
        database_environment: DatabaseEnvironment, object_data_type: str) -> dict:
    return _get_generic_object_data_mapped_by_names_by_environment_and_type(object_data_type=object_data_type,
                                                                            database_environment=database_environment,
                                                                            data_fetcher=get_full_object_data())


def get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment: DatabaseEnvironment, object_data_type: str) -> dict:
    return _get_generic_object_data_mapped_by_names_by_environment_and_type(object_data_type=object_data_type,
                                                                            database_environment=database_environment,
                                                                            data_fetcher=get_full_migrated_object_data())


def get_object_data_mapped_by_names_by_environment(
        database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> dict:
    object_data_dictionary = {}
    object_data = get_full_object_data()  # Fetch JSON data

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
    object_data = get_full_object_data()  # Fetch JSON data

    for one_root in object_data.get("root", []):  # Ensure "root" exists
        if one_root.get("environment") == database_environment.value:
            for one_object in one_root.get("objects", []):  # Ensure "objects" exists
                object_data_names.append(one_object.get("name", ""))  # Default to empty string if "name" is missing

    return object_data_names


def get_full_object_data() -> dict:
    config_file = get_object_data_file_path()
    return read_json_file(config_file)


def get_only_migrated_objects(database_environment: DatabaseEnvironment) -> list[dict]:
    config_file = get_migrated_object_data_file_path()
    object_data = read_json_file(config_file)
    root_data = object_data.get("root", {})
    only_objects = []
    for one_root_data in root_data:
        if one_root_data.get("environment", "") == database_environment.value:
            environment_objects = one_root_data.get("objects", {})
            for one_object in environment_objects:
                only_objects.append(one_object)

    return only_objects


def get_only_objects(database_environment: DatabaseEnvironment) -> list[dict]:
    config_file = get_object_data_file_path()
    object_data = read_json_file(config_file)
    root_data = object_data.get("root", {})
    only_objects = []
    for one_root_data in root_data:
        if one_root_data.get("environment", "") == database_environment.value:
            environment_objects = one_root_data.get("objects", {})
            for one_object in environment_objects:
                only_objects.append(one_object)

    return only_objects


def get_only_filtered_migrated_objects(database_environment: DatabaseEnvironment, object_type: ObjectDataTypes) -> list[
    dict]:
    config_file = get_migrated_object_data_file_path()
    object_data = read_json_file(config_file)
    root_data = object_data.get("root", {})
    only_objects = []
    for one_root_data in root_data:
        if one_root_data.get("environment", "") == database_environment.value:
            environment_objects = one_root_data.get("objects", {})
            for one_object in environment_objects:
                if one_object.get("type") == object_type.value:
                    only_objects.append(one_object)

    return only_objects


def get_only_filtered_objects(database_environment: DatabaseEnvironment, object_type: ObjectDataTypes) -> list[dict]:
    config_file = get_object_data_file_path()
    object_data = read_json_file(config_file)
    root_data = object_data.get("root", {})
    only_objects = []
    for one_root_data in root_data:
        if one_root_data.get("environment", "") == database_environment.value:
            environment_objects = one_root_data.get("objects", {})
            for one_object in environment_objects:
                if one_object.get("type") == object_type.value:
                    only_objects.append(one_object)

    return only_objects


def get_full_migrated_object_data() -> dict:
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


def extract_table_metadata_from_database(db_pool: OracleDBConnectionPool,
                                         table_names: [str],
                                         object_origin: ObjectOriginType = ObjectOriginType.DEPENDENCY):
    """
    Generate a JSON file containing metadata for given tables across all accessible schemas.

    :param object_origin:
    :param db_pool:
    :param table_names: List of table names (e.g., ["SZTBLAN", "ANOTHER_TABLE"])
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
            custom_table = is_custom_table(table_name)
            table_entry = {
                "object_status": ObjectTargetType.INSTALL.value if custom_table else ObjectTargetType.SKIP.value,
                "origin": object_origin.value,
                "name": table_name,
                "type": "TABLE",
                "owner": schema,
                "custom": custom_table,
                "columns": columns[schema].get(table_name, []),
                "attributes": attributes.get(schema, {}).get(table_name, {}),
                "comments": transformed_comments,
                "indexes": indexes.get(schema, {}).get(table_name, []),
                "sequences": [],
                "triggers": get_trigger_names_and_status(triggers=triggers, schema=schema, table_name=table_name)
            }
            table_metadata.append(table_entry)

    return json.dumps(table_metadata, indent=4)


def add_base_tables_manager(db_pool: OracleDBConnectionPool, database_environment=DatabaseEnvironment):
    logging.info("Starting: add base tables to object data")
    unique_tables = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.TABLE,
                                                                     environment=database_environment,
                                                                     is_custom=False)

    if unique_tables:
        json_attributes_from_tables = extract_table_metadata_from_database(db_pool=db_pool, table_names=unique_tables,
                                                                           object_origin=ObjectOriginType.DEPENDENCY)
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
                                                                                    unique_sequences=unique_sequences,
                                                                                    object_origin=ObjectOriginType.DEPENDENCY)
        add_new_object_to_data_file(environment=database_environment, new_json_data=
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
        json_attributes_from_additional_tables = extract_table_metadata_from_database(db_pool=db_pool,
                                                                                      table_names=additional_tables,
                                                                                      object_origin=ObjectOriginType.MANUAL)
        add_new_object_to_data_file(database_environment, new_json_data=
        json_attributes_from_additional_tables)

    if unique_tables:
        json_attributes_from_unique_tables = extract_table_metadata_from_database(db_pool=db_pool,
                                                                                  table_names=unique_tables,
                                                                                  object_origin=ObjectOriginType.DEPENDENCY)
        add_new_object_to_data_file(database_environment, new_json_data=
        json_attributes_from_unique_tables)
    else:
        logging.info("No unique custom tables found. Skipping db operations.")
    logging.info("Ending: add custom tables to object data")


def add_custom_triggers_manager(db_pool: OracleDBConnectionPool, database_environment: DatabaseEnvironment):
    logging.info("Starting: add custom tables to object data")

    unique_triggers = extract_table_unique_dependencies_types_from_data_file(table_object_type=TableObject.TRIGGER,
                                                                             environment=database_environment)
    # Check if the list is not empty
    if unique_triggers:
        json_attributes_from_triggers = extract_triggers_from_database(db_pool=db_pool,
                                                                       unique_triggers=unique_triggers,
                                                                       object_origin=ObjectOriginType.DEPENDENCY)

        add_new_object_to_data_file(environment=database_environment, new_json_data=
        json_attributes_from_triggers)
    else:
        print("No unique triggers found. Skipping db operations.")
    logging.info("Ending: add custom tables to object data")


def migrate_sequences_manager(database_environment: DatabaseEnvironment):
    unique_sequences = extract_unique_dependencies_types_from_data_file(environment=database_environment,
                                                                        database_object_type=DatabaseObject.SEQUENCE,
                                                                        is_custom=True)

    sequence_object_data = get_object_data_mapped_by_names_by_environment_and_type(
        object_data_type=ObjectDataTypes.SEQUENCE.value, database_environment=database_environment)

    for one_sequence in unique_sequences:

        current_sequence = sequence_object_data[one_sequence]
        sequence_name = current_sequence["name"]
        grants = read_custom_data(grant_type=GrantType.SEQUENCE, object_addon_type=ObjectAddonType.GRANTS,
                                  b9_object_name=sequence_name, b9_object_owner="UVM")

        synonyms = read_custom_data(object_addon_type=ObjectAddonType.SYNONYMS, b9_object_name=sequence_name,
                                    b9_object_owner="UVM")

        if current_sequence:
            new_sequence = {
                "origin": current_sequence.get("origin", ObjectOriginType.DEPENDENCY.value),
                "owner": current_sequence["owner"],
                "name": sequence_name,
                "type": current_sequence["type"],
                "deployment": current_sequence["deployment"],
                "min_value": current_sequence["min_value"],
                "max_value": current_sequence["max_value"],
                "increment_by": current_sequence["increment_by"],
                "cycle_flag": current_sequence["cycle_flag"],
                "order_flag": current_sequence["order_flag"],
                "cache_size": current_sequence["cache_size"],
                "last_number": current_sequence["last_number"],
                "grants": grants["grants"],
                "synonym": synonyms,
            }

            add_or_update_object_data_file(environment=database_environment, new_json_data=new_sequence)


def migrate_tables_manager(database_environment: DatabaseEnvironment):
    unique_dependency_tables = extract_unique_dependencies_types_from_data_file(environment=database_environment,
                                                                                database_object_type=DatabaseObject.TABLE,
                                                                                is_custom=True)
    unique_object_tables = extract_unique_object_types_from_data_file(environment=database_environment,
                                                                      database_object_type=DatabaseObject.TABLE,
                                                                      is_custom=True)

    object_data = get_full_object_data()
    unique_tables = unique_dependency_tables.union(unique_object_tables)

    for one_table in unique_tables:
        b9_nombre = one_table
        b9_esquema = "UVM"
        converted_table_data = migrate_b9_table_to_b9(json_data=object_data,
                                                      b9_table_name=b9_nombre,
                                                      b9_owner=b9_esquema)

        add_or_update_object_data_file(new_json_data=converted_table_data,
                                       environment=database_environment)


def migrate_packages_manager(database_environment: DatabaseEnvironment):
    packages_from_object_data = get_object_data_mapped_by_names_by_environment_and_type(
        object_data_type=DatabaseObject.PACKAGE.name, database_environment=database_environment)

    for package_name, package_dependencies in packages_from_object_data.items():
        object_status = package_dependencies.get("object_status", ObjectTargetType.SKIP.value)
        if object_status == ObjectTargetType.INSTALL.value:
            grants = read_custom_data(grant_type=GrantType.PACKAGE, object_addon_type=ObjectAddonType.GRANTS,
                                      b9_object_name=package_name, b9_object_owner="UVM")
            synonyms = read_custom_data(object_addon_type=ObjectAddonType.SYNONYMS, b9_object_name=package_name,
                                        b9_object_owner="UVM")

            # Add grants and synonyms to the package data
            packages_from_object_data[package_name]["grants"] = grants["grants"]
            packages_from_object_data[package_name]["synonym"] = synonyms

            add_or_update_object_data_file(new_json_data=packages_from_object_data[package_name],
                                           environment=database_environment)


def migrate_addon_sequences_manager(database_environment: DatabaseEnvironment):
    unique_dependency_tables = extract_unique_dependencies_types_from_data_file(environment=database_environment,
                                                                                database_object_type=DatabaseObject.TABLE,
                                                                                is_custom=True)
    unique_object_tables = extract_unique_object_types_from_data_file(environment=database_environment,
                                                                      database_object_type=DatabaseObject.TABLE,
                                                                      is_custom=True)

    unique_tables = unique_dependency_tables.union(unique_object_tables)

    for one_table in unique_tables:
        b9_nombre = one_table
        b9_esquema = "UVM"
        custom_sequences_addon_data = migrate_sequence_to_b9(b9_table_name=b9_nombre,
                                                             b9_owner=b9_esquema)
        for custom_sequence_addon_data in custom_sequences_addon_data:
            add_or_update_object_data_file(new_json_data=custom_sequence_addon_data,
                                           environment=database_environment)


def migrate_addon_triggers_manager(database_environment: DatabaseEnvironment):
    unique_dependency_tables = extract_unique_dependencies_types_from_data_file(environment=database_environment,
                                                                                database_object_type=DatabaseObject.TABLE,
                                                                                is_custom=True)
    unique_object_tables = extract_unique_object_types_from_data_file(environment=database_environment,
                                                                      database_object_type=DatabaseObject.TABLE,
                                                                      is_custom=True)

    unique_tables = unique_dependency_tables.union(unique_object_tables)

    for one_table in unique_tables:
        b9_nombre = one_table
        b9_esquema = "UVM"
        custom_sequences_addon_data = migrate_trigger_to_b9(b9_table_name=b9_nombre,
                                                            b9_owner=b9_esquema)

        for custom_sequence_addon_data in custom_sequences_addon_data:
            add_or_update_object_data_file(new_json_data=custom_sequence_addon_data,
                                           environment=database_environment)


def migrate_functions_manager(database_environment: DatabaseEnvironment):
    object_data = get_only_filtered_objects(database_environment=database_environment,
                                            object_type=ObjectDataTypes.FUNCTION)

    for one_object_data in object_data:
        if one_object_data.get("object_status") == ObjectTargetType.INSTALL.value:
            one_object_data = filter_dependencies(one_object_data)
            add_or_update_object_data_file(environment=database_environment, new_json_data=one_object_data)


def migrate_procedures_manager(database_environment: DatabaseEnvironment):
    object_data = get_only_filtered_objects(database_environment=database_environment,
                                            object_type=ObjectDataTypes.PROCEDURE)

    for one_object_data in object_data:
        if one_object_data.get("object_status") == ObjectTargetType.INSTALL.value:
            one_object_data = filter_dependencies(one_object_data)
            add_or_update_object_data_file(environment=database_environment, new_json_data=one_object_data)


def filter_dependencies(data):
    """
    Filters out non-custom tables from the dependencies.

    Args:
        data (dict): The input JSON data containing dependencies.

    Returns:
        dict: The modified data with non-custom tables removed.
    """
    if 'dependencies' in data:
        if 'tables' in data['dependencies']:
            # Filter out tables where 'custom' is False
            data['dependencies']['tables'] = [
                table for table in data['dependencies']['tables']
                if table.get('custom', True)  # Default to True if 'custom' key doesn't exist
            ]
        if 'functions' in data['dependencies']:
            # Filter out functions where 'Object Status' is not INSTALL
            data['dependencies']['functions'] = [
                function for function in data['dependencies']['functions']
                if function.get('object_status', ObjectTargetType.SKIP.value) == ObjectTargetType.INSTALL.value
            ]
        if 'procedures' in data['dependencies']:
            # Filter out procedure where 'Object Status' is not INSTALL
            data['dependencies']['procedures'] = [
                procedure for procedure in data['dependencies']['procedures']
                if procedure.get('object_status', ObjectTargetType.SKIP.value) == ObjectTargetType.INSTALL.value
            ]
    return data
