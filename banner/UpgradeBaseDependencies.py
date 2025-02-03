import json

import cx_Oracle

from db.DatabaseProperties import DatabaseObject, DatabaseEnvironment, TableObject
from db.OracleDatabaseTools import get_db_connection
from db.datasource.TablesDatasource import fetch_table_columns_for_tables_grouped_by_schema_and_table_name, \
    fetch_table_attributes_for_tables_grouped_by_schema_and_table_name, \
    fetch_column_comments_for_tables_grouped_by_schema_and_table_name, \
    fetch_full_indexes_for_tables_grouped_by_schema_and_table_name
from db.datasource.TriggersDatasource import fetch_triggers_for_tables
from files.ObjectDataFile import create_object_base_manager, extract_unique_dependencies_types_from_data_file, \
    add_new_object_to_data_file, extract_sequences_attributes_from_database, \
    extract_table_unique_dependencies_types_from_data_file, extract_triggers_from_database
from tools.BusinessRulesTools import is_custom_table

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


if __name__ == "__main__":
    create_object_base_manager()
    add_base_tables_manager()
    add_custom_sequences_manager()
    add_custom_tables_manager()
    add_custom_triggers_manager()
