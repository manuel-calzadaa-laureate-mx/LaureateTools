import csv
import json

import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection
from db.datasource.B7ToB9MappingDatasource import get_full_mapping_by_name_list
from tools.MigrationTools import convert_object_to_banner9, ObjectType
from tools.ObjectDataTools import extract_all_objects_from_data_file


def create_mapping_json(source_file):
    """
    Creates a mapping JSON structure from the source JSON data.

    Args:
        source_file (str): Path to the source JSON file.

    Returns:
        dict: The generated mapping JSON structure.
    """
    try:
        # Read the source JSON file
        with open(source_file, 'r') as file:
            source_data = json.load(file)

        # Initialize the mapping structure
        mapping = {"mapping": []}

        # Iterate over the source data to construct the mappings
        for root_item in source_data.get("root", []):
            source_environment = root_item.get("environment", "")
            for obj in root_item.get("objects", []):
                obj_type = obj.get("type", "")
                obj_owner = obj.get("owner", "")
                obj_name = obj.get("name", "")

                try:
                    # Attempt to convert object type string to ObjectType enum
                    object_type = ObjectType[obj_type].value
                    migrated_object = convert_object_to_banner9(object_type, obj_owner, obj_name)
                except KeyError:
                    # Handle cases where the object type is invalid
                    print(f"Invalid object type: {obj_type}. Skipping object.")

                # Construct the mapping entry
                mapping_entry = {
                    "type": obj_type,
                    "source": {
                        "environment": source_environment,
                        "owner": obj_owner,
                        "name": obj_name
                    },
                    "target": {
                        "environment": "banner9",
                        "owner": migrated_object["owner"],
                        "name": migrated_object["name"],
                    }
                }
                mapping["mapping"].append(mapping_entry)

        return mapping
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error processing the JSON file: {e}")
        return {}


def normalize_value(value : str):
    """
    Returns 'none' if the input value is 'N/A' or '[ SE ELIMINA ]',
    otherwise returns the original value.
    """
    if value in {"N/A", "[ SE ELIMINA ]", None, "", 'null'}:
        return "none"
    return value

def update_mapping_table_from_file() -> None :
    print("TODO")

def create_mapping_from_database(
        db_connection,
        object_data_input_file: str,
        output_csv: str
) -> None:
    """
    Check if names from the JSON file exist in the table and write them into a CSV file.

    Args:
        db_connection (cx_Oracle.Connection): Oracle database connection.
        object_data_input_file (dict): JSON-like data containing object definitions.
        output_csv (str): Path to the output CSV file.

    Returns:
        None: Outputs the CSV file.
    """
    # Extract object data from JSON
    object_data = extract_all_objects_from_data_file(object_data_input_file)

    # Extract all names from the JSON objects
    names_from_json = [obj["NAME"] for obj in object_data]

    # Use the existing function to fetch full mapping data
    matched_rows = get_full_mapping_by_name_list(names_from_json, db_connection)

    # Extract names that exist in the table
    existing_names = {row["GZTBTMPEO_B7_NOMBRE"] for row in matched_rows}

    # Create a mapping from NAME to object_data for quick lookups
    object_data_map = {obj["NAME"]: obj for obj in object_data}

    # Open the CSV file for writing
    with open(output_csv, mode="w", newline="") as file:
        writer = csv.writer(file)

        # Write the CSV header
        writer.writerow([
            "IS_MAPPED",
            "B7_TIPO", "B7_ESQUEMA", "B7_PAQUETE", "B7_NOMBRE",
            "B9_TIPO", "B9_ESQUEMA", "B9_PAQUETE", "B9_NOMBRE"
        ])

        # Write rows for each name extracted from the JSON file
        for name in names_from_json:
            if name in existing_names:
                # Find the corresponding row for the name
                matched_row = next(row for row in matched_rows if row["GZTBTMPEO_B7_NOMBRE"] == name)
                writer.writerow([
                    "true",
                    matched_row.get("GZTBTMPEO_B7_TIPO", ""),
                    matched_row.get("GZTBTMPEO_B7_ESQUEMA", ""),
                    normalize_value(matched_row.get("GZTBTMPEO_B7_PAQUETE", "")),
                    matched_row.get("GZTBTMPEO_B7_NOMBRE", ""),
                    matched_row.get("GZTBTMPEO_B9_TIPO", ""),
                    matched_row.get("GZTBTMPEO_B9_ESQUEMA", ""),
                    normalize_value(matched_row.get("GZTBTMPEO_B9_PAQUETE", "")),
                    normalize_value(matched_row.get("GZTBTMPEO_B9_NOMBRE", ""))
                ])
            else:
                # Use data from object_data if the name is not found
                obj = object_data_map.get(name, {})
                if obj["CUSTOM"] != False:
                    writer.writerow([
                        "false",
                        obj.get("TYPE", None),  # B7_TIPO
                        obj.get("SCHEMA", ""),  # B7_ESQUEMA
                        normalize_value(obj.get("PACKAGE", "")),  # B7_PAQUETE
                        obj.get("NAME", ""),  # B7_NOMBRE
                        obj.get("TYPE", ""),  # B9_TYPE (same as B7)
                        "UVM",  # B9_SCHEMA
                        "none",  # B9_PACKAGE
                        "none"  # B9_NAME
                    ])


if __name__ == "__main__":
    object_data = "../../object_data.json"
    config_file = '../../config/db_config.json'  # JSON file containing db credentials
    output_csv_tile = '../mapping.csv'
    # Load configuration and connect to the db
    connection = get_db_connection(DatabaseEnvironment.BANNER9)
    create_mapping_from_database(db_connection=connection, object_data_input_file=object_data, output_csv=output_csv_tile)
    connection.close()
