import json

from db.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_attributes_from_tables
from tools.ObjectDataTools import extract_unique_tables


def append_metadata_to_json(input_file, environment, metadata_json):
    """
    Append metadata JSON to the specified environment in the input JSON file.

    :param input_file: Path to the input JSON file
    :param environment: Environment name to append the metadata to
    :param metadata_json: JSON string to append
    """
    with open(input_file, "r") as file:
        data = json.load(file)

    # Find the correct environment and append metadata
    for env in data.get("root", []):
        if env.get("environment") == environment:
            env["objects"].append(json.loads(metadata_json))
            break

    # Write back to the file
    with open(input_file, "w") as file:
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_tables = extract_unique_tables(object_data, "banner7")
    print(unique_tables)
    connection = get_connection("../../db_config.json", "banner7")

    json_attributes_from_tables = extract_attributes_from_tables(connection, unique_tables)
    append_metadata_to_json(object_data, "banner7", json_attributes_from_tables)

