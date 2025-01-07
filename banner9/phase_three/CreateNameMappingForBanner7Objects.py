import json

from tools.MigrationTools import convert_object_to_banner9, ObjectType


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


if __name__ == "__main__":
    object_data = "../../object_data.json"

    # object_list = read_all_objects_from_object_data(object_data)
    # mapping_data = create_mapping_json(object_data)

    # Print the generated mapping data
    # print(json.dumps(mapping_data, indent=4))
