import json

def extract_unique_tables(input_file_name, environment):
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

        table_names = set()

        # Iterate through root objects and filter by environment
        for item in data['root']:
            if item.get('environment') == environment:
                objects = item.get('objects', [])
                for obj in objects:
                    dependencies = obj.get('dependencies', {})
                    tables = dependencies.get('tables', [])
                    for table in tables:
                        if table.get('custom'):
                            table_names.add(table.get('name').upper())

        return sorted(table_names)

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_file_name}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_file_name}' is not a valid JSON file.")

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
