from enum import Enum


class Environment(Enum):
    BANNER7 = "banner7"
    BANNER9 = "banner9"


# Example usage
def add_environment_to_json(file_path, new_environment: Environment):
    """
    Adds a new environment object with an empty 'objects' array to the JSON file
    under the 'root' key if it doesn't already exist.

    Args:
        file_path (str): Path to the JSON file.
        new_environment (Environment): The environment to add.

    Returns:
        None
    """
    import json
    try:
        # Read the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Check if the environment already exists
        if not any(env.get("environment") == new_environment.value for env in data.get("root", [])):
            # Add the new environment object
            data["root"].append({
                "environment": new_environment.value,
                "objects": []
            })
            # Write back to the JSON file
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"Environment '{new_environment.value}' added.")
        else:
            print(f"Environment '{new_environment.value}' already exists.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error processing the JSON file: {e}")


file_path = "../../object_data.json"
add_environment_to_json(file_path, Environment.BANNER9)
