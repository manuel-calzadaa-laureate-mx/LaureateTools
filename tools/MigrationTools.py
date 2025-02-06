from enum import Enum

from files.ObjectAddonsFile import refactor_table_columns, refactor_table_comments, refactor_table_indexes, \
    read_custom_table_data, ObjectAddonType


class ObjectType(Enum):
    TABLE = "TABLE"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    SEQUENCE = "SEQUENCE"


def convert_object_owner(object_owner: str) -> str:
    """
    Converts the object owner to the appropriate format for Banner 9.

    Args:
        object_owner (str): The original object owner.

    Returns:
        str: The converted object owner.
    """
    owner_map = {
    }
    # Use the mapping if available; otherwise, return the original owner
    return owner_map.get(object_owner.upper(), object_owner)


def convert_table_name_to_banner9(object_owner: str, object_name: str) -> dict[str, str]:
    """
    Converts a table name and owner to the Banner 9 format.

    Args:
        object_owner (str): The original owner of the table.
        object_name (str): The original table name.

    Returns:
        dict: A dictionary containing the converted owner and name.
    """
    # Convert the table name if it is custom (second letter is 'Z')
    return_object_name = (
        "TZTB" + object_name[2:]
        if len(object_name) > 1 and object_name[1] == 'Z'
        else object_name
    )

    # Use the helper function to convert the owner
    return_object_owner = convert_object_owner(object_owner)
    return {"name": return_object_name, "owner": return_object_owner}


def convert_procedure_name_to_banner9(object_owner: str, object_name: str) -> dict[str, str]:
    return {"name": object_name, "owner": object_owner}


def convert_function_name_to_banner9(object_owner: str, object_name: str) -> dict[str, str]:
    return {"name": object_name, "owner": object_owner}


def convert_sequence_name_to_banner9(object_owner: str, object_name: str) -> dict[str, str]:
    return {"name": object_name, "owner": object_owner}


# Main conversion function
def convert_object_to_banner9(object_type: ObjectType, object_owner: str, object_name: str) -> dict[str, str]:
    """
    Converts an object name to the Banner 9 format based on its type.

    Args:
        object_type (ObjectType): The type of the object (e.g., TABLE, PROCEDURE).
        object_owner (str): The original owner of the object.
        object_name (str): The original object name.

    Returns:
        dict: The converted object in Banner 9 format.
    """

    # Map object types to their respective conversion functions
    conversion_map = {
        ObjectType.TABLE: convert_table_name_to_banner9,
        ObjectType.PROCEDURE: convert_procedure_name_to_banner9,
        ObjectType.FUNCTION: convert_function_name_to_banner9,
        ObjectType.SEQUENCE: convert_sequence_name_to_banner9,
    }

    # Get the appropriate conversion function for the object type
    conversion_func = conversion_map.get(object_type)
    if not conversion_func:
        raise ValueError(f"Unsupported object type: {object_type}")

    # Call the conversion function with the object details
    converted_object = conversion_func(object_owner, object_name)

    # Ensure the result is returned as expected
    return {
        "type": object_type.value.lower(),
        "owner": converted_object["owner"],
        "name": converted_object["name"],
    }


def migrate_b7_table_to_b9(json_data: dict, b7_table_name: str, b9_table_name: str,
                           b9_owner: str = "UVM"):
    """
    Adds a new table object to the specified environment in the JSON data.

    Args:
        json_data (dict): The JSON data as a Python dictionary.
        b7_table_name (str): The name of the original table to copy.
        b9_owner (str): The owner of the new table.
        b9_table_name (str): The name of the new table.
        new_environment (str): The target environment to add the new table to.

    Returns:
        dict: Updated JSON data.
        :param b9_table_name:
        :param b7_table_name:
        :param json_data:
        :param b9_owner:
    """
    # Find the original table
    original_table = None
    for env in json_data.get("root", []):
        for obj in env.get("objects", []):
            if obj.get("name") == b7_table_name:
                original_table = obj
                break
        if original_table:
            break

    if not original_table:
        raise ValueError(f"Original table '{b7_table_name}' not found in the JSON data.")

    columns = refactor_table_columns(original_table.get("columns", []), b7_table_name, b9_table_name)
    comments = refactor_table_comments(original_table.get("comments", {}), b7_table_name, b9_table_name)
    indexes = refactor_table_indexes(original_table.get("indexes", []), b7_table_name, b9_table_name)
    sequences = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.SEQUENCES)
    triggers = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.TRIGGERS)
    grants = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.GRANTS)
    synonym = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.SYNONYMS)
    new_table = {
        "name": b9_table_name,
        "type": "TABLE",
        "owner": b9_owner,
        "custom": original_table.get("custom", True),
        "columns": columns["columns"],
        "attributes": original_table.get("attributes", {}),
        "comments": comments["comments"],
        "indexes": indexes["indexes"],
        "sequences": sequences["sequences"],
        "triggers": triggers["triggers"],
        "grants": grants["grants"],
        "synonym": synonym,
    }

    return new_table


if __name__ == "__main__":
    result = convert_object_to_banner9(ObjectType.TABLE, "SATURN", "GZXBLABLA")
    print(result)
