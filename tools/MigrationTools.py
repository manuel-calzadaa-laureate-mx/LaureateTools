from enum import Enum


class ObjectType(Enum):
    TABLE = "TABLE"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    SEQUENCE = "SEQUENCE"


def extract_table_info(table_name: str) -> dict:
    # Extract prefix: first two characters, where the second character must be "Z"
    if len(table_name) >= 2 and table_name[1] == "Z":
        prefix = table_name[:2]
    else:
        raise ValueError(f"Invalid table name: {table_name} does not meet the criteria.")

    # Extract base: check if "TB" exists, and use the part after it
    if "TB" in table_name:
        base_start = table_name.index("TB") + 2
        base = table_name[base_start:]
    else:
        # If "TB" is not present, use the part after the prefix
        base = table_name[2:]

    # Return the extracted values
    return {
        "prefix": prefix,
        "base": base,
        "table_name": table_name
    }


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


if __name__ == "__main__":
    result = convert_object_to_banner9(ObjectType.TABLE, "SATURN", "GZXBLABLA")
    print(result)
