from enum import Enum

from db.DatabaseProperties import DatabaseEnvironment
from files.ObjectAddonsFile import read_custom_table_data, ObjectAddonType, get_custom_indexes
from files.TableColumnSubstituteFile import get_tables_column_substitute_file
from tools.CommonTools import MultiCounter, extract_table_info, refactor_tagged_text


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
    ## TZSEXXXXX
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


def migrate_b9_table_to_b9(json_data: dict, b9_table_name: str,
                           b9_owner: str = "UVM"):
    # Find the original table
    original_table = None
    for env in json_data.get("root", []):
        if env.get("environment") != DatabaseEnvironment.BANNER9.value:
            continue
        for obj in env.get("objects", []):
            if obj.get("name") == b9_table_name:
                original_table = obj
                break
        if original_table:
            break

    if not original_table:
        raise ValueError(f"Original table '{b9_table_name}' not found in the JSON data.")

    columns = _refactor_table_columns(original_table.get("columns", []), b9_table_name, b9_table_name)
    comments = _refactor_table_comments(original_table.get("comments", {}), b9_table_name, b9_table_name)
    indexes = _refactor_table_indexes(original_table.get("indexes", []), b9_table_name, b9_table_name)
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
    if not b7_table_name:
        return None

    # Find the original table
    original_table = None
    for env in json_data.get("root", []):
        if env.get("environment") != DatabaseEnvironment.BANNER7.value:
            continue
        for obj in env.get("objects", []):
            if obj.get("name") == b7_table_name:
                original_table = obj
                break
        if original_table:
            break

    if not original_table:
        raise ValueError(f"Original table '{b7_table_name}' not found in the JSON data.")

    columns = _refactor_table_columns(original_table.get("columns", []), b7_table_name, b9_table_name)
    comments = _refactor_table_comments(original_table.get("comments", {}), b7_table_name, b9_table_name)
    indexes = _refactor_table_indexes(original_table.get("indexes", []), b7_table_name, b9_table_name)
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


class IndexType(Enum):
    PRIMARY_KEY = 'KP'
    CHECK_CONSTRAINT = 'CK'
    FOREIGN_KEY = 'FK'
    UNIQUE_KEY = 'UK'
    INDEX = 'IX'


def _find_index_type(constraint_type: str | None, uniqueness: str) -> IndexType:
    if constraint_type:
        if uniqueness == "UNIQUE":
            if constraint_type == 'P':
                return IndexType.PRIMARY_KEY
            elif constraint_type == 'C':
                return IndexType.CHECK_CONSTRAINT
            elif constraint_type == 'R':
                return IndexType.FOREIGN_KEY
            elif constraint_type == 'U':
                return IndexType.UNIQUE_KEY
    elif uniqueness == "UNIQUE":
        return IndexType.UNIQUE_KEY  # Manually created unique index

    if uniqueness == "NONUNIQUE":
        return IndexType.INDEX

    raise ValueError(
        f"Couldn't find a proper IndexType for ConstraintType:{constraint_type} and Uniqueness:{uniqueness}")


def _refactor_table_indexes(b7_table_indexes: [dict], b7_table_name: str, b9_table_name: str) -> [dict]:
    updated_indexes = []
    counter = MultiCounter()
    extracted_table_info = extract_table_info(b9_table_name)
    prefix = extracted_table_info.get("prefix")
    base = extracted_table_info.get("base", "")

    ## PROCESS CUSTOM INDEXES
    custom_indexes = get_custom_indexes()
    custom_table_indexes = []
    if custom_indexes:
        custom_indexes_plain = custom_indexes["indexes"]
        for one_custom_index in custom_indexes_plain:
            updated_custom_index = one_custom_index.copy()

            constraint_type = one_custom_index.get("constraint_type", "")
            uniqueness = one_custom_index.get("uniqueness", "")
            index_type = _find_index_type(constraint_type=constraint_type, uniqueness=uniqueness)

            index_type_counter = counter.next(index_type.value)
            one_custom_index_original_name = one_custom_index["name"]
            new_custom_index_name = refactor_tagged_text(original_text=one_custom_index_original_name,
                                                         tags=["{prefix}", "{base}", "{serial}"],
                                                         replacement_text=[prefix, base, index_type_counter])

            updated_custom_index["name"] = new_custom_index_name
            updated_custom_index["columns"] = []

            updated_custom_index_columns = []
            custom_columns = one_custom_index.get("columns", [])

            for one_custom_column in custom_columns:
                updated_custom_index_column = one_custom_column.copy()
                column_name = one_custom_column.get("column_name")
                updated_custom_index_column["column_name"] = refactor_tagged_text(original_text=column_name,
                                                                                  tags=["{table}"],
                                                                                  replacement_text=[b9_table_name])
                updated_custom_index_columns.append(updated_custom_index_column)
            updated_custom_index["columns"] = updated_custom_index_columns
            custom_table_indexes.append(updated_custom_index)



    ## PROCESS NORMAL OCCURRING INDEXES
    for one_index in b7_table_indexes:
        skip_index = False
        updated_index = one_index.copy()

        ## CREATE THE COLUMNS
        updated_index["columns"] = []
        updated_index_columns = []
        columns = one_index.get("columns", [])
        for one_column in columns:
            updated_index_column = one_column.copy()
            column_name = _refactor_column_name(one_column.get("column_name"),
                                    b7_table_name=b7_table_name,
                                    b9_table_name=b9_table_name)
            updated_index_column["column_name"] = column_name

            ## IF THIS IS A CUSTOM INDEX SKIP IT
            is_custom_index = is_custom_index_column(column_name= column_name, custom_indexes= custom_table_indexes)
            if is_custom_index:
                skip_index = True
                break

            updated_index_columns.append(updated_index_column)

        if skip_index:
            continue

        ## CREATE THE BODY
        constraint_type = one_index.get("constraint_type", "")
        uniqueness = one_index.get("uniqueness", "")
        index_type = _find_index_type(constraint_type=constraint_type, uniqueness=uniqueness)
        index_type_counter = counter.next(index_type.value)
        new_index_name = prefix + index_type.value + base + "_" + index_type_counter

        updated_index["name"] = new_index_name

        updated_index["columns"] = updated_index_columns
        updated_indexes.append(updated_index)

    combined_indexes = updated_indexes + custom_table_indexes
    result = {"indexes": combined_indexes}
    return result

def is_custom_index_column(column_name: str, custom_indexes: list[dict])-> bool:
    for custom_index in custom_indexes:
        custom_columns = custom_index.get("columns", [])
        for custom_column in custom_columns:
            custom_column_name = custom_column.get("column_name","")
            if custom_column_name == column_name:
                return True
    return False



def _refactor_table_comments(b7_table_comments: list[dict], b7_table_name: str, b9_table_name: str) -> dict[
    str, list[dict]]:
    """
    Renames comment references based on table name changes and appends missing custom comments.

    Parameters:
        b7_table_comments (list[dict]): List of dictionaries representing table comments.
        b7_table_name (str): The old table name prefix.
        b9_table_name (str): The new table name prefix.

    Returns:
        dict[str, list[dict]]: A dictionary containing the updated list of comments.
    """
    updated_comments = []

    for comment in b7_table_comments:
        column_name = _refactor_column_name(comment['name'], b7_table_name, b9_table_name)

        # Update the comment dictionary
        updated_comment = comment.copy()
        updated_comment['name'] = column_name

        updated_comments.append(updated_comment)

    # Fetch all custom table comments
    custom_table_comments = read_custom_table_data(
        b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.COMMENTS
    )

    # Find missing custom comments
    missing_custom_comments = _find_missing_custom_comments(
        updated_comments=updated_comments, custom_comments=custom_table_comments
    )

    # Combine updated and missing comments
    return {"comments": updated_comments + missing_custom_comments}


def _find_missing_custom_comments(updated_comments: list[dict], custom_comments: dict[str, list[dict]]) -> list[dict]:
    """
    Finds comments from custom_comments that are missing in updated_comments.

    Parameters:
        updated_comments (list[dict]): The list of already processed comments.
        custom_comments (dict[str, list[dict]]): A dictionary containing a key "comments" with a list of custom comments.

    Returns:
        list[dict]: The list of missing custom comments.
    """
    updated_names = {comment["name"] for comment in updated_comments}

    # Ensure custom_comments has the expected structure
    if "comments" not in custom_comments or not isinstance(custom_comments["comments"], list):
        raise ValueError("custom_comments must be a dictionary containing a list under the key 'comments'.")

    return [comment for comment in custom_comments["comments"] if comment["name"] not in updated_names]


def _refactor_column_name(column_name: str, b7_table_name: str, b9_table_name: str) -> str:
    # Check if the name starts with old_table_name
    if not column_name.startswith(b7_table_name):
        # Prepend old_table_name if it does not start with it
        column_name = f"{b7_table_name}_{column_name}"
    # Replace old_table_name with new_table_name
    column_name = column_name.replace(b7_table_name, b9_table_name, 1)
    return column_name


def _refactor_table_columns(b7_table_columns: list[dict], b7_table_name: str, b9_table_name: str) -> dict[
    str, list[dict]]:
    """
    Renames columns based on table name changes and appends missing custom columns.

    Parameters:
        b7_table_columns (list[dict]): List of dictionaries representing columns.
        b7_table_name (str): The old table name prefix.
        b9_table_name (str): The new table name prefix.

    Returns:
        dict[str, list[dict]]: A dictionary containing the updated list of columns under the key 'columns'.
    """

    # Rename columns according to rules
    updated_columns = []
    for column in b7_table_columns:
        column_name = column["name"]

        # Ensure the name starts with the old table name
        if not column_name.startswith(b7_table_name):
            column_name = f"{b7_table_name}_{column_name}"

        # Replace the old table name with the new one (only first occurrence)
        column_name_preview = column_name.replace(b7_table_name, b9_table_name, 1)
        column_name = get_tables_column_substitute_file(table_name=b9_table_name, original_column=column_name_preview)

        updated_column = column.copy()
        updated_column["name"] = column_name
        updated_columns.append(updated_column)

    # Fetch all custom columns
    all_custom_table_columns = read_custom_table_data(
        b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.COLUMNS
    )

    # Find missing custom columns
    missing_custom_columns = _find_missing_custom_columns(
        updated_columns=updated_columns, custom_columns=all_custom_table_columns
    )

    # Return the final dictionary format
    return {"columns": updated_columns + missing_custom_columns["columns"]}


def _find_missing_custom_columns(updated_columns: list[dict], custom_columns: dict[str, list[dict]]) -> dict[
    str, list[dict]]:
    """
    Finds columns from custom_columns that are missing in updated_columns.

    Parameters:
        updated_columns (list[dict]): The list of already processed columns.
        custom_columns (dict[str, list[dict]]): A dictionary containing a key "columns" with a list of custom columns.

    Returns:
        dict[str, list[dict]]: A dictionary with the missing custom columns under the key 'columns'.
    """
    updated_names = {col["name"] for col in updated_columns}

    # Ensure custom_columns contains the "columns" key and is a list
    if "columns" not in custom_columns or not isinstance(custom_columns["columns"], list):
        raise ValueError("custom_columns must be a dictionary containing a list under the key 'columns'.")

    # Find missing columns
    missing_columns = [col for col in custom_columns["columns"] if col["name"] not in updated_names]

    return {"columns": missing_columns}


if __name__ == "__main__":
    result = convert_object_to_banner9(ObjectType.TABLE, "SATURN", "GZXBLABLA")
    print(result)
