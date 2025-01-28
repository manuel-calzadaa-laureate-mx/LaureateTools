import os
from enum import Enum

from tools.FileTools import read_json_file


class ObjectAddonType(Enum):
    TRIGGERS = "triggers"
    COLUMNS = "columns"
    COMMENTS = "comments"
    INDEXES = "indexes"
    SEQUENCES = "sequences"
    GRANTS = "grants"
    SYNONYMS = "synonyms"

class GrantType(Enum):
    TABLE = "table"
    PACKAGE = "package"
    SEQUENCE = "sequence"

OBJECT_ADDONS_JSON = "../config/object_addons.json"

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


def _get_custom_comments(json_data, b9_table_name: str):
    comments = []
    fields = json_data["root"]["comments"]
    for field in fields:
        # Replace "{table}" in the field name with the table_name
        field_name = field["name"].replace("{table}", b9_table_name)
        # Add the comment to the comments dictionary
        comments.append({
            "name": field_name, "comment": field["comment"]
        })
    # Return the comments structure
    return {"comments": comments}


def _get_custom_table_columns(json_data: dict, b9_table_name: str):
    fields = json_data["root"]["columns"]

    columns = [
        {
            "name": field["name"].replace("{table}", b9_table_name),
            "type": field["type"],
            "length": field["length"],
            "precision": field["precision"] if field["precision"] is not None else None,
            "scale": field["scale"] if field["scale"] is not None else None,
            "nullable": field["nullable"]
        }
        for field in fields
    ]

    return {"columns": columns}


def _get_custom_sequences(json_data: dict, b9_table_name: str):
    fields = json_data["root"]["sequences"]

    sequences = [
        {
            "name": field["name"].replace("{table}", b9_table_name),
            "increment_by": field["increment_by"],
            "start_with": field["start_with"],
            "max_value": field["max_value"],
            "cycle": field["cycle"],
            "cache": field["cache"]
        }
        for field in fields
    ]
    return {"sequences": sequences}


def _get_custom_indexes(json_data: dict, b9_table_name: str, owner: str = "UVM"):
    table_info = extract_table_info(table_name=b9_table_name)
    indexes = json_data["root"]["indexes"]
    transformed_indexes = []

    for index in indexes:
        transformed_index = {
            "name": index["name"]
            .replace("{owner}", owner)
            .replace("{prefix}", table_info.get("prefix"))
            .replace("{base}", table_info.get("base")),
            "uniqueness": index["uniqueness"],
            "tablespace": "DEVELOPMENT",
            "columns": [
                {
                    "column_name": column["column_name"].replace("{table}", b9_table_name),
                    "column_position": column["column_position"],
                    "descend": column["descend"],
                    "index_type": column["index_type"],
                    "column_expression": column["column_expression"]
                }
                for column in index["columns"]
            ]
        }
        transformed_indexes.append(transformed_index)

    return {"indexes": transformed_indexes}

def _get_custom_all_table_grants(json_data: dict, object_name: str):
    table_grants = _get_custom_grants(json_data=json_data, object_name=object_name)

    ## Get the sequence names:
    custom_sequences = _get_custom_sequences(json_data=json_data, b9_table_name=object_name)
    sequence_names = [sequence['name'] for sequence in custom_sequences['sequences']]
    sequence_grants = _get_custom_grants_multiple_objects(json_data=json_data, object_names=sequence_names)
    merged_grants = table_grants.get("grants", []) + sequence_grants.get("grants", [])
    return {"grants": merged_grants}

def _get_custom_grants(json_data: dict, object_name: str, grant_type: GrantType = GrantType.TABLE):
    # Navigate to the grant type within the JSON structure
    fields = json_data["root"]["grants"][0][grant_type.value]
    script_template = fields["script"]
    owners = fields["owner"]

    grants = [
        script_template.replace("{object}", object_name).replace("{owner}", owner)
        for owner in owners
    ]

    return {"grants": grants}

def _get_custom_grants_multiple_objects(
    json_data: dict,
    object_names: list,
    grant_type: GrantType = GrantType.TABLE
):
    # Navigate to the grant type within the JSON structure
    fields = json_data["root"]["grants"][0][grant_type.value]
    script_template = fields["script"]
    owners = fields["owner"]

    all_grants = [
        script_template.replace("{object}", object_name).replace("{owner}", owner)
        for object_name in object_names
        for owner in owners
    ]

    return {"grants": all_grants}


def _get_custom_triggers(json_data: dict, b9_table_name: str):
    fields = json_data["root"]["triggers"]
    table_info = extract_table_info(table_name=b9_table_name)
    prefix = table_info.get("prefix")
    base = table_info.get("base")

    triggers = [
        {
            "name": field["name"].replace("{prefix}", prefix).replace("{base}", base),
            "table": field["table"].replace("{table}", b9_table_name),
            "event": field["event"],
            "body": field["body"].replace("{table}", b9_table_name)
        }
        for field in fields
    ]
    return {"triggers": triggers}


def read_custom_table_data(b9_table_name: str, object_addon_type: ObjectAddonType):
    """
    Generalized function to read custom table data based on addon type.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    custom_addons_file = os.path.join(script_dir, OBJECT_ADDONS_JSON)
    json_custom_data = read_json_file(custom_addons_file)

    # Route to the appropriate function based on the addon type
    if object_addon_type == ObjectAddonType.COLUMNS:
        return _get_custom_table_columns(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.COMMENTS:
        return _get_custom_comments(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.INDEXES:
        return _get_custom_indexes(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.SEQUENCES:
        return _get_custom_sequences(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.TRIGGERS:
        return _get_custom_triggers(json_data=json_custom_data,b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.GRANTS:
        return _get_custom_all_table_grants(json_data=json_custom_data, object_name=b9_table_name)
    else:
        raise ValueError(f"Unsupported addon type: {object_addon_type}")


def refactor_table_indexes(b7_table_indexes: [dict], b7_table_name: str, b9_table_name: str) -> [dict]:
    updated_indexes = []

    for one_index in b7_table_indexes:
        updated_index = one_index.copy()
        updated_index["name"] = one_index.get("name").replace(b7_table_name, b9_table_name)
        updated_index["columns"] = []

        updated_index_columns = []
        columns = one_index.get("columns", [])
        for one_column in columns:
            updated_index_column = one_column.copy()
            updated_index_column["column_name"] = refactor_column_name(one_column.get("column_name"),
                                                                       b7_table_name=b7_table_name,
                                                                       b9_table_name=b9_table_name)
            updated_index_columns.append(updated_index_column)
        updated_index["columns"] = updated_index_columns
        updated_indexes.append(updated_index)
    custom_table_indexes = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.INDEXES)

    combined_indexes = updated_indexes + custom_table_indexes['indexes']
    result = {"indexes": combined_indexes}
    return result


def refactor_table_comments(b7_table_comments: [dict], b7_table_name: str, b9_table_name: str) -> [dict]:
    updated_comments = []

    for comments in b7_table_comments:
        column_name = refactor_column_name(comments['name'], b7_table_name, b9_table_name)

        # Update the comments dictionary
        updated_comment = comments.copy()
        updated_comment['name'] = column_name

        updated_comments.append(updated_comment)

    custom_table_comments = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.COMMENTS)

    combined_comments = updated_comments + custom_table_comments['comments']
    result = {"comments": combined_comments}
    return result


def refactor_column_name(column_name: str, b7_table_name: str, b9_table_name: str) -> str:
    # Check if the name starts with old_table_name
    if not column_name.startswith(b7_table_name):
        # Prepend old_table_name if it does not start with it
        column_name = f"{b7_table_name}_{column_name}"
    # Replace old_table_name with new_table_name
    column_name = column_name.replace(b7_table_name, b9_table_name, 1)
    return column_name


def refactor_table_columns(b7_table_columns: [dict], b7_table_name: str, b9_table_name: str) -> [dict]:
    """
    Renames columns based on the rules provided.

    Parameters:
    columns (list): List of dictionaries representing columns.
    old_table_name (str): The old table name prefix.
    new_table_name (str): The new table name prefix.

    Returns:
    list: A new list of dictionaries with updated column names.
    """
    updated_columns = []

    for column in b7_table_columns:
        column_name = column['name']

        # Check if the name starts with old_table_name
        if not column_name.startswith(b7_table_name):
            # Prepend old_table_name if it does not start with it
            column_name = f"{b7_table_name}_{column_name}"

        # Replace old_table_name with new_table_name
        column_name = column_name.replace(b7_table_name, b9_table_name, 1)

        # Update the column dictionary
        updated_column = column.copy()
        updated_column['name'] = column_name

        updated_columns.append(updated_column)

    custom_table_columns = read_custom_table_data(b9_table_name=b9_table_name, object_addon_type=ObjectAddonType.COLUMNS)
    combined_columns = updated_columns + custom_table_columns['columns']
    result = {"columns": combined_columns}
    return result
