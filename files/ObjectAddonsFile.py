import os
from enum import Enum

from tools.CommonTools import extract_table_info, refactor_tagged_text
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


OBJECT_ADDONS_DATA_JSON = "../config/object_addons.json"


def get_object_addons_data() -> dict:
    config_file = get_object_addons_file_path()
    return read_json_file(config_file)


def get_object_addons_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, OBJECT_ADDONS_DATA_JSON)


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


def _get_custom_table_columns(json_data: dict, b9_table_name: str)-> dict:
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
    extracted_table_info = extract_table_info(table_name=b9_table_name)
    prefix = extracted_table_info.get("prefix")
    base = extracted_table_info.get("base")

    sequences = [
        {
            "name": refactor_tagged_text(original_text=field["name"],
                                         tags=["{prefix}", "{base}"],
                                         replacement_text=[prefix, base]),
            "increment_by": field["increment_by"],
            "start_with": field["start_with"],
            "max_value": field["max_value"],
            "cycle": field["cycle"],
            "cache": field["cache"]
        }
        for field in fields
    ]
    return {"sequences": sequences}


def get_custom_indexes() -> dict:
    addons_data = get_object_addons_data()
    return _get_custom_indexes(addons_data)


def _get_custom_indexes(json_data: dict):
    indexes = json_data["root"]["indexes"]
    transformed_indexes = []

    for index in indexes:
        transformed_index = {
            "name": index["name"],
            "uniqueness": index["uniqueness"],
            "constraint_type": index["constraint_type"],
            "tablespace": "DEVELOPMENT",
            "columns": [
                {
                    "column_name": column["column_name"],
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
    sequence_grants = _get_custom_grants_multiple_objects(json_data=json_data, object_names=sequence_names,
                                                          grant_type=GrantType.SEQUENCE)
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


def _get_custom_synonym(json_data: dict, b9_table_name: str) -> str:
    return json_data["root"]["synonym"].replace("{table}", b9_table_name)


def _get_custom_grants_multiple_objects(
        json_data: dict,
        object_names: list,
        grant_type: GrantType = GrantType.TABLE
):
    # Locate the correct grant type in the grants list
    fields = next((grant[grant_type.value] for grant in json_data["root"]["grants"] if grant_type.value in grant), None)

    if not fields:
        raise ValueError(f"Grant type '{grant_type.value}' not found in JSON data.")

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
            "name": refactor_tagged_text(original_text=field["name"],
                                         tags=["{prefix}", "{base}"],
                                         replacement_text=[prefix, base]),
            "table": field["table"].replace("{table}", b9_table_name),
            "event": field["event"],
            "body": refactor_tagged_text(original_text=field["body"],
                                         tags=["{prefix}", "{base}", "{table}"],
                                         replacement_text=[prefix, base, b9_table_name]),
        }
        for field in fields
    ]
    return {"triggers": triggers}


def read_custom_table_data(b9_table_name: str, object_addon_type: ObjectAddonType):
    """
    Generalized function to read custom table data based on addon type.
    """

    json_custom_data = get_object_addons_data()

    # Route to the appropriate function based on the addon type
    if object_addon_type == ObjectAddonType.COLUMNS:
        return _get_custom_table_columns(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.COMMENTS:
        return _get_custom_comments(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.INDEXES:
        return _get_custom_indexes(json_data=json_custom_data)
    elif object_addon_type == ObjectAddonType.SEQUENCES:
        return _get_custom_sequences(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.TRIGGERS:
        return _get_custom_triggers(json_data=json_custom_data, b9_table_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.GRANTS:
        return _get_custom_all_table_grants(json_data=json_custom_data, object_name=b9_table_name)
    elif object_addon_type == ObjectAddonType.SYNONYMS:
        return _get_custom_synonym(json_data=json_custom_data, b9_table_name=b9_table_name)
    else:
        raise ValueError(f"Unsupported addon type: {object_addon_type}")
