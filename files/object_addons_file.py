import os
from enum import Enum
from typing import Optional

from tools.common_tools import refactor_tagged_text, extract_object_structure
from tools.file_tools import read_json_file


class ObjectAddonType(Enum):
    TRIGGERS = "triggers"
    COLUMNS = "columns"
    COMMENTS = "comments"
    INDEXES = "indexes"
    SEQUENCES = "sequences"
    GRANTS = "grants"
    SETUP_GRANTS = "setup_grants"
    REVOKES = "revokes"
    SYNONYMS = "synonyms"
    SETUP_SYNONYMS = "setup_synonyms"
    DROP_SYNONYMS = "drop_synonyms"


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


def _get_custom_table_columns(json_data: dict, b9_table_name: str) -> dict:
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
    extracted_table_info = extract_object_structure(object_name=b9_table_name)
    prefix = extracted_table_info.get("prefix")
    base = extracted_table_info.get("base")
    sequence_name = f"{prefix}SE{base}"

    sequence = [{
        "name": sequence_name,
        "increment_by": field["increment_by"],
        "start_with": field["start_with"],
        "max_value": field["max_value"],
        "cycle": field["cycle"],
        "cache": field["cache"],
    } for field in fields]

    return sequence


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


def _get_custom_setup_grants(json_data: dict, object_owner: str, object_name: str, grant_type: GrantType):
    # Navigate to the grant type within the JSON structure
    fields = json_data["root"]["setup_grants"][grant_type.value]
    script_template = fields["scripts"]
    schemas = fields["schema"]

    grants = [
        script_template.replace("{owner}", object_owner).replace("{name}", object_name).replace(
            "{schema}", schema)
        for schema in schemas
    ]

    return {"grants": grants}


def _get_custom_grants(json_data: dict, object_owner: str, object_name: str, grant_type: GrantType):
    # Navigate to the grant type within the JSON structure
    fields = json_data["root"]["grants"][grant_type.value]
    script_template = fields["scripts"]
    schemas = fields["schema"]

    extracted_table_info = extract_object_structure(object_name=object_name)
    prefix = extracted_table_info.get("prefix")
    base = extracted_table_info.get("base")
    owner = object_owner

    grants = [
        script_template.replace("{owner}", owner).replace("{prefix}", prefix).replace("{base}", base).replace(
            "{schema}", schema)
        for schema in schemas
    ]

    return {"grants": grants}


def _get_custom_revokes(json_data: dict, object_owner: str, object_name: str, grant_type: GrantType):
    # Navigate to the grant type within the JSON structure
    fields = json_data["root"]["revokes"][grant_type.value]
    script_template = fields["scripts"]  # The inner block template
    schemas = fields["schema"]

    extracted_table_info = extract_object_structure(object_name=object_name)
    prefix = extracted_table_info.get("prefix")
    base = extracted_table_info.get("base")
    owner = object_owner

    # Generate all inner blocks for each schema
    inner_blocks = []
    for schema in schemas:
        inner_block = script_template.replace("{owner}", owner) \
            .replace("{prefix}", prefix) \
            .replace("{base}", base) \
            .replace("{schema}", schema)
        inner_blocks.append(inner_block)

    # Combine all inner blocks with newlines and wrap in outer BEGIN/END
    combined_script = "BEGIN\n" + "\n".join(inner_blocks) + "\nEND;"

    return {"revokes": combined_script}


def _get_custom_synonym(json_data: dict, b9_table_name: str) -> str:
    return json_data["root"]["synonym"]["create"].replace("{object}", b9_table_name)


def _get_custom_setup_synonym(json_data: dict, b9_owner_name: str, b9_table_name: str) -> str:
    return json_data["root"]["setup_synonym"]["create"].replace("{object}", b9_table_name).replace("{owner}",
                                                                                                   b9_owner_name)


def _get_custom_drop_setup_synonym(json_data: dict, b9_owner_name: str, b9_table_name: str) -> str:
    return json_data["root"]["setup_synonym"]["drop"].replace("{object}", b9_table_name).replace("{owner}",
                                                                                                 b9_owner_name)


def _get_custom_drop_synonym(json_data: dict, b9_table_name: str) -> str:
    return json_data["root"]["synonym"]["drop"].replace("{object}", b9_table_name)


def get_custom_grants_multiple_objects(
        json_data: dict,
        object_names: list,
        grant_type: GrantType = GrantType.TABLE
):
    # Locate the correct grant type in the grants list
    fields = json_data["root"]["grants"].get(grant_type.value)

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
    table_info = extract_object_structure(object_name=b9_table_name)
    prefix = table_info.get("prefix")
    base = table_info.get("base")

    triggers = [
        {
            "name": refactor_tagged_text(original_text=field["name"],
                                         tags=["{prefix}", "{base}"],
                                         replacement_text=[prefix, base]),
            "table": refactor_tagged_text(original_text=field["table"],
                                          tags=["{owner}", "{table}"],
                                          replacement_text=["UVM", b9_table_name]),
            "event": field["event"],
            "body": refactor_tagged_text(original_text=field["body"],
                                         tags=["{prefix}", "{base}", "{table}", "{owner}"],
                                         replacement_text=[prefix, base, b9_table_name, "UVM"]),
        }
        for field in fields
    ]
    return triggers


def read_custom_data(object_addon_type: ObjectAddonType, b9_object_name: str, b9_object_owner: Optional[str] = None,
                     grant_type: Optional[GrantType] = None
                     ):
    """
    Generalized function to read custom table data based on addon type.
    """
    json_custom_data = get_object_addons_data()

    # Route to the appropriate function based on the addon type
    if object_addon_type == ObjectAddonType.COLUMNS:
        return _get_custom_table_columns(json_data=json_custom_data, b9_table_name=b9_object_name)
    elif object_addon_type == ObjectAddonType.COMMENTS:
        return _get_custom_comments(json_data=json_custom_data, b9_table_name=b9_object_name)
    elif object_addon_type == ObjectAddonType.INDEXES:
        return _get_custom_indexes(json_data=json_custom_data)
    elif object_addon_type == ObjectAddonType.SEQUENCES:
        return _get_custom_sequences(json_data=json_custom_data, b9_table_name=b9_object_name)
    elif object_addon_type == ObjectAddonType.TRIGGERS:
        return _get_custom_triggers(json_data=json_custom_data, b9_table_name=b9_object_name)
    elif object_addon_type == ObjectAddonType.GRANTS:
        return _get_custom_grants(json_data=json_custom_data, object_name=b9_object_name, grant_type=grant_type,
                                  object_owner=b9_object_owner)
    elif object_addon_type == ObjectAddonType.SETUP_GRANTS:
        return _get_custom_setup_grants(json_data=json_custom_data, object_name=b9_object_name, grant_type=grant_type,
                                        object_owner=b9_object_owner)
    elif object_addon_type == ObjectAddonType.REVOKES:
        return _get_custom_revokes(json_data=json_custom_data, object_name=b9_object_name, grant_type=grant_type,
                                   object_owner=b9_object_owner)
    elif object_addon_type == ObjectAddonType.SYNONYMS:
        return _get_custom_synonym(json_data=json_custom_data, b9_table_name=b9_object_name)
    elif object_addon_type == ObjectAddonType.SETUP_SYNONYMS:
        return _get_custom_setup_synonym(json_data=json_custom_data, b9_table_name=b9_object_name,
                                         b9_owner_name=b9_object_owner)
    elif object_addon_type == ObjectAddonType.DROP_SYNONYMS:
        return _get_custom_drop_synonym(json_data=json_custom_data, b9_table_name=b9_object_name)
    else:
        raise ValueError(f"Unsupported addon type: {object_addon_type}")
