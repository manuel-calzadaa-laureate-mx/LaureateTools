from tools.MigrationTools import extract_table_info


def get_custom_comments(json_data, b9_table_name: str):
    comments = {}
    fields = json_data["root"]["fields"]

    for field in fields:
        # Replace "{table}" in the field name with the table_name
        field_name = field["name"].replace("{table}", b9_table_name)
        # Add the comment to the comments dictionary
        comments[field_name] = field["comment"]

    # Return the comments structure
    return {"comments": comments}

def get_custom_table_columns(json_data: dict, b9_table_name: str):
    fields = json_data["root"]["fields"]

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

def get_custom_indexes(input_json : dict, b9_table_name: str, owner: str = "UVM"):

    table_info = extract_table_info(table_name=b9_table_name)
    indexes = input_json["root"]["indexes"]
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