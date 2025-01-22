from db.DatabaseProperties import DatabaseObject, DatabaseEnvironment
from tools.CustomFileTools import get_custom_table_columns, get_custom_comments, get_custom_indexes
from tools.FileTools import read_json_file
from tools.MappingFileTools import csv_to_json
from tools.ObjectDataFileTools import add_new_environment, add_or_update_object_in_data_file


def migrate_b7_table_to_b9(json_data: dict, b7_table_name: str, b9_table_name: str,
                           b9_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER9, b9_owner: str = "UVM"):
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
        :param b9_environment:
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
    # Create the new table object
    new_table = {
        "name": b9_table_name,
        "type": "TABLE",
        "owner": b9_owner,
        "custom": original_table.get("custom", True)
        , "columns": columns["columns"]
        , "attributes": original_table.get("attributes", {})
        , "comments": comments["comments"]
        , "indexes": indexes["indexes"]
        # ,        "sequences": original_table.get("sequences", [])
        # ,        "triggers": original_table.get("triggers", [])
        ,
    }

    return new_table


def read_custom_table_columns(b9_table_name: str):
    custom_addons_file = "../object_addons.json"
    json_custom_data = read_json_file(custom_addons_file)
    return get_custom_table_columns(json_custom_data, b9_table_name=b9_table_name)


def read_custom_table_comments(b9_table_name: str):
    custom_addons_file = "../object_addons.json"
    json_custom_data = read_json_file(custom_addons_file)
    return get_custom_comments(json_custom_data, b9_table_name=b9_table_name)


def read_custom_table_indexes(b9_table_name):
    custom_addons_file = "../object_addons.json"
    json_custom_data = read_json_file(custom_addons_file)
    return get_custom_indexes(json_custom_data, b9_table_name=b9_table_name)


def refactor_table_indexes(b7_table_indexes: [dict], b7_table_name: str, b9_table_name: str) -> [dict]:
    updated_indexes = []

    for one_index in b7_table_indexes:
        updated_index = one_index.copy()
        updated_index["name"] = one_index.get("name").replace(b7_table_name, b9_table_name),
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
    custom_table_indexes = read_custom_table_indexes(b9_table_name=b9_table_name)

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

    custom_table_comments = read_custom_table_comments(b9_table_name=b9_table_name)

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

    custom_table_columns = read_custom_table_columns(b9_table_name=b9_table_name)
    combined_columns = updated_columns + custom_table_columns['columns']
    result = {"columns": combined_columns}
    return result


def read_mapping_data(database_object: DatabaseObject = DatabaseObject.TABLE) -> [dict]:
    all_mapped_records = csv_to_json(csv_file_path=mapping_file)
    mapping_data = {}
    for one_record in all_mapped_records:
        if (one_record["IS_MAPPED"]
                and one_record["B7_TIPO"] == database_object.name
                and one_record["B9_NOMBRE"] != 'none'):
            mapping_data[one_record["B7_NOMBRE"]] = {
                "B9_ESQUEMA": one_record["B9_ESQUEMA"],
                "B9_PAQUETE": one_record["B9_PAQUETE"],
                "B9_NOMBRE": one_record["B9_NOMBRE"]
            }
    return mapping_data


if __name__ == "__main__":
    object_data = "../../object_data.json"
    config_file = '../../db_config.json'  # JSON file containing db credentials
    mapping_file = "../mapping.csv"
    # read all mapped objects

    add_new_environment(file_path=object_data, new_environment=DatabaseEnvironment.BANNER9)

    migration_data = read_mapping_data()
    for table_name_key in migration_data:
        b9_mapping_data = migration_data.get(table_name_key, {})
        b9_paquete = b9_mapping_data.get("B9_PAQUETE")
        b9_nombre = b9_mapping_data.get("B9_NOMBRE")
        b9_esquema = b9_mapping_data.get("B9_ESQUEMA")

        json_object_data = read_json_file(object_data)
        converted_table_data = migrate_b7_table_to_b9(json_data=json_object_data, b7_table_name=table_name_key,
                                                      b9_table_name=b9_nombre, b9_owner=b9_esquema)

        add_or_update_object_in_data_file(object_data_file=object_data, new_object=converted_table_data,
                                          environment=DatabaseEnvironment.BANNER9)


        # #
        # # table_name = "TZTBRCIBK"
        # custom_addons_file = "../object_addons.json"
        # json_custom_data = read_json_file(custom_addons_file)

        # custom_columns_addon = get_custom_table_columns(json_data, table_name)
        # print(json.dumps(custom_columns_addon, indent=4))
        # custom_indexes_addon = get_custom_indexes(json_data, table_name)
        # print(json.dumps(custom_indexes_addon, indent=4))

        # read all the table data
        # if field doesn't start with table name append it
        # add fields + new fields
        # add comments + add new comments
        # add indexes + add new indexes

        # add new sequence object
        # add new trigger object

        # if ...
