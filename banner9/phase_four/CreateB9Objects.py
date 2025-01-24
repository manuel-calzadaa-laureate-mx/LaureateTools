from db.DatabaseProperties import DatabaseEnvironment
from tools.FileTools import read_json_file
from tools.MappingFileTools import read_mapping_data
from tools.MigrationTools import migrate_b7_table_to_b9
from tools.ObjectDataTools import add_new_environment, add_or_update_object_in_data_file, load_object_data_to_json

if __name__ == "__main__":
    object_data = "../../object_data.json"
    # read all mapped objects

    # add_new_environment(file_path=object_data, new_environment=DatabaseEnvironment.BANNER9)

    migration_data = read_mapping_data()
    for table_name_key in migration_data:
        b9_mapping_data = migration_data.get(table_name_key, {})
        b9_paquete = b9_mapping_data.get("B9_PAQUETE")
        b9_nombre = b9_mapping_data.get("B9_NOMBRE")
        b9_esquema = b9_mapping_data.get("B9_ESQUEMA")

        json_object_data = load_object_data_to_json()
        converted_table_data = migrate_b7_table_to_b9(json_data=json_object_data, b7_table_name=table_name_key,
                                                      b9_table_name=b9_nombre, b9_owner=b9_esquema)

        add_or_update_object_in_data_file(new_object=converted_table_data,
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
