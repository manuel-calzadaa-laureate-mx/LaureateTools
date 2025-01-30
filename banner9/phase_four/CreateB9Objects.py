from db.DatabaseProperties import DatabaseEnvironment
from files.MappingFile import read_mapping_data
from tools.MigrationTools import migrate_b7_table_to_b9
from files.ObjectDataFile import add_or_update_object_in_data_file, load_object_data_to_json

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



