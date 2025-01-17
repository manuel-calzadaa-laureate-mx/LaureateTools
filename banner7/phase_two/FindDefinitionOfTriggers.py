from db.DatabaseProperties import DatabaseEnvironment, TableObject
from db.OracleDatabaseTools import get_connection
from tools.ObjectDataFileTools import extract_table_unique_dependencies_types_from_data_file, \
    extract_triggers_from_database, add_new_object_to_data_file

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_triggers = extract_table_unique_dependencies_types_from_data_file(input_file_name=object_data,
                                                                             table_object_type=TableObject.TRIGGER,
                                                                             environment=DatabaseEnvironment.BANNER7)
    # Check if the list is not empty
    if unique_triggers:
        # Proceed only if unique_triggers is not empty
        connection = get_connection("../../db_config.json", DatabaseEnvironment.BANNER7)
        json_attributes_from_triggers = extract_triggers_from_database(connection=connection,
                                                                              unique_triggers=unique_triggers)

        add_new_object_to_data_file(object_data_file=object_data, environment=DatabaseEnvironment.BANNER7,
                                    new_json_data=json_attributes_from_triggers)
    else:
        print("No unique triggers found. Skipping db operations.")
