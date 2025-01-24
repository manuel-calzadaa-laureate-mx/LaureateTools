from db.DatabaseProperties import DatabaseEnvironment, TableObject
from db.OracleDatabaseTools import get_db_connection
from tools.ObjectDataTools import extract_table_unique_dependencies_types_from_data_file, \
    extract_triggers_from_database, add_new_object_to_data_file

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_triggers = extract_table_unique_dependencies_types_from_data_file(table_object_type=TableObject.TRIGGER,
                                                                             environment=DatabaseEnvironment.BANNER7)
    # Check if the list is not empty
    if unique_triggers:
        # Proceed only if unique_triggers is not empty
        connection = get_db_connection(DatabaseEnvironment.BANNER7)
        json_attributes_from_triggers = extract_triggers_from_database(connection=connection,
                                                                       unique_triggers=unique_triggers)

        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_triggers)
    else:
        print("No unique triggers found. Skipping db operations.")
