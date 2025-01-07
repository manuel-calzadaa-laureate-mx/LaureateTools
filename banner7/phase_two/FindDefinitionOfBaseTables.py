from db.DatabaseProperties import DatabaseEnvironment, DatabaseObject
from db.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_table_metadata_from_database
from tools.ObjectDataFileTools import add_new_object_element_to_object_data_file, extract_unique_dependencies_from_data_file

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_tables = extract_unique_dependencies_from_data_file(input_file_name=object_data,
                                                               database_object_type=DatabaseObject.TABLE,
                                                                     environment=DatabaseEnvironment.BANNER7,
                                                                     is_custom=False)

    if unique_tables:
        connection = get_connection("../../db_config.json", DatabaseEnvironment.BANNER7)
        json_attributes_from_tables = extract_table_metadata_from_database(connection, unique_tables)
        add_new_object_element_to_object_data_file(object_data, DatabaseEnvironment.BANNER7,
                                                   json_attributes_from_tables)
    else:
        print("No unique base tables found. Skipping db operations.")
