from database.DatabaseProperties import DatabaseEnvironment
from database.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_attributes_from_tables
from tools.ObjectDataFileTools import extract_unique_tables_from_procedure_and_function_dependencies, \
    add_new_object_element_to_object_data_file

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_tables = extract_unique_tables_from_procedure_and_function_dependencies(input_file_name=object_data,
                                                                                   environment=DatabaseEnvironment.BANNER7,
                                                                                   is_custom=False)

    if unique_tables:
        connection = get_connection("../../db_config.json", DatabaseEnvironment.BANNER7)
        json_attributes_from_tables = extract_attributes_from_tables(connection, unique_tables)
        add_new_object_element_to_object_data_file(object_data, DatabaseEnvironment.BANNER7,
                                                   json_attributes_from_tables)
    else:
        print("No unique base tables found. Skipping database operations.")
