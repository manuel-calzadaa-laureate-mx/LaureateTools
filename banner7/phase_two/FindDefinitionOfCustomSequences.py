from db.DatabaseProperties import DatabaseEnvironment, DatabaseObject
from db.OracleDatabaseTools import get_connection
from tools.ObjectDataFileTools import add_new_object_element_to_object_data_file, extract_attributes_from_sequences, \
     extract_unique_dependencies_from_data_file

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_sequences = extract_unique_dependencies_from_data_file(input_file_name=object_data,database_object_type=DatabaseObject.SEQUENCE, environment=DatabaseEnvironment.BANNER7)

    # Check if the list is not empty
    if unique_sequences:
        # Proceed only if unique_sequences is not empty
        connection = get_connection("../../db_config.json", DatabaseEnvironment.BANNER7)
        json_attributes_from_sequences = extract_attributes_from_sequences(connection, unique_sequences)
        add_new_object_element_to_object_data_file(object_data, DatabaseEnvironment.BANNER7,
                                                   json_attributes_from_sequences)
    else:
        print("No unique sequences found. Skipping db operations.")
