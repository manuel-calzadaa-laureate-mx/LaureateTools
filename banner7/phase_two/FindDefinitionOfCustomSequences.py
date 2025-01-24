from db.DatabaseProperties import DatabaseEnvironment, DatabaseObject
from db.OracleDatabaseTools import get_db_connection
from tools.ObjectDataTools import add_new_object_to_data_file, extract_sequences_attributes_from_database, \
    extract_unique_dependencies_types_from_data_file

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_sequences = extract_unique_dependencies_types_from_data_file(database_object_type=DatabaseObject.SEQUENCE,
                                                                        environment=DatabaseEnvironment.BANNER7)

    # Check if the list is not empty
    if unique_sequences:
        # Proceed only if unique_sequences is not empty
        connection = get_db_connection(DatabaseEnvironment.BANNER7)
        json_attributes_from_sequences = extract_sequences_attributes_from_database(connection, unique_sequences)
        add_new_object_to_data_file(environment=DatabaseEnvironment.BANNER7, new_json_data=
        json_attributes_from_sequences)
    else:
        print("No unique sequences found. Skipping db operations.")
