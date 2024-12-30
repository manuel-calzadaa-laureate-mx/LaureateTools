from db.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_attributes_from_tables
from tools.ObjectDataTools import extract_unique_tables, append_metadata_to_json

if __name__ == "__main__":
    object_data = "../../object_data.json"

    unique_tables = extract_unique_tables(object_data, "banner7")
    connection = get_connection("../../db_config.json", "banner7")

    json_attributes_from_tables = extract_attributes_from_tables(connection, unique_tables)
    append_metadata_to_json(object_data, "banner7", json_attributes_from_tables)

