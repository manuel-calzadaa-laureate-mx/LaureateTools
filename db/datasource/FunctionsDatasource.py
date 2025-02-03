def get_packaged_object_owner(object_dict: dict, db_connection) -> tuple:
    """Retrieve the owner, package, and procedure for packaged objects."""
    object_name = object_dict["NAME"]
    package_name = object_dict["PACKAGE"]
    query = """
    SELECT owner, object_name, procedure_name
    FROM all_procedures
    WHERE procedure_name = :object_name
    AND object_name = :package_name
    """
    cursor = db_connection.cursor()
    cursor.execute(query, object_name=object_name, package_name=package_name)
    result = cursor.fetchone()
    return result if result else None

def get_independent_object_owners(object_dict: dict, db_connection) -> list:
    """Retrieve the owners for independent functions."""
    object_name = object_dict["NAME"]
    query = """
    SELECT DISTINCT owner, name
    FROM ALL_SOURCE
    WHERE NAME = :object_name
    """
    cursor = db_connection.cursor()
    cursor.execute(query, object_name=object_name)
    return cursor.fetchall()