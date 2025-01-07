def get_packaged_object_owner(object: str, db_connection) -> tuple:
    """Retrieve the owner, package, and procedure for packaged objects."""
    package, func_name = object.split('.')
    query = """
    SELECT owner, object_name, procedure_name
    FROM all_procedures
    WHERE procedure_name = :func_name
    AND object_name = :package
    """
    cursor = db_connection.cursor()
    cursor.execute(query, func_name=func_name, package=package)
    return cursor.fetchone()

def get_independent_object_owners(func_name: str, db_connection) -> list:
    """Retrieve the owners for independent functions."""
    query = """
    SELECT DISTINCT owner, name
    FROM ALL_SOURCE
    WHERE NAME = :func_name
    """
    cursor = db_connection.cursor()
    cursor.execute(query, func_name=func_name)
    return cursor.fetchall()