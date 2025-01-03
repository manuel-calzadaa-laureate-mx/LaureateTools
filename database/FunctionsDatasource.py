def get_packaged_function_owner(function: str, db_connection) -> tuple:
    """Retrieve the owner, package, and procedure for packaged functions."""
    package, func_name = function.split('.')
    query = """
    SELECT owner, object_name, procedure_name
    FROM all_procedures
    WHERE procedure_name = :func_name
    AND object_name = :package
    """
    cursor = db_connection.cursor()
    cursor.execute(query, func_name=func_name, package=package)
    return cursor.fetchone()


def get_independent_function_owners(function: str, db_connection) -> list:
    """Retrieve the owners for independent functions."""
    query = """
    SELECT DISTINCT owner, name
    FROM ALL_SOURCE
    WHERE NAME = :function_name AND type = 'FUNCTION'
    """
    cursor = db_connection.cursor()
    cursor.execute(query, function_name=function)
    return cursor.fetchall()