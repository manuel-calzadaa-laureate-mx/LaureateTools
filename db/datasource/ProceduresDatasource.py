import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection


def query_all_procedures_by_owner_and_package(owner, package=None):
    """
    Query the ALL_PROCEDURES table to get procedures for the given owner and package.
    """
    connection = get_db_connection(database_name=DatabaseEnvironment.BANNER7)
    cursor = connection.cursor()
    if package:
        cursor.execute("""
            SELECT PROCEDURE_NAME
            FROM ALL_PROCEDURES
            WHERE OWNER = :owner AND OBJECT_NAME = :package AND PROCEDURE_NAME is not null
        """, {'owner': owner, 'package': package})
    else:
        cursor.execute("""
            SELECT OBJECT_NAME
            FROM ALL_PROCEDURES
            WHERE OWNER = :owner AND OBJECT_TYPE = 'PROCEDURE'
        """, {'owner': owner})
    procedures = [row[0] for row in cursor.fetchall()]

    cursor.close()
    connection.close()
    return procedures


def query_all_procedures_by_package(connection, package):
    """
    Query the ALL_PROCEDURES table to get procedures for the given owner and package.
    """
    cursor = connection.cursor()
    cursor.execute("""
        SELECT OWNER, PACKAGE, PROCEDURE_NAME
        FROM ALL_PROCEDURES
        WHERE OBJECT_NAME = :package AND PROCEDURE_NAME is not null
    """, {'package': package})

    procedures = [{"owner": row[0], "package": row[1], "procedure_name": row[2]} for row in cursor.fetchall()]
    cursor.close()
    return procedures


def query_all_procedures_by_owner_and_list_of_procedures(connection: cx_Oracle.Connection, owner: str,
                                                         list_of_procedures: [str]):
    """
    Query the ALL_PROCEDURES table to get procedures for the given owner and procedure.
    """
    # Create placeholders for the list of procedures
    placeholders = ", ".join([":proc" + str(i) for i in range(len(list_of_procedures))])

    # Dynamically create the query
    query = f"""
        SELECT OWNER, PACKAGE_NAME, OBJECT_NAME AS PROCEDURE_NAME
        FROM ALL_PROCEDURES
        WHERE OWNER = :owner AND OBJECT_NAME IN ({placeholders})
    """

    # Bind parameters
    params = {"owner": owner}
    params.update({f"proc{i}": proc for i, proc in enumerate(list_of_procedures)})

    # Execute the query
    cursor = connection.cursor()
    cursor.execute(query, params)

    # Fetch results
    procedures = [{"owner": row[0], "package": row[1], "procedure_name": row[2]} for row in cursor.fetchall()]
    cursor.close()
    return procedures


def query_sources(owner: str, package: str = None, procedure: str = None,
                  function: str = None, database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7):
    """
    Extracts the source code of a procedure or a package body from the ALL_SOURCE table.

    Args:
        connection (cx_Oracle.Connection): Oracle db connection.
        owner (str): The owner of the object.
        package (str): The package name (optional).
        procedure (str): The procedure name.

    Returns:
        str: The concatenated source code.
        :param database_environment:
        :param procedure:
        :param package:
        :param owner:
        :param function:
    """
    connection = get_db_connection(database_name=database_environment)
    cursor = connection.cursor()

    if package:
        # Query for package body when a package is provided
        query = """
            SELECT TEXT
            FROM ALL_SOURCE
            WHERE OWNER = :owner
              AND TYPE = 'PACKAGE BODY'
              AND NAME = :package
              AND length(trim(ALL_SOURCE.TEXT)) > 1              
            ORDER BY LINE
        """
        cursor.execute(query, {'owner': owner, 'package': package})
    elif procedure:
        # Query for standalone procedure
        query = """
            SELECT TEXT
            FROM ALL_SOURCE
            WHERE OWNER = :owner
              AND TYPE = 'PROCEDURE'
              AND NAME = :procedure
              AND length(trim(ALL_SOURCE.TEXT)) > 1
            ORDER BY LINE
        """
        cursor.execute(query, {'owner': owner, 'procedure': procedure})
    elif function:
        # Query for standalone function
        query = """
            SELECT TEXT
            FROM ALL_SOURCE
            WHERE OWNER = :owner
              AND TYPE = 'FUNCTION'
              AND NAME = :function
              AND length(trim(ALL_SOURCE.TEXT)) > 1
            ORDER BY LINE
        """
        cursor.execute(query, {'owner': owner, 'function': function})

    rows = cursor.fetchall()
    source_code = [row[0] for row in rows]
    cursor.close()
    connection.close()
    return source_code
