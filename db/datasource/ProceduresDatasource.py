import cx_Oracle

from db.OracleDatabaseTools import OracleDBConnectionPool


def query_all_procedures_by_owner_and_package(db_pool: OracleDBConnectionPool, owner: str, package: str = None):
    """
    Query the ALL_PROCEDURES table to get procedures for the given owner and package.
    """
    with db_pool.get_connection() as connection:
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


def query_sources(db_pool: OracleDBConnectionPool, owner: str, package: str = None, procedure: str = None,
                  function: str = None):
    with db_pool.get_connection() as connection:
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

    return source_code
