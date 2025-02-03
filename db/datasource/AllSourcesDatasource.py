from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection


def get_all_owners(database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> list:
    """Retrieve all the system owners"""
    connection = get_db_connection(database_name=database_environment)
    cursor = connection.cursor()
    query = """
    SELECT DISTINCT owner
    FROM ALL_SOURCE
    """
    cursor.execute(query)
    owners = cursor.fetchall()
    cursor.close()
    connection.close()
    return owners
