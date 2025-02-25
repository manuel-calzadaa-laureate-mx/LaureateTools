from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection

from contextlib import closing


def get_packaged_object_owner(object_dict: dict,
                              database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> tuple:
    """Retrieve the owner, package, and procedure for packaged objects."""
    object_name = object_dict["NAME"]
    package_name = object_dict["PACKAGE"]
    query = """
    SELECT owner, object_name, procedure_name
    FROM all_procedures
    WHERE procedure_name = :object_name
    AND object_name = :package_name
    """

    with closing(get_db_connection(database_name=database_environment)) as db_connection:
        with closing(db_connection.cursor()) as cursor:
            cursor.execute(query, {"object_name": object_name, "package_name": package_name})
            result = cursor.fetchone()

    return result if result else None


from contextlib import closing


def get_independent_object_owners(object_dict: dict, database_environment=DatabaseEnvironment.BANNER7) -> list:
    """Retrieve the owners for independent functions."""
    object_name = object_dict["NAME"]
    query = """
    SELECT DISTINCT owner, name
    FROM ALL_SOURCE
    WHERE NAME = :object_name
    """

    with closing(get_db_connection(database_environment)) as db_connection:
        with closing(db_connection.cursor()) as cursor:
            cursor.execute(query, {"object_name": object_name})
            return cursor.fetchall()
