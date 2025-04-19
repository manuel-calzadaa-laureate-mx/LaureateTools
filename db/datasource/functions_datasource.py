from db.oracle_database_tools import OracleDBConnectionPool


def get_private_package_object_owner(object_dict: dict, db_pool: OracleDBConnectionPool) -> tuple:
    """Retrieve the owner, package, and procedure for packaged objects."""

    with db_pool.get_connection() as connection:
        object_name = object_dict["NAME"]
        package_name = object_dict["PACKAGE"]

        query = """
        SELECT owner, object_name, :object_name
        FROM all_procedures
        WHERE object_name = :package_name
        """

        cursor = connection.cursor()
        cursor.execute(query, {"object_name": object_name, "package_name": package_name})
        result = cursor.fetchone()

        if result is None:
            return None

        # Unpack the result and replace the third element with our fixed object_name
        owner, package, _ = result
        return owner, package, object_name


def get_public_package_object_owner(object_dict: dict, db_pool: OracleDBConnectionPool) -> tuple:
    """Retrieve the owner, package, and procedure for packaged objects."""

    with db_pool.get_connection() as connection:
        object_name = object_dict["NAME"]
        package_name = object_dict["PACKAGE"]

        query = """
        SELECT owner, object_name, procedure_name
        FROM all_procedures
        WHERE procedure_name = :object_name
        AND object_name = :package_name
        """

        cursor = connection.cursor()
        cursor.execute(query, {"object_name": object_name, "package_name": package_name})
        result = cursor.fetchone()
        return result if result else None


def get_independent_object_owners(object_dict: dict, db_pool: OracleDBConnectionPool) -> list:
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        """Retrieve the owners for independent functions."""
        object_name = object_dict["NAME"]
        query = """
        SELECT DISTINCT owner, name
        FROM ALL_SOURCE
        WHERE NAME = :object_name
        """
        cursor.execute(query, {"object_name": object_name})
        return cursor.fetchall()
