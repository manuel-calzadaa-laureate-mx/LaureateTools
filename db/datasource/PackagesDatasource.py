from db.OracleDatabaseTools import OracleDBConnectionPool


def get_package_records(package_owner: str, package_names: list[str],
                        db_pool: OracleDBConnectionPool):
    """
       Extracts the source code of procedures or package bodies from the ALL_SOURCE table for a list of package names.

       Args:
           package_owner (str): The owner of the objects.
           package_names (List[str]): A list of package names.

       Returns:
           List[Tuple]: A list of tuples containing the source code records for the specified packages.
       """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Query for package bodies when a list of package names is provided
        query = """
               SELECT
                   owner,
                   name,
                   type,
                   line,
                   text
               FROM
                   all_source
               WHERE
                   type IN ('PACKAGE','PACKAGE BODY')
                   AND owner = :package_owner
                   AND name IN ({})
           """.format(", ".join([f":package_name_{i}" for i in range(len(package_names))]))

        # Create a dictionary of parameters for the query
        params = {'package_owner': package_owner}
        params.update({f'package_name_{i}': package_name for i, package_name in enumerate(package_names)})

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return rows


def get_package_record(package_owner: str, package_name: str,
                       db_pool: OracleDBConnectionPool):
    """
    Extracts the source code of a procedure or a package body from the ALL_SOURCE table.

    Args:
        owner (str): The owner of the object.
        package (str): The package name (optional).

    Returns:
        str: The concatenated source code.
        :param db_pool:
        :param package_name:
        :param package_owner:
    """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Query for package body when a package is provided
        query = """
            SELECT
                owner,
                name,
                type,
                line,
                text
            FROM
                all_source
            WHERE
                type IN ( 'PACKAGE')
                AND owner = :package_owner
                AND name = :package_name
        """
        cursor.execute(query, {'package_owner': package_owner, 'package_name': package_name})
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return rows
