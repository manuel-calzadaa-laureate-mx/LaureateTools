import logging
from typing import List

from db.database_properties import DatabaseEnvironment
from db.oracle_database_tools import OracleDBConnectionPool


def get_package_records(package_owner: str, package_names: list[str],
                        db_pool: OracleDBConnectionPool):
    """
       Extracts the source code of procedures or package bodies from the ALL_SOURCE table for a list of package names.

       Args:
           package_owner (str): The owner of the objects.
           package_names (List[str]): A list of package names.

       Returns:
           List[Tuple]: A list of tuples containing the source code records for the specified packages.
           :param package_names:
           :param package_owner:
           :param db_pool:
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
        return rows


def get_package_body(package_owner: str, package_name: str,
                     db_pool: OracleDBConnectionPool):
    """
         Extracts the source code of procedures from the ALL_SOURCE table for a list of package names.

         Args:
             package_owner (str): The owner of the objects.
             package_names (List[str]): A list of package names.

         Returns:
             List[Tuple]: A list of tuples containing the source code records for the specified packages.
             :param package_name:
             :param package_owner:
             :param db_pool:
         """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Query for package bodies when a list of package names is provided
        query = """
                 SELECT
                    text
                 FROM
                     all_source
                 WHERE
                     type IN ('PACKAGE BODY')
                     AND owner = :package_owner
                     AND name = :package_name
                 ORDER BY
                     line
             """

        # Create a dictionary of parameters for the query
        params = {'package_owner': package_owner, "package_name": package_name}
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        return rows


def get_package_specification(package_owner: str, package_name: str,
                              db_pool: OracleDBConnectionPool):
    """
         Extracts the source code of procedures from the ALL_SOURCE table for a list of package names.

         Args:
             package_owner (str): The owner of the objects.
             package_names (List[str]): A list of package names.

         Returns:
             List[Tuple]: A list of tuples containing the source code records for the specified packages.
             :param package_name:
             :param package_owner:
             :param db_pool:
         """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Query for package bodies when a list of package names is provided
        query = """
                 SELECT
                    text
                 FROM
                     all_source
                 WHERE
                     type IN ('PACKAGE')
                     AND owner = :package_owner
                     AND name = :package_name
                 ORDER BY
                     line
             """

        # Create a dictionary of parameters for the query
        params = {'package_owner': package_owner, "package_name": package_name}
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
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
        return rows


def find_if_packages_exist(db_pool: OracleDBConnectionPool, package_candidates: List[str]) -> list:
    """Check which of the candidate names are existing packages in the database"""
    if not package_candidates:
        return []

    try:
        with db_pool.get_connection() as connection:
            with connection.cursor() as cursor:
                # Always include a comma, even for single items
                if len(package_candidates) == 1:
                    quoted_names = f"'{package_candidates[0].upper()}'"
                else:
                    quoted_names = ", ".join(f"'{name.upper()}'" for name in package_candidates)
                query = f"""
                    SELECT object_name
                    FROM all_objects
                    WHERE object_name IN ({quoted_names})
                    AND object_type = 'PACKAGE'
                    ORDER BY object_name
                """  # Removed the semicolon at the end as it's not needed in cursor.execute
                cursor.execute(query)
                return cursor.fetchall()
    except Exception as e:
        logging.error(f"Error checking package existence: {e}")
        return []


if __name__ == "__main__":
    db_pool_banner9 = OracleDBConnectionPool(database_name=DatabaseEnvironment.BANNER9)
    package_specs = get_package_specification(package_owner="UVM", db_pool=db_pool_banner9, package_name="TZPKFPLIA")
    package_body = get_package_body(package_owner="UVM", db_pool=db_pool_banner9, package_name="TZPKFPLIA")
