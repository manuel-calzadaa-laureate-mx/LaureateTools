from db.oracle_database_tools import OracleDBConnectionPool


def get_all_non_oracle_owners(db_pool: OracleDBConnectionPool) -> list:
    """Retrieve all the system owners"""
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()
        query = """
            SELECT username
            FROM dba_users
            WHERE oracle_maintained = 'N'
            AND ACCOUNT_STATUS = 'OPEN'
            ORDER BY username;
        """
        cursor.execute(query)
        owners = cursor.fetchall()
        cursor.close()
        return owners


def get_all_owners_of_packages(db_pool: OracleDBConnectionPool) -> list:
    """Retrieve all the system owners"""
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()
        query = """
        SELECT DISTINCT owner
        FROM ALL_SOURCE
        """
        cursor.execute(query)
        owners = cursor.fetchall()
        cursor.close()
        return owners
