from db.OracleDatabaseTools import OracleDBConnectionPool


def get_all_owners(db_pool: OracleDBConnectionPool) -> list:
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
