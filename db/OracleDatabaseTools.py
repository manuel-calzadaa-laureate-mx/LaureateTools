import json
import os
from contextlib import contextmanager

import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment

DB_CONFIG_FILE = "../config/db_config.json"


class OracleDBConnectionPool:
    _instances = {}  # Dictionary to store instances for different databases

    def __new__(cls, database_name: DatabaseEnvironment):
        if database_name not in cls._instances:
            instance = super(OracleDBConnectionPool, cls).__new__(cls)
            instance._initialize_connection_pool(database_name)
            cls._instances[database_name] = instance
        return cls._instances[database_name]

    def _initialize_connection_pool(self, database_name: DatabaseEnvironment):
        """Initialize the connection pool."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(script_dir, DB_CONFIG_FILE)  # Move one directory up

        configs = _load_config(config_file)
        database_config = _get_config_for_database(configs, database_name)

        # Create a connection pool
        self._connection_pool = cx_Oracle.SessionPool(
            user=database_config['username'],
            password=database_config['password'],
            dsn=f"{database_config['host']}:{database_config['port']}/{database_config['service_name']}",
            min=1,  # Minimum number of connections in the pool
            max=5,  # Maximum number of connections in the pool
            increment=1  # Number of connections to add when the pool is exhausted
        )

    @contextmanager
    def get_connection(self):
        """Context manager for acquiring and releasing a connection."""
        connection = self._connection_pool.acquire()
        try:
            yield connection
        finally:
            self._connection_pool.release(connection)

    def close_pool(self):
        """Close the connection pool."""
        if self._connection_pool is not None:
            self._connection_pool.close()
            self._connection_pool = None

    @classmethod
    def close_all_pools(cls):
        """Close all connection pools."""
        for instance in cls._instances.values():
            instance.close_pool()
        cls._instances.clear()


def _load_config(config_file):
    """Load db configuration from a JSON file."""
    with open(config_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def _get_config_for_database(configs, database_name: DatabaseEnvironment):
    """Retrieve the config for a specific db name."""
    for entry in configs:
        if entry['databaseName'].upper() == database_name.name:
            return entry['config']
    raise ValueError(f"Database '{database_name.name}' not found in configurations.")


def _build_connection_string(config):
    """Construct the Oracle connection string from configuration."""
    return f"{config['username']}/{config['password']}@{config['host']}:{config['port']}/{config['service_name']}"


def is_oracle_built_in_object(text) -> bool:
    # List of Oracle built-in functions and keywords to ignore
    oracle_built_ins = {
        "DUAL", "TO_CHAR", "TO_NUMBER", "TRUNC", "SYSDATE", "MAX", "MIN", "AVG", "COUNT",
        "NVL", "COALESCE", "SUBSTR", "LENGTH", "INSTR", "DECODE", "CASE",
        "GREATEST", "LEAST", "ROUND", "DISTINCT", "WHERE", "SELECT", "IN",
        "GROUP", "BY", "ORDER", "DBMS_OUTPUT.PUT_LINE", "IF", "PUT_LINE", "VALUES", "SUM",
        "VARCHAR2", "TO_DATE", "ABS", "AND", "EXISTS", "LPAD", "NUMBER", "FROM", "TRIM", "OR", "VARCHAR", "RETURN",
        "NULL", "ELSE", "ELSIF", "END", "FOR", "LOOP", "NOT", "LIKE", "BETWEEN", "IS",
        "ROWNUM", "USER", "CONNECT_BY_ROOT", "PRIOR", "LEVEL", "ROWID", "SYS_CONNECT_BY_PATH",
        "NEXTVAL", "CURRVAL", "RAISE_APPLICATION_ERROR", "PUT_LINE"
    }

    # Check if the text is in the built-in set
    return text.upper() in oracle_built_ins


# Example usage
if __name__ == "__main__":
    # Initialize the connection pools
    db_pool_banner7 = OracleDBConnectionPool(DatabaseEnvironment.BANNER7)
    db_pool_banner9 = OracleDBConnectionPool(DatabaseEnvironment.BANNER9)

    # Use the connection manager for banner7
    with db_pool_banner7.get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchall()
        print(result)

    # Use the connection manager for banner9
    with db_pool_banner9.get_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchall()
        print(result)

    # Optionally, close all pools when done (e.g., during application shutdown)
    OracleDBConnectionPool.close_all_pools()
