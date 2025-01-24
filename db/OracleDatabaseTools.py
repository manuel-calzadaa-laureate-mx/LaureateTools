import json
import os

import cx_Oracle

from db.DatabaseProperties import DatabaseEnvironment

DB_CONFIG_FILE = "../config/db_config.json"


def _load_config(config_file):
    """Load db configuration from a JSON file."""
    with open(config_file, 'r') as file:
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

def get_db_connection(database_name: DatabaseEnvironment) -> cx_Oracle.Connection:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, DB_CONFIG_FILE)  # Move one directory up

    configs = _load_config(config_file)
    database_config = _get_config_for_database(configs, database_name)
    connection_config = _build_connection_string(database_config)
    return cx_Oracle.Connection(connection_config)

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
        "NEXTVAL", "CURRVAL", "RAISE_APPLICATION_ERROR","PUT_LINE"
    }

    # Check if the text is in the built-in set
    return text.upper() in oracle_built_ins
