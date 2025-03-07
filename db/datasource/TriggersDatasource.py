from db.DatabaseProperties import DatabaseObject, DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool


def fetch_triggers_elements_from_database(db_pool: OracleDBConnectionPool, trigger_names: [str]):
    """
    Query the ALL_TRIGGERS table to get details for the given list of trigger names.
    """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Create a dynamic query to handle the list of trigger names
        query = """
        SELECT
            OWNER,
            TABLE_NAME,
            TRIGGER_NAME,
            TRIGGER_TYPE,
            TRIGGERING_EVENT,
            REFERENCING_NAMES,
            WHEN_CLAUSE,
            STATUS,
            DESCRIPTION,
            TRIGGER_BODY
        FROM
            ALL_TRIGGERS
        WHERE
            BASE_OBJECT_TYPE = 'TABLE'
            AND TRIGGER_NAME IN ({})
        """.format(",".join([f"'{name}'" for name in trigger_names]))
        cursor.execute(query)

        trigger = [{
            "owner": row[0],
            "table_name": row[1],
            "trigger_name": row[2],
            "trigger_type": row[3],
            "triggering_event": row[4],
            "referencing_names": row[5],
            "when_clause": row[6],
            "status": row[7],
            "description": row[8],
            "trigger_body": row[9],
        } for row in cursor.fetchall()]

        cursor.close()
        return trigger


def fetch_triggers_for_tables(db_pool: OracleDBConnectionPool, table_names: [str]):
    """
    Fetch triggers from the ALL_TRIGGERS table grouped by owner and table name.

    Args:
        connection: cx_Oracle connection object.
        tables: List of table names to filter triggers.

    Returns:
        dict: Nested dictionary grouped by owner and table name.
              {owner: {table_name: [trigger_records]}}
              :param table_names:
              :param db_pool:
    """

    table_names_upper = [name.upper() for name in table_names]

    # Dynamically create placeholders for the IN clause
    placeholders = ','.join([f':name{i}' for i in range(len(table_names_upper))])

    query = f"""
    SELECT
        OWNER,
        TABLE_NAME,
        TRIGGER_NAME,
        TRIGGER_TYPE,
        TRIGGERING_EVENT,
        REFERENCING_NAMES,
        WHEN_CLAUSE,
        STATUS,
        DESCRIPTION,
        TRIGGER_BODY
    FROM
        ALL_TRIGGERS
    WHERE
        BASE_OBJECT_TYPE = 'TABLE'
        AND TABLE_NAME IN ({placeholders})
    """

    # Prepare a dictionary to group results
    grouped_data = {}

    with db_pool.get_connection() as connection:
        cursor = connection.cursor()
        # Convert tables list into Oracle's SQL-compatible format
        bind_variables = {f'name{i}': name for i, name in enumerate(table_names_upper)}
        cursor.execute(query, bind_variables)

        for row in cursor:
            owner = row[0]
            table_name = row[1]

            # Initialize nested dictionaries if not present
            if owner not in grouped_data:
                grouped_data[owner] = {}

            if table_name not in grouped_data[owner]:
                grouped_data[owner][table_name] = []

            # Append trigger record
            grouped_data[owner][table_name].append({
                "trigger_name": row[2],
                "trigger_type": row[3],
                "triggering_event": row[4],
                "referencing_names": row[5],
                "when_clause": row[6],
                "status": row[7],
                "description": row[8],
                "trigger_body": row[9],
            })

    return grouped_data


if __name__ == "__main__":
    object_data = "../../object_data.json"
    database_object_type = DatabaseObject.TABLE
    environment = DatabaseEnvironment.BANNER7
    database_config = "../../db_config.json"
