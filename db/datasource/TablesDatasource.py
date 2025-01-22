import cx_Oracle


def fetch_table_columns_for_tables(connection: cx_Oracle.Connection, table_names: [str]):
    """
    Fetch columns and their attributes for given tables across all accessible schemas.

    :param connection: Database connection object
    :param table_names: List of table names
    :return: Dictionary grouped by schema and table name, containing columns and their attributes
    """
    cursor = connection.cursor()
    table_names_upper = [name.upper() for name in table_names]
    cursor.execute(f"""
        SELECT owner, table_name, column_name, data_type, data_length, data_precision, data_scale, nullable
        FROM ALL_TAB_COLUMNS
        WHERE table_name IN ({','.join([':name' + str(i) for i in range(len(table_names))])})
        ORDER BY owner, table_name, column_id
    """, {f'name{i}': name for i, name in enumerate(table_names_upper)})
    results = cursor.fetchall()
    cursor.close()
    return results


def fetch_table_columns_for_tables_grouped_by_schema_and_table_name(connection: cx_Oracle.Connection,
                                                                    table_names: [str]):
    results = fetch_table_columns_for_tables(connection=connection, table_names=table_names)
    grouped_columns = {}
    for row in results:
        schema, table_name, column_name, data_type, data_length, data_precision, data_scale, nullable = row
        if schema not in grouped_columns:
            grouped_columns[schema] = {}
        if table_name not in grouped_columns[schema]:
            grouped_columns[schema][table_name] = []
        grouped_columns[schema][table_name].append({
            "name": column_name,
            "type": data_type,
            "length": data_length,
            "precision": data_precision,
            "scale": data_scale,
            "nullable": nullable == "Y"
        })
    return grouped_columns


def fetch_table_attributes_for_tables(connection: cx_Oracle.Connection, table_names: [str]):
    """
    Fetch storage parameters for given tables across all accessible schemas.

    :param connection: Database connection object
    :param table_names: List of table names
    :return: Dictionary grouped by schema and table name, containing table attributes
    """
    cursor = connection.cursor()
    table_names_upper = [name.upper() for name in table_names]
    cursor.execute(f"""
        SELECT owner, table_name, pct_free, pct_used, ini_trans, max_trans, logging, tablespace_name
        FROM ALL_TABLES
        WHERE table_name IN ({','.join([':name' + str(i) for i in range(len(table_names))])})
    """, {f'name{i}': name for i, name in enumerate(table_names_upper)})
    results = cursor.fetchall()
    cursor.close()
    return results


def fetch_table_attributes_for_tables_grouped_by_schema_and_table_name(connection: cx_Oracle.Connection,
                                                                       table_names: [str]):
    results = fetch_table_attributes_for_tables(connection=connection, table_names=table_names)
    grouped_attributes = {}
    for row in results:
        schema, table_name, pct_free, pct_used, ini_trans, max_trans, logging, tablespace_name = row
        if schema not in grouped_attributes:
            grouped_attributes[schema] = {}
        grouped_attributes[schema][table_name] = {
            "pct_free": pct_free,
            "pct_used": pct_used,
            "ini_trans": ini_trans,
            "max_trans": max_trans,
            "logging": logging,
            "tablespace": tablespace_name
        }
    return grouped_attributes


def fetch_column_comments_for_tables_grouped_by_schema_and_table_name(connection: cx_Oracle.Connection,
                                                                      table_names: [str]):
    """
    Fetch column comments for given tables across all accessible schemas.

    :param connection: Database connection object
    :param table_names: List of table names
    :return: Dictionary grouped by schema and table name, containing column comments
    """
    cursor = connection.cursor()
    table_names_upper = [name.upper() for name in table_names]
    cursor.execute(f"""
        SELECT owner, table_name, column_name, comments
        FROM ALL_COL_COMMENTS
        WHERE table_name IN ({','.join([':name' + str(i) for i in range(len(table_names))])})
    """, {f'name{i}': name for i, name in enumerate(table_names_upper)})
    results = cursor.fetchall()
    cursor.close()

    grouped_comments = {}
    for row in results:
        schema, table_name, column_name, comments = row
        if schema not in grouped_comments:
            grouped_comments[schema] = {}
        if table_name not in grouped_comments[schema]:
            grouped_comments[schema][table_name] = {}
        grouped_comments[schema][table_name][column_name] = comments
    return grouped_comments


def fetch_indexes_for_tables_grouped_by_schema_and_table_name(connection: cx_Oracle.Connection, table_names: [str]):
    """
    Fetch index details for given tables across all accessible schemas.

    :param connection: Database connection object
    :param table_names: List of table names
    :return: Dictionary grouped by schema and table name, containing index metadata
    """
    cursor = connection.cursor()
    table_names_upper = [name.upper() for name in table_names]
    cursor.execute(f"""
        SELECT 
            ai.owner,
            ai.table_name,
            ai.index_name,
            ai.uniqueness,
            ai.tablespace_name,
            ai.ini_trans,
            ai.max_trans,
            ai.logging,
            ai.pct_free,
            uc.constraint_type
        FROM 
            ALL_INDEXES ai
        LEFT JOIN 
            USER_CONSTRAINTS uc
        ON 
            ai.index_name = uc.index_name
        AND 
            ai.table_name = uc.table_name
        AND 
            ai.owner = uc.owner
        WHERE 
            ai.table_name IN ({','.join([':name' + str(i) for i in range(len(table_names))])})
        ORDER BY 
            ai.owner, 
            ai.table_name, 
            ai.index_name;
    """, {f'name{i}': name for i, name in enumerate(table_names_upper)})
    results = cursor.fetchall()
    cursor.close()

    grouped_indexes = {}
    for row in results:
        schema, table_name, index_name, uniqueness, tablespace_name, ini_trans, max_trans, logging, pct_free, constraint_type = row
        if schema not in grouped_indexes:
            grouped_indexes[schema] = {}
        if table_name not in grouped_indexes[schema]:
            grouped_indexes[schema][table_name] = []
        grouped_indexes[schema][table_name].append({
            "name": index_name,
            "constraint_type": constraint_type,
            "uniqueness": uniqueness,
            "tablespace": tablespace_name,
            "ini_trans": ini_trans,
            "max_trans": max_trans,
            "logging": logging,
            "pct_free": pct_free
        })
    return grouped_indexes


def fetch_full_indexes_for_tables_grouped_by_schema_and_table_name(connection: cx_Oracle.Connection,
                                                                   table_names: [str]):
    """
    Fetch index details along with column metadata and expressions for given tables in a single query.

    :param connection: Database connection object
    :param table_names: List of table names
    :return: Dictionary grouped by schema and table name, containing index metadata with column details
    """
    cursor = connection.cursor()
    table_names_upper = [name.upper() for name in table_names]

    # Combined query to fetch index, column, and expression details
    query = f"""
        SELECT 
            ai.owner AS schema_name,
            ai.table_name,
            ai.index_name,
            ai.index_type,
            ai.uniqueness,
            ai.tablespace_name,
            ai.ini_trans,
            ai.max_trans,
            ai.logging,
            ai.pct_free,
            aic.column_name,
            aic.column_position,
            aic.descend,
            aie.column_expression,
            ac.constraint_type
        FROM 
            ALL_INDEXES ai
        LEFT JOIN 
            ALL_IND_COLUMNS aic 
            ON ai.owner = aic.index_owner 
            AND ai.index_name = aic.index_name
        LEFT JOIN 
            ALL_IND_EXPRESSIONS aie
            ON ai.owner = aie.index_owner
            AND ai.index_name = aie.index_name
            AND aic.column_position = aie.column_position
        LEFT JOIN 
            ALL_CONSTRAINTS ac
            ON ai.owner = ac.owner
            AND ai.index_name = ac.index_name
            AND ai.table_name = ac.table_name
        WHERE 
            ai.table_name IN ({','.join([':name' + str(i) for i in range(len(table_names))])})
        ORDER BY 
            ai.owner, 
            ai.table_name, 
            ai.index_name, 
            aic.column_position
    """

    cursor.execute(query, {f'name{i}': name for i, name in enumerate(table_names_upper)})
    results = cursor.fetchall()
    cursor.close()

    # Build the structure
    grouped_indexes = {}
    for row in results:
        (schema, table_name, index_name, index_type, uniqueness, tablespace_name, ini_trans, max_trans,
         logging, pct_free, column_name, column_position, descend, column_expression, constraint_type) = row

        if schema not in grouped_indexes:
            grouped_indexes[schema] = {}
        if table_name not in grouped_indexes[schema]:
            grouped_indexes[schema][table_name] = []

        # Check if the index already exists in the list
        index_list = grouped_indexes[schema][table_name]
        index = next((idx for idx in index_list if idx["name"] == index_name), None)

        if not index:
            # Add new index entry
            index = {
                "name": index_name,
                "uniqueness": uniqueness,
                "constraint_type" : constraint_type,
                "tablespace": tablespace_name,
                "ini_trans": ini_trans,
                "max_trans": max_trans,
                "logging": logging,
                "pct_free": pct_free,
                "columns": []
            }
            index_list.append(index)

        # Add column or expression details if available
        if column_name or column_expression:
            index["columns"].append({
                "column_name": column_name,
                "column_position": column_position,
                "descend": descend,
                "index_type": index_type,
                "column_expression": column_expression
            })

    return grouped_indexes
