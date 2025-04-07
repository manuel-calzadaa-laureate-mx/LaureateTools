import logging
from datetime import datetime
from typing import List, Dict

import cx_Oracle

from db.oracle_database_tools import OracleDBConnectionPool


def query_mapping_table_by_object_type(db_pool: OracleDBConnectionPool, mapping_object_type: str) -> list[dict]:
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()
        # Construct the query with a bind variable for each name
        query = f"""
        SELECT 
            GZTBTMPEO_B7_TIPO,
            GZTBTMPEO_B7_ESQUEMA,
            GZTBTMPEO_B7_PAQUETE,
            GZTBTMPEO_B7_NOMBRE,
            GZTBTMPEO_B9_TIPO,
            GZTBTMPEO_B9_ESQUEMA,
            GZTBTMPEO_B9_PAQUETE,
            GZTBTMPEO_B9_NOMBRE,
            GZTBTMPEO_DESCRIPCION,
            GZTBTMPEO_OBSERVACION,
            GZTBTMPEO_ACTIVITY_DATE,
            GZTBTMPEO_USER,
            GZTBTMPEO_DATA_ORIGIN
        FROM GZTBTMPEO
        WHERE GZTBTMPEO_B7_TIPO = :object_type
        """

        cursor.execute(query, object_type=mapping_object_type)
        rows = cursor.fetchall()
        results = []
        for row in rows:
            if row:
                # Map the result to a dictionary
                columns = [col[0] for col in cursor.description]
                results.append(dict(zip(columns, row)))
        return results


def query_mapping_table(db_pool: OracleDBConnectionPool) -> list[dict]:
    """
    Retrieve all fields from the GTTBTMPEO table for a list of GZTBTMPEO_B7_NOMBRE values.

    Returns:
        list: A list of dictionaries containing all fields of the matched rows.
    """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Construct the query with a bind variable for each name
        query = f"""
        SELECT 
            GZTBTMPEO_B7_TIPO,
            GZTBTMPEO_B7_ESQUEMA,
            GZTBTMPEO_B7_PAQUETE,
            GZTBTMPEO_B7_NOMBRE,
            GZTBTMPEO_B9_TIPO,
            GZTBTMPEO_B9_ESQUEMA,
            GZTBTMPEO_B9_PAQUETE,
            GZTBTMPEO_B9_NOMBRE,
            GZTBTMPEO_DESCRIPCION,
            GZTBTMPEO_OBSERVACION,
            GZTBTMPEO_ACTIVITY_DATE,
            GZTBTMPEO_USER,
            GZTBTMPEO_DATA_ORIGIN
        FROM GZTBTMPEO
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        results = []
        for row in rows:
            if row:
                # Map the result to a dictionary
                columns = [col[0] for col in cursor.description]
                results.append(dict(zip(columns, row)))
        return results


def query_mapping_by_b7_names(db_pool: OracleDBConnectionPool, b7_names: []) -> [dict]:
    if not b7_names:
        return []

    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

        # Construct the query with a bind variable for each name
        placeholders = ', '.join([f":name{i}" for i in range(len(b7_names))])
        query = f"""
        SELECT 
            GZTBTMPEO_B7_TIPO,
            GZTBTMPEO_B7_ESQUEMA,
            GZTBTMPEO_B7_PAQUETE,
            GZTBTMPEO_B7_NOMBRE,
            GZTBTMPEO_B9_TIPO,
            GZTBTMPEO_B9_ESQUEMA,
            GZTBTMPEO_B9_PAQUETE,
            GZTBTMPEO_B9_NOMBRE,
            GZTBTMPEO_DESCRIPCION,
            GZTBTMPEO_OBSERVACION,
            GZTBTMPEO_ACTIVITY_DATE,
            GZTBTMPEO_USER,
            GZTBTMPEO_DATA_ORIGIN
        FROM GZTBTMPEO
        WHERE GZTBTMPEO_B7_NOMBRE IN ({placeholders})
        """

        # Create the bind variables dictionary
        bind_vars = {f"name{i}": name for i, name in enumerate(b7_names)}
        cursor.execute(query, bind_vars)
        rows = cursor.fetchall()
        results = []
        for row in rows:
            if row:
                # Map the result to a dictionary
                columns = [col[0] for col in cursor.description]
                results.append(dict(zip(columns, row)))

        return results


def query_mapping_by_b7_name(db_pool: OracleDBConnectionPool, b7_name: str) -> dict:
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()
        query = """
        SELECT 
            GZTBTMPEO_B7_TIPO,
            GZTBTMPEO_B7_ESQUEMA,
            GZTBTMPEO_B7_PAQUETE,
            GZTBTMPEO_B7_NOMBRE,
            GZTBTMPEO_B9_TIPO,
            GZTBTMPEO_B9_ESQUEMA,
            GZTBTMPEO_B9_PAQUETE,
            GZTBTMPEO_B9_NOMBRE,
            GZTBTMPEO_DESCRIPCION,
            GZTBTMPEO_OBSERVACION,
            GZTBTMPEO_ACTIVITY_DATE,
            GZTBTMPEO_USER,
            GZTBTMPEO_DATA_ORIGIN
        FROM GZTBTMPEO
        WHERE GZTBTMPEO_B7_NOMBRE = :b7_name
        """

        cursor.execute(query, b7_name=b7_name)

        row = cursor.fetchone()
        if row:
            # Map the result to a dictionary
            columns = [col[0] for col in cursor.description]
            result = dict(zip(columns, row))
        else:
            result = None

        return result


def insert_mapping_data(db_pool: OracleDBConnectionPool, rows_to_insert: List[Dict[str, str]]):
    """
    Inserts data into the GZTBTMPEO table with additional default values.

    Args:
        :param rows_to_insert:
        :param db_pool:
    """
    with db_pool.get_connection() as connection:
        cursor = connection.cursor()

    if not rows_to_insert:
        logging.info("No valid rows to insert.")
        return

    # Define default values
    default_description = "N/A"
    default_observation = None
    default_user = "WIKI-BOT"
    default_data_origin = "MANUAL"
    current_date = datetime.now().strftime("%d/%m/%y")

    # Prepare the insert query
    insert_query = """
    INSERT INTO GZTBTMPEO (
        GZTBTMPEO_B7_TIPO,
        GZTBTMPEO_B7_ESQUEMA,
        GZTBTMPEO_B7_PAQUETE,
        GZTBTMPEO_B7_NOMBRE,
        GZTBTMPEO_B9_TIPO,
        GZTBTMPEO_B9_ESQUEMA,
        GZTBTMPEO_B9_PAQUETE,
        GZTBTMPEO_B9_NOMBRE,
        GZTBTMPEO_DESCRIPCION,
        GZTBTMPEO_OBSERVACION,
        GZTBTMPEO_ACTIVITY_DATE,
        GZTBTMPEO_USER,
        GZTBTMPEO_DATA_ORIGIN
    ) VALUES (
        '{B7_TIPO}',
        '{B7_ESQUEMA}',
        '{B7_PAQUETE}',
        '{B7_NOMBRE}',
        '{B9_TIPO}',
        '{B9_ESQUEMA}',
        '{B9_PAQUETE}',
        '{B9_NOMBRE}',
        '{DESCRIPCION}',
        '{OBSERVACION}',
        TO_DATE('{ACTIVITY_DATE}', 'DD/MM/YY'),
        '{USER}',
        '{DATA_ORIGIN}'
    )"""

    # Add default values to each row and construct the final query with values
    for row in rows_to_insert:
        row.update({
            "DESCRIPCION": default_description,
            "OBSERVACION": default_observation,
            "ACTIVITY_DATE": current_date,
            "USER": default_user,
            "DATA_ORIGIN": default_data_origin
        })

        # Replace 'none' with 'N/A' for all keys in the row
        for key, value in row.items():
            if value == 'none':
                row[key] = 'N/A'

        # Create a final query string by replacing placeholders with actual values
        formatted_query = insert_query.format(
            B7_TIPO=row['B7_TIPO'],
            B7_ESQUEMA=row['B7_ESQUEMA'],
            B7_PAQUETE=row['B7_PAQUETE'],
            B7_NOMBRE=row['B7_NOMBRE'],
            B9_TIPO=row['B9_TIPO'],
            B9_ESQUEMA=row['B9_ESQUEMA'],
            B9_PAQUETE=row['B9_PAQUETE'],
            B9_NOMBRE=row['B9_NOMBRE'],
            DESCRIPCION=row['DESCRIPCION'],
            OBSERVACION=row['OBSERVACION'] if row['OBSERVACION'] is not None else 'NULL',
            ACTIVITY_DATE=row['ACTIVITY_DATE'],
            USER=row['USER'],
            DATA_ORIGIN=row['DATA_ORIGIN']
        )

        print(f"Executing query: {formatted_query}")

        # Insert the row into the database
        try:
            cursor.execute(formatted_query)
            connection.commit()
            logging.info("Successfully inserted row.")
        except cx_Oracle.DatabaseError as e:
            logging.error(f"Error inserting row: {e}")
            connection.rollback()

    logging.info(f"Successfully inserted {len(rows_to_insert)} rows.")
