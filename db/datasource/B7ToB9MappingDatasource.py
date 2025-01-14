from datetime import datetime
from typing import List, Dict

import cx_Oracle


def get_full_mapping_by_name_list(b7_names: list, db_connection) -> list:
    """
    Retrieve all fields from the GTTBTMPEO table for a list of GZTBTMPEO_B7_NOMBRE values.

    Args:
        b7_names (list): A list of values for GZTBTMPEO_B7_NOMBRE to query.
        db_connection (cx_Oracle.Connection): The Oracle database connection.

    Returns:
        list: A list of dictionaries containing all fields of the matched rows.
    """
    if not b7_names:
        return []

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

    cursor = db_connection.cursor()

    # Create the bind variables dictionary
    bind_vars = {f"name{i}": name for i, name in enumerate(b7_names)}

    cursor.execute(query, bind_vars)
    rows = cursor.fetchall()

    # Map the result rows to dictionaries
    columns = [col[0] for col in cursor.description]
    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    return result

def get_full_mapping_by_name(b7_name: str, db_connection) -> dict:
    """
    Retrieve all fields from the GTTBTMPEO table by GZTBTMPEO_B7_NOMBRE.

    Args:
        b7_name (str): The value of GZTBTMPEO_B7_NOMBRE to query.
        db_connection (cx_Oracle.Connection): The Oracle database connection.

    Returns:
        dict: A dictionary containing all fields of the matched row or None if no match is found.
    """
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
    FROM GTTBTMPEO
    WHERE GZTBTMPEO_B7_NOMBRE = :b7_name
    """

    cursor = db_connection.cursor()
    cursor.execute(query, b7_name=b7_name)

    row = cursor.fetchone()
    if row:
        # Map the result to a dictionary
        columns = [col[0] for col in cursor.description]
        result = dict(zip(columns, row))
    else:
        result = None

    cursor.close()
    return result





def insert_data_to_table(rows_to_insert: List[Dict[str, str]], db_connection: cx_Oracle.Connection):
    """
    Inserts data into the GZTBTMPEO table with additional default values.

    Args:
        rows_to_insert (List[Dict[str, str]]): List of dictionaries containing data to insert.
        db_connection (cx_Oracle.Connection): The Oracle database connection.
    """
    if not rows_to_insert:
        print("No valid rows to insert.")
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
        cursor = db_connection.cursor()
        try:
            cursor.execute(formatted_query)
            db_connection.commit()
            print("Successfully inserted row.")
        except cx_Oracle.DatabaseError as e:
            print(f"Error inserting row: {e}")
            db_connection.rollback()
        finally:
            cursor.close()

    print(f"Successfully inserted {len(rows_to_insert)} rows.")
