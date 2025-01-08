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
