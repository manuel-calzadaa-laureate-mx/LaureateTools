def fetch_attributes_for_sequences(connection, sequence_names):
    """
    Query the ALL_SEQUENCES table to get details for the given list of sequence names.
    """
    cursor = connection.cursor()

    # Create a dynamic query to handle the list of sequence names
    query = """
        SELECT 
            SEQUENCE_OWNER,
            SEQUENCE_NAME,
            MIN_VALUE,
            MAX_VALUE,
            INCREMENT_BY,
            CYCLE_FLAG,
            ORDER_FLAG,
            CACHE_SIZE,
            LAST_NUMBER
        FROM 
            ALL_SEQUENCES
        WHERE SEQUENCE_NAME IN ({})
    """.format(",".join([f"'{name}'" for name in sequence_names]))

    cursor.execute(query)

    sequences = [{
        "sequence_owner": row[0],
        "sequence_name": row[1],
        "min_value": row[2],
        "max_value": row[3],
        "increment_by": row[4],
        "cycle_flag": row[5],
        "order_flag": row[6],
        "cache_size": row[7],
        "last_number": row[8]
    } for row in cursor.fetchall()]

    cursor.close()
    connection.close()
    return sequences
