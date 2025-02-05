import csv
from typing import List, Dict

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_db_connection
from db.datasource.MappingDatasource import insert_mapping_data


def upload_mapping_file(mapping_file_name: str) -> List[Dict[str, str]]:
    """
    Reads a CSV file and filters rows based on specific conditions, returning valid rows as a list of dictionaries.

    Args:
        mapping_file_name (str): Path to the CSV file containing the data.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing valid rows.
    """
    rows_to_insert = []

    with open(mapping_file_name, mode='r') as file:
        reader = csv.DictReader(file)

        for row in reader:
            is_mapped = row['IS_MAPPED']
            b9_paquete = row['B9_PAQUETE']
            b9_nombre = row['B9_NOMBRE']
            b7_tipo = row['B7_TIPO']

            # Skip rows where B9_PAQUETE or B9_NOMBRE are "none", "", or empty
            if is_mapped.upper() == 'TRUE':
                continue

            if (b9_paquete.lower() == "none" and b9_nombre.lower() == "none") or (not b9_paquete or not b9_nombre):
                continue

            # Add the row to the list
            rows_to_insert.append({
                "B7_TIPO": row['B7_TIPO'],
                "B7_ESQUEMA": row['B7_ESQUEMA'],
                "B7_PAQUETE": row['B7_PAQUETE'],
                "B7_NOMBRE": row['B7_NOMBRE'],
                "B9_TIPO": row['B9_TIPO'],
                "B9_ESQUEMA": row['B9_ESQUEMA'],
                "B9_PAQUETE": b9_paquete,
                "B9_NOMBRE": b9_nombre
            })

    return rows_to_insert


if __name__ == "__main__":
    mapping_input_file = "../banner9/mapping.csv"
    db_config = '../../db_config.json'
    db_connection = get_db_connection(database_name=DatabaseEnvironment.BANNER9)
    mapping = upload_mapping_file(mapping_file_name=mapping_input_file)
    print(mapping)
    insert_mapping_data(rows_to_insert=mapping)
    db_connection.close()