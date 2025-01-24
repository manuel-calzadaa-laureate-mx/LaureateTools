import csv
import json
import os
from typing import Optional, List
from db.DatabaseProperties import DatabaseObject


def load_mapping_file_to_json(mapping_file_path: str, json_file_path: Optional[str] = None) -> List[dict]:
    """
    Converts a CSV file to JSON format.

    Args:
        mapping_file_path (str): Path to the CSV file.
        json_file_path (Optional[str]): Path to save the JSON file. If None, JSON will not be saved.

    Returns:
        List[dict]: The data from the CSV as a list of dictionaries.
    """
    with open(mapping_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]

    # Convert IS_MAPPED column to boolean
    for row in data:
        row['IS_MAPPED'] = row['IS_MAPPED'].lower() == 'true'

    # Save to JSON file if a path is provided
    if json_file_path:
        with open(json_file_path, mode='w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)

    return data


def read_mapping_data(database_object: DatabaseObject = DatabaseObject.TABLE) -> [dict]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    mapping_file_path = os.path.join(script_dir, "../banner9/mapping.csv")
    all_mapped_records = load_mapping_file_to_json(mapping_file_path=mapping_file_path)
    mapping_data = {}
    for one_record in all_mapped_records:
        if (one_record["IS_MAPPED"]
                and one_record["B7_TIPO"] == database_object.name
                and one_record["B9_NOMBRE"] != 'none'):
            mapping_data[one_record["B7_NOMBRE"]] = {
                "B9_ESQUEMA": one_record["B9_ESQUEMA"],
                "B9_PAQUETE": one_record["B9_PAQUETE"],
                "B9_NOMBRE": one_record["B9_NOMBRE"]
            }
    return mapping_data
