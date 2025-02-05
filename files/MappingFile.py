import csv
import json
import os
from typing import Optional, List

from tools.FileTools import read_csv_file

MAPPING_FILE_PATH = "../workfiles/mapping.csv"


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

def get_mapping_file_path()-> str:
    mapping_path = os.path.dirname(os.path.abspath(__file__))
    mapping_file_path = os.path.join(mapping_path, MAPPING_FILE_PATH)
    return mapping_file_path

def get_mapping_data() -> list[dict]:
    return read_csv_file(get_mapping_file_path())

def write_mapping_file(mapping_data: list[dict]) -> None:
    mapping_file_path = get_mapping_file_path()

    with open(mapping_file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "IS_MAPPED",
            "B7_TIPO", "B7_ESQUEMA", "B7_PAQUETE", "B7_NOMBRE",
            "B9_TIPO", "B9_ESQUEMA", "B9_PAQUETE", "B9_NOMBRE"
        ])

        writer.writeheader()
        writer.writerows(mapping_data)