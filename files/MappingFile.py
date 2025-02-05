import csv
import json
import os
from enum import Enum
from typing import Optional, List

from tools.FileTools import read_csv_file

MAPPING_FILE_PATH = "../workfiles/mapping.csv"

class MappingFileTypes(Enum):
    TRIGGER = 'TRIGGER'
    PACKAGE = 'PACKAGE'
    PROCEDURE = 'PROCEDURE'
    FUNCTION = 'FUNCTION'
    SEQUENCE = 'SEQUENCE'
    TYPE = 'TYPE'
    VIEW = 'VIEW'
    TABLE = 'TABLE'

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

def _is_valid_mapping_data_row(mapping_data_row: dict)-> bool:
    if mapping_data_row["B9_NOMBRE"].upper() == "NONE":
        return False
    return True

def get_filtered_mapping_data_by_type_and_is_mapped(mapping_object_types: MappingFileTypes)-> list[dict]:
    mapping_data = get_mapping_data_by_type_and_is_mapped(mapping_object_types=mapping_object_types)
    filtered_mapping_data = []
    for one_mapping_data in mapping_data:
        if not _is_valid_mapping_data_row(mapping_data_row=one_mapping_data):
            continue
        filtered_mapping_data.append(one_mapping_data)
    return filtered_mapping_data

def get_mapping_data_by_type_and_is_mapped(mapping_object_types: MappingFileTypes)-> list[dict]:
    mapping_data = get_mapping_data()
    mapping_data_by_type = []
    # Convert IS_MAPPED column to boolean
    for one_mapping_element in mapping_data:
        if one_mapping_element:
            if (one_mapping_element['B7_TIPO'] == mapping_object_types.value and
                one_mapping_element['IS_MAPPED'].lower() == 'true'):
                mapping_data_by_type.append(one_mapping_element)
    return mapping_data_by_type


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