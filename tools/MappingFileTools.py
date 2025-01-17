import csv
import json
from typing import Optional, List


def csv_to_json(csv_file_path: str, json_file_path: Optional[str] = None) -> List[dict]:
    """
    Converts a CSV file to JSON format.

    Args:
        csv_file_path (str): Path to the CSV file.
        json_file_path (Optional[str]): Path to save the JSON file. If None, JSON will not be saved.

    Returns:
        List[dict]: The data from the CSV as a list of dictionaries.
    """
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
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

