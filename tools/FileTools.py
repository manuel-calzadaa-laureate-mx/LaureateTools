import csv
import json

def write_json_to_file(json_data: dict, output_filename: str) -> None:
    """
    Write a JSON object to a file.

    Args:
        json_data (dict): The JSON-compatible dictionary to write.
        output_filename (str): The path to the output file.
    """
    with open(output_filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(json_data, jsonfile, indent=4)

    print(f'Successfully wrote JSON data to {output_filename}')

def read_json_file(input_filename: str) -> dict:
    try:
        with open(input_filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_filename}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_filename}' is not a valid JSON file.")

def read_csv_file(input_file: str):
    """Read the input CSV file into memory as a list of dictionaries."""
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        return [row for row in reader]

def write_csv_file(output_file: str, data):
    """Write the processed data into a CSV file."""
    with open(output_file, mode='w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(data)