import csv
import json
import logging
import os


def write_json_file(json_data: dict, output_filename: str) -> None:
    """
    Write a JSON object to a file.

    Args:
        json_data (dict): The JSON-compatible dictionary to write.
        output_filename (str): The path to the output file.
    """
    with open(output_filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(json_data, jsonfile, indent=4)

    logging.info(f'Successfully wrote JSON data to {output_filename}')


def read_json_file(input_filename: str) -> dict:
    try:
        with open(input_filename, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{input_filename}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{input_filename}' is not a valid JSON file.")


def read_csv_file(input_file: str) -> list[dict]:
    """Read the input CSV file into memory as a list of dictionaries."""
    if not os.path.exists(input_file):  # Check if file exists
        return []

    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        return [row for row in reader]


def write_csv_file(output_file: str, data_to_write, is_append: bool = False) -> None:
    """
    Write data to a CSV file. Supports both list of lists and list of dictionaries.
    Allows appending or overwriting the file.

    Args:
        output_file (str): Path to the output CSV file.
        data_to_write (list[list] | list[dict]): Data to write to the CSV.
        is_append (bool): If True, appends data to the file. If False, overwrites the file.
    """
    if data_to_write:

        mode = 'a' if is_append else 'w'  # 'a' for append, 'w' for overwrite
        write_header = not is_append  # Write header only if not appending

        with open(output_file, mode=mode, newline='', encoding='utf-8') as outfile:
            if isinstance(data_to_write[0], dict):  # Handling list[dict]
                headers = list(data_to_write[0].keys())
                writer = csv.DictWriter(outfile, fieldnames=headers)
                if write_header:
                    writer.writeheader()  # Write header only if not appending
                writer.writerows(data_to_write)
            elif isinstance(data_to_write[0], list):  # Handling list[list]
                writer = csv.writer(outfile)
                if write_header:
                    writer.writerow(data_to_write[0])  # Assume the first row contains headers
                    data_to_write = data_to_write[1:]  # Remove the header row if appending
                writer.writerows(data_to_write)
            else:
                raise TypeError("Invalid data format. Expected list of lists or list of dictionaries.")

        logging.info(f"CSV {'appended to' if is_append else 'written to'} {output_file}")
