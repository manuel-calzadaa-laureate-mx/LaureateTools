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