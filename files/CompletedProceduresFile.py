import logging
import os

from db.datasource.ProceduresDatasource import query_sources
from tools.FileTools import read_csv_file

B7_SOURCE_CODE = "../workfiles/b7_sources"

COMPLETED_PROCEDURES_FILE = "../workfiles/completed_procedures.csv"


def _extract_package_body_specific_object_from_source_code_data(source_code_lines: str, procedure_name: str):
    """
    Extract the source code for a specific procedure or function from the given package source.

    Args:
        source_code_lines (list): Lines of the package source code.
        procedure_name (str): The name of the procedure or function to extract.

    Returns:
        list: Lines of the source code for the specified procedure or function.
    """
    in_procedure = False
    procedure_code = []

    # Normalize procedure name for matching (remove spaces, case-insensitive comparison)
    procedure_name = procedure_name.strip().lower()

    for line in source_code_lines:
        normalized_line = line.strip().lower()

        # Check if the line contains the procedure or function definition
        if (f"procedure {procedure_name}" in normalized_line or
                f"function {procedure_name}" in normalized_line):
            in_procedure = True  # Start capturing lines
            procedure_code.append(line)
            continue

        # Stop capturing when we reach the `END` of the procedure or function
        if in_procedure:
            procedure_code.append(line)
            if normalized_line == "end;" or normalized_line.startswith(f"end {procedure_name}"):
                break

    return procedure_code


def _group_data_into_packages(csv_data):
    grouped_rows = {}
    for row in csv_data:
        package = row['Package'].strip() if row['Package'] else None

        if package not in grouped_rows:
            grouped_rows[package] = []

        grouped_rows[package].append(row)
    return grouped_rows


def _write_extracted_data_to_source_code_files(extracted_data: dict, source_code_folder: str):
    for entry in extracted_data['source_codes']:
        file_name = entry['file_name']
        source_code_lines = entry['source_code']
        full_file_path = os.path.join(source_code_folder, file_name)
        with open(full_file_path, 'w', encoding='utf-8') as sql_file:
            for line in source_code_lines:
                sql_file.write(line)


def extract_source_code_from_completed_procedures_file():
    """Read, process, and write extracted source code."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    completed_procedures_csv_file_path = os.path.join(script_dir, COMPLETED_PROCEDURES_FILE)
    csv_data = read_csv_file(completed_procedures_csv_file_path)
    grouped_data = _group_data_into_packages(csv_data)
    extracted_data = _process_source_code_extraction(grouped_data)
    source_code_folder = os.path.join(script_dir, B7_SOURCE_CODE)
    _write_extracted_data_to_source_code_files(extracted_data, source_code_folder)


def _process_source_code_extraction(data: dict) -> dict:
    source_codes = []

    for package, rows in data.items():
        try:
            owner = rows[0]['Owner'].strip()
            local_package = package if package else "NONE"
            logging.info(f"Processing package group: Owner={owner}, Package={local_package}")

            package_source_code = None
            if package:
                package_source_code = query_sources(
                    owner=owner,
                    package=package
                )

            for row in rows:
                procedure = row['Procedure'].strip()
                function = row['Function'].strip() if row['Function'] else None

                file_name = f"{owner}.{local_package}.{function}.sql" if function else f"{owner}.{local_package}.{procedure}.sql"

                source_code_lines = package_source_code
                if not package:
                    logging.info(
                        f"Extracting individual source code: Owner={owner}, Procedure={procedure}, Function={function}")
                    source_code_lines = query_sources(
                        owner=owner,
                        procedure=procedure,
                        function=function
                    )

                specific_source_code = _process_source_code(source_code_lines, package, procedure, function)

                # Append the expected JSON structure
                source_codes.append({
                    "file_name": file_name,
                    "source_code": specific_source_code
                })

        except Exception as e:
            logging.error(f"Error processing package group: Owner={owner}, Package={local_package}, Error: {e}")

    return {"source_codes": source_codes}


def _process_source_code(source_code_lines: str,
                         package: str = None,
                         procedure: str = None,
                         function: str = None) -> str:
    """
    Writes the source code to the specified file, based on whether it's a procedure or function in a package or standalone.

    Args:
        source_code_lines (str): The source code lines to write.
        package (str): The package name, if applicable.
        procedure (str): The procedure name, if applicable.
        function (str): The function name, if applicable.
        output_file_path (str): The output file path.
    """
    if package:
        # If part of a package, extract specific code (function or procedure)
        if procedure:
            specific_source_code = _extract_package_body_specific_object_from_source_code_data(source_code_lines,
                                                                                               procedure)
        elif function:
            specific_source_code = _extract_package_body_specific_object_from_source_code_data(source_code_lines,
                                                                                               function)
        else:
            specific_source_code = source_code_lines  # If no specific procedure or function, use full code

    else:
        # Not part of a package, write the entire source code
        specific_source_code = source_code_lines

    return specific_source_code


if __name__ == "__main__":
    extract_source_code_from_completed_procedures_file()
