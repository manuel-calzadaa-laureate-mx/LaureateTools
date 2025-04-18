import logging
import os
from typing import List, Dict

from db.database_properties import DatabaseEnvironment
from db.datasource.functions_datasource import get_public_package_object_owner, get_independent_object_owners, \
    get_private_package_object_owner
from db.datasource.procedures_datasource import query_sources
from db.oracle_database_tools import OracleDBConnectionPool
from files.source_code_file import get_source_code_folder
from tools.common_tools import get_all_current_owners, split_table_name_into_package_and_table_name
from tools.file_tools import read_csv_file, write_csv_file
from tools.package_tools import get_packages_as_list

COMPLETED_PROCEDURES_FILE_PATH = "../workfiles/b9_output/completed_procedures.csv"


def _is_packaged_object(object_name: str) -> bool:
    """Check if the function follows the PACKAGE.FUNCTION_NAME pattern."""
    return '.' in object_name


def _normalize_object_names(objects: list) -> set:
    """
    Normalize objects names to ensure consistency.
    Extracts only the object name from PACKAGE.OBJECT patterns.
    """
    return {object.split('.')[-1] for object in objects}


def _get_only_new_entries(to_append: list[dict], already_in_the_file: list[dict]) -> list[dict]:
    """Return the list of dicts from to_append that are not in already_in_the_file."""

    # Normalize already_in_the_file and convert to a set for quick lookup
    already_in_set = {
        ("" if obj['Function'] is None else obj['Function'], obj['Owner'], obj['Package'], obj['Procedure'])
        for obj in already_in_the_file
    }

    # Filter out items from to_append that exist in already_in_the_file
    filtered_to_append = [
        obj for obj in to_append
        if ("" if obj['Function'] is None else obj['Function'], obj['Owner'], obj['Package'],
            obj['Procedure']) not in already_in_set
    ]

    return filtered_to_append


def _write_completed_procedures_data(completed_data_to_append):
    completed_procedures = get_completed_procedures()
    only_new_entries = _get_only_new_entries(to_append=completed_data_to_append,
                                             already_in_the_file=completed_procedures)
    if only_new_entries:
        write_csv_file(output_file=get_completed_procedures_file_path(), data_to_write=only_new_entries,
                       is_append=True)


def update_missing_procedures_to_add_manager(objects: list[dict],
                                             db_pool: OracleDBConnectionPool) -> None:
    logging.info("Starting: update missing procedures to add")
    object_data_to_append = _find_missing_data_to_add(db_pool=db_pool, objects=objects)
    logging.info(f"this is the data to be added: {object_data_to_append}")
    _write_completed_procedures_data(completed_data_to_append=object_data_to_append)


def _find_missing_data_to_add(objects: list[dict],
                              db_pool: OracleDBConnectionPool,
                              database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER9) -> \
        list[list]:
    """Update the procedures file based on whether the function is packaged or independent.
    :param db_pool:
    """
    hidden_dependencies = []
    for one_object in objects:
        logging.info(f"Processing function: {one_object}")
        if one_object["PACKAGE"]:
            result = get_public_package_object_owner(object_dict=one_object, db_pool=db_pool)

            if result is None:
                result = get_private_package_object_owner(object_dict=one_object, db_pool=db_pool)

            if result is None:
                logging.info(f"Could not retrieve owner/package/procedure for {one_object}")

                # check if package is really the owner
                all_owners = get_all_current_owners(db_pool=db_pool)
                # retrieve the "package" value
                supposed_owner, object_name = split_table_name_into_package_and_table_name(obj_name=one_object)
                if supposed_owner in all_owners:
                    hidden_dependencies.append([supposed_owner, None, object_name, None])
                    logging.info(f"Success! it was the owner {one_object}")
                continue
            owner, package, procedure = result
            hidden_dependencies.append(
                {"Owner": owner, "Package": package, "Procedure": procedure, "Function": None})

        else:
            if one_object["OWNER"]:
                owner = one_object["OWNER"]
                procedure = one_object["NAME"]
                hidden_dependencies.append(
                    {"Owner": owner, "Package": None, "Procedure": procedure, "Function": None})
            else:
                result = get_independent_object_owners(one_object, database_environment)
                logging.info(f"non-packaged object: {result}")
                if result is None:
                    logging.info(f"Could not retrieve owner/procedure for {one_object}")
                    continue
                for owner, procedure in result:
                    hidden_dependencies.append(
                        {"Owner": owner, "Package": None, "Procedure": procedure, "Function": None})

    return hidden_dependencies


def _extract_all_package_body_objects_from_source_code_data(source_code_lines: str, owner: str, package: str):
    objects = []
    current_object = None
    object_code = []

    for line in source_code_lines:
        stripped_line = line.strip()
        upper_line = stripped_line.upper()

        # Check for procedure or function start
        if (upper_line.startswith('PROCEDURE ') or upper_line.startswith('FUNCTION ')) and (
                len(stripped_line) > len('PROCEDURE ') and stripped_line[len('PROCEDURE'):].lstrip()[0].isalnum()):

            if current_object:  # Save previous object if exists
                objects.append({
                    'object_package': package,
                    'object_owner': owner,
                    'object_type': current_object['type'].upper(),
                    'object_name': current_object['name'],
                    'code': ''.join(object_code).strip()
                })

            # Start new object
            parts = stripped_line.split()
            object_type = parts[0].upper()
            object_name = parts[1].split('(')[0]  # Get name before parameters

            current_object = {
                'object_package': package,
                'object_owner': owner,
                'type': object_type,
                'name': object_name
            }
            object_code = [line]

        elif current_object:  # If we're inside an object
            object_code.append(line)

            # Improved and properly parenthesized END detection
            if (upper_line.replace(' ', '') == 'END;' or
                    (upper_line.startswith('END ') and
                     (stripped_line.endswith(';') and
                      (upper_line.endswith(';') or
                       f'END {current_object["name"].upper()};' in upper_line)))):
                objects.append({
                    'object_package': package,
                    'object_owner': owner,
                    'object_type': current_object['type'],
                    'object_name': current_object['name'],
                    'code': ''.join(object_code).strip()
                })
                current_object = None
                object_code = []

    # Add any remaining object
    if current_object:
        objects.append({
            'object_package': package,
            'object_owner': owner,
            'object_type': current_object['type'],
            'object_name': current_object['name'],
            'code': ''.join(object_code).strip()
        })

    return objects


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

        if in_procedure:
            # Check for new procedure/function before appending
            if (f"procedure " in normalized_line or
                    f"function " in normalized_line):
                break

            procedure_code.append(line)

            if normalized_line.startswith(f"end {procedure_name}"):
                break

    return procedure_code


def _group_list_of_objects_by_packages_from_csv_data(csv_data: list):
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
        full_file_path = os.path.join(source_code_folder, file_name)

        if os.path.exists(full_file_path):
            logging.info(f"skipping {file_name} creation")
            continue

        # Write the new file
        with open(full_file_path, 'w', encoding='utf-8') as sql_file:
            sql_file.writelines(entry['source_code'])


def create_source_code_manager(db_pool: OracleDBConnectionPool,
                               database_environment: DatabaseEnvironment):
    """Read, process, and write extracted source code.
    :param database_environment:
    :param db_pool:
    """
    logging.info("Starting: extract source code")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    completed_procedures_csv_file_path = os.path.join(script_dir, get_completed_procedures_file_path())
    csv_data = read_csv_file(completed_procedures_csv_file_path)
    grouped_data = _group_list_of_objects_by_packages_from_csv_data(csv_data=csv_data)
    mapped_extracted_data = _group_source_code_maped_by_package_and_name(db_pool=db_pool, data=grouped_data)
    grouped_data_by_filename = _create_data_process_by_filename(grouped_data, mapped_extracted_data)
    source_code_folder = os.path.join(script_dir, get_source_code_folder(database_environment))
    _write_extracted_data_to_source_code_files(grouped_data_by_filename, source_code_folder)
    logging.info("Ending: extract source code")


def _create_data_process_by_filename(grouped_data: List[dict], mapped_extracted_data: Dict) -> Dict:
    source_codes = []
    for one_package_data in grouped_data:
        logging.info(f"Processing {one_package_data} package")
        one_package_objects = grouped_data[one_package_data]
        for one_package_object in one_package_objects:
            procedure = one_package_object.get("Procedure")
            function = one_package_object.get("Function")
            owner = one_package_object.get("Owner")
            package = one_package_object.get("Package")

            # Determine object name
            if procedure:
                object_name = procedure
            elif function:
                object_name = function
            else:
                logging.warning(f"Neither procedure nor function specified for {object_name}")
                continue
            logging.info(f"Processing {object_name}")
            extracted_data = mapped_extracted_data.get((package, object_name))

            specific_source_code = extracted_data.get("code") if extracted_data else None

            file_extension = ".sql.missing" if not specific_source_code else ".sql"
            file_name = f"{owner}.{package}.{function}{file_extension}" if function else f"{owner}.{package}.{procedure}{file_extension}"

            # Append the expected JSON structure
            source_codes.append({
                "file_name": file_name,
                "source_code": specific_source_code
            })
    return {"source_codes": source_codes}


def _group_source_code_maped_by_package_and_name(db_pool: OracleDBConnectionPool, data: dict) -> dict:
    object_map = {}  # New: Will store objects with (package, object_name) keys

    for package, rows in data.items():
        try:
            ## SWEEP PACKAGES
            if package:
                owner = rows[0]['Owner'].strip()
                logging.info(f"Processing package: Owner={owner}, Package={package}")

                package_source_code = query_sources(
                    owner=owner,
                    package=package,
                    db_pool=db_pool
                )

                # Get all objects from the package body
                package_body_objects = _extract_all_package_body_objects_from_source_code_data(
                    source_code_lines=package_source_code,
                    owner=owner,
                    package=package
                )

                # Add each package object to the map
                for obj in package_body_objects:
                    key = (obj['object_package'], obj['object_name'])  # Tuple key
                    object_map[key] = obj  # Store object in map

                continue

            ## SWEEP INDEPENDENT FUNCTION AND PROCEDURES
            if not package:
                for row in rows:
                    owner = row['Owner'].strip()
                    procedure = row['Procedure'].strip() if row['Procedure'] else None
                    function = row['Function'].strip() if row['Function'] else None
                    object_name = row['Name'].strip()

                    # Determine object type
                    if procedure:
                        object_type = "PROCEDURE"
                    elif function:
                        object_type = "FUNCTION"
                    else:
                        logging.warning(f"Neither procedure nor function specified for {object_name}")
                        continue

                    logging.info(
                        f"Extracting individual source code: Owner={owner}, "
                        f"Object={object_name}, Type={object_type}"
                    )

                    source_code_lines = query_sources(
                        owner=owner,
                        procedure=procedure,
                        function=function,
                        db_pool=db_pool
                    )

                    # Create object entry
                    obj = {
                        'object_owner': owner,
                        'object_package': None,  # Standalone objects have no package
                        'object_type': object_type,
                        'object_name': object_name,
                        'code': '\n'.join(source_code_lines).strip()
                    }

                    key = (None, obj['object_name'])  # Key for standalone objects
                    object_map[key] = obj  # Store in map

        except Exception as e:
            logging.error(f"Error processing package/row {package}: {str(e)}")
            continue

    return object_map  # Returns a dict with (package, object_name) keys


def _process_source_code_extraction(db_pool: OracleDBConnectionPool, data: dict) -> dict:
    source_codes = []

    for package, rows in data.items():
        try:
            owner = rows[0]['Owner'].strip()
            local_package = package if package else "NONE"
            logging.info(f"Processing package group: Package={local_package}")

            package_source_code = None
            if package:
                package_source_code = query_sources(
                    owner=owner,
                    package=package,
                    db_pool=db_pool
                )

            for row in rows:
                procedure = row['Procedure'].strip()
                function = row['Function'].strip() if row['Function'] else None
                source_code_lines = package_source_code
                if not package:
                    logging.info(
                        f"Extracting individual source code: Owner={owner}, Procedure={procedure}, Function={function}")
                    source_code_lines = query_sources(
                        owner=owner,
                        procedure=procedure,
                        function=function,
                        db_pool=db_pool
                    )

                specific_source_code = _process_source_code(source_code_lines, package, procedure, function)

                file_extension = ".sql.missing" if not specific_source_code else ".sql"
                file_name = f"{owner}.{local_package}.{function}{file_extension}" if function else f"{owner}.{local_package}.{procedure}{file_extension}"

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


def _find_all_objects_in_package_body_source_code(source_code_lines: str,
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


def _find_one_object_in_package_body_source_code(source_code_lines: str,
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


def get_completed_procedures_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, COMPLETED_PROCEDURES_FILE_PATH)
    return source_folder


def get_completed_procedures() -> list[dict]:
    return read_csv_file(get_completed_procedures_file_path())


def create_package_specification_source_code_manager(db_pool: OracleDBConnectionPool,
                                                     database_environment: DatabaseEnvironment):
    """Read, process, and write extracted source code."""
    logging.info("Starting: extract package specification source code")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    completed_procedures_csv_file_path = os.path.join(script_dir, get_completed_procedures_file_path())
    csv_data = read_csv_file(completed_procedures_csv_file_path)

    ## Extract unique package names
    package_names = set()
    for row in csv_data:
        package_name = row.get("Package")
        if package_name:
            package_names.add(package_name)

    ## Extract package specification code:
    all_package_records = get_packages_as_list(package_owner="UVM", package_names=list(package_names), db_pool=db_pool)

    # Initialize the extracted_data dictionary
    extracted_data = {'source_codes': []}

    for one_package_name, one_package_attributes in all_package_records.items():
        # Extract the package owner
        package_owner = one_package_attributes.get("owner")

        # Extract the PACKAGE and PACKAGE BODY sections
        package_specs_code = one_package_attributes.get('code', {}).get('PACKAGE', {}).get('lines', [])
        package_body_code = one_package_attributes.get('code', {}).get('PACKAGE BODY', {}).get('lines', [])

        # Combine the lines into a single string for each section
        def combine_lines(lines):
            return ''.join(line['text'] for line in lines)

        package_specs_code_str = combine_lines(package_specs_code)
        package_body_code_str = combine_lines(package_body_code)

        # Create the file name
        file_name = f"{package_owner}.{one_package_name}.NONE.sql"

        # Append the file name and source code to the extracted_data dictionary
        extracted_data['source_codes'].append({
            'file_name': file_name,
            'source_code': package_specs_code_str  # Use package_specs_code_str for the package specification
        })
    source_code_folder = os.path.join(script_dir, get_source_code_folder(database_environment))
    _write_extracted_data_to_source_code_files(extracted_data, source_code_folder)

    logging.info("Ending: extract source code")


def get_completed_procedures_name_list() -> list:
    """Read, process, and write extracted source code."""
    logging.info("Starting: extract package specification source code")
    all_completed_procedures = get_completed_procedures()
    packages = set()
    for row in all_completed_procedures:
        packages.add(row.get("Package"))
    return list(packages)


if __name__ == "__main__":
    print(get_completed_procedures_name_list())
