import logging
import os

import pandas as pd

from db.database_properties import DatabaseEnvironment
from db.oracle_database_tools import OracleDBConnectionPool
from files.b9_completed_procedures_file import update_missing_procedures_to_add_manager, create_source_code_manager, \
    get_completed_procedures_name_list
from files.b9_incomplete_procedures_file import get_incomplete_procedures
from files.dependency_file import extract_unique_existing_objects, extract_object_with_missing_status, \
    filter_missing_status_dependencies, find_delta_of_missing_dependencies, \
    is_object_dependency_procedure_or_function, is_object_need_process
from files.source_code_file import extract_all_dependencies_from_one_source_code_data, get_source_code_folder
from tools.common_tools import get_all_current_owners, split_table_name_into_package_and_table_name, ObjectTargetType
from tools.file_tools import write_csv_file, read_csv_file, read_json_file

DEPENDENCIES_FILE_PATH = "../workfiles/b9_output/dependencies.csv"
MISSING_DEPENDENCIES_FILE_PATH = "../workfiles/b9_output/missing_dependencies.csv"
_OBJECT_DATA_JSON = "../workfiles/b9_output/object_data.json"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_dependencies_data() -> list[dict]:
    dependency_file_path = get_dependency_file_path()
    return read_csv_file(dependency_file_path)


def get_missing_dependencies_data() -> list[dict]:
    missing_dependencies_file_path = get_missing_dependencies_file_path()
    return read_csv_file(missing_dependencies_file_path)


def _extract_unique_dependency_objects(dependency_data: list[dict]) -> list:
    """
    Reads a file with dependency information and extracts unique function dependency names.

    Parameters:
        file_path (str): The path to the dependencies file.

    Returns:
        list: A sorted list of unique function dependency names.
    """
    unique_existing_dependency_objects = []
    unique_dependency_names = []

    try:
        for row in dependency_data:
            if (is_object_dependency_procedure_or_function(object=row,
                                                           object_name="DEPENDENCY_TYPE")
                    and is_object_need_process(object=row, object_name="DEPENDENCY_NAME",
                                               unique_names=unique_dependency_names)):
                unique_dependency_names.append(row['DEPENDENCY_NAME'].strip().upper())
                unique_existing_dependency_objects.append(
                    {"STATUS": row["STATUS"],
                     "DEPENDENCY_OWNER": row['DEPENDENCY_OWNER'],
                     "DEPENDENCY_PACKAGE": row['DEPENDENCY_PACKAGE'],
                     "DEPENDENCY_NAME": row['DEPENDENCY_NAME'].strip().upper()})
        return unique_existing_dependency_objects

    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []


def _find_missing_dependencies() -> list:
    logging.info(f"Starting: find missing dependencies")

    dependencies_data = read_csv_file(get_dependency_file_path())
    # ALL OBJECTS IN DEPENDENCY DATA
    all_unique_existing_objects = extract_unique_existing_objects(dependencies_data)

    # ALL DEPENDENCY OBJECTS IN DEPENDENCY DATA
    all_dependency_objects = _extract_unique_dependency_objects(dependencies_data)

    # OBJECTS WITH MISSING STATUS IN DEPENDENCY DATA
    all_missing_objects = extract_object_with_missing_status(dependencies_data)

    # REMOVE OBJECTS WITH MISSING STATUS FROM ALL DEPENDENCY OBJECTS
    valid_dependency_objects_with_ok_status = filter_missing_status_dependencies(all_dependency_objects,
                                                                                 all_missing_objects)

    remaining_objects = find_delta_of_missing_dependencies(all_unique_existing_objects,
                                                           valid_dependency_objects_with_ok_status)
    logging.info(f"These the still missing objects {remaining_objects}")
    return remaining_objects


def _write_dependencies_file(dependencies_data: list[dict]):
    """
    Writes dependency data to a CSV file, ensuring the correct headers are added.

    Args:
        dependencies_data (list[dict]): The dependency data to be written.
    """
    dependency_file = get_dependency_file_path()
    is_append = os.path.exists(dependency_file)  # Check if file exists

    write_csv_file(output_file=dependency_file, data_to_write=dependencies_data, is_append=is_append)


def append_package_dependencies():
    installable_package_list = get_completed_procedures_name_list()

    # Load the CSV file into a DataFrame
    dependencies_file = get_dependency_file_path()
    df = pd.read_csv(dependencies_file)

    # Filter out rows where STATUS is MISSING
    df = df[df['STATUS'] != 'MISSING']

    # Get unique OBJECT_PACKAGE values
    unique_packages = df['OBJECT_PACKAGE'].unique()

    # Create a list to store new rows
    new_rows = []

    # Iterate over each unique package
    for package in unique_packages:
        if pd.isna(package):  # Skip rows where OBJECT_PACKAGE is NaN
            continue

        # Filter rows that belong to the current package
        package_rows = df[df['OBJECT_PACKAGE'] == package]

        # Get unique OBJECT_NAME values for the current package
        unique_object_names = package_rows['OBJECT_NAME'].unique()

        # Create new rows for each unique OBJECT_NAME
        for object_name in unique_object_names:
            # Get the corresponding row to copy OBJECT_OWNER, OBJECT_TYPE, etc.
            original_row = package_rows[package_rows['OBJECT_NAME'] == object_name].iloc[0]

            status = ObjectTargetType.INSTALL.value if package in installable_package_list else ObjectTargetType.SKIP.value
            # Build the new row
            new_row = {
                'STATUS': status,
                'OBJECT_OWNER': original_row['OBJECT_OWNER'],
                'OBJECT_TYPE': 'PACKAGE',
                'OBJECT_PACKAGE': 'NONE',  # Set as NONE for the root
                'OBJECT_NAME': package,  # The package name
                'DEPENDENCY_OWNER': original_row['OBJECT_OWNER'],
                'DEPENDENCY_TYPE': original_row['OBJECT_TYPE'],
                'DEPENDENCY_PACKAGE': package,  # The original package
                'DEPENDENCY_NAME': object_name
            }
            new_rows.append(new_row)

    # Create a DataFrame from the new rows
    new_df = pd.DataFrame(new_rows)

    # Append the new rows to the original DataFrame
    updated_df = pd.concat([df, new_df], ignore_index=True)

    # Save the updated DataFrame to the same CSV file
    updated_df.to_csv(dependencies_file, index=False)

    logging.info("New rows added to 'dependencies.csv'.")


def fix_dependencies_file():
    # Step 1: Read the CSV file
    rows = get_dependencies_data()

    # Step 2: Create a dictionary for rows where OBJECT_PACKAGE is 'NONE'
    dependency_dict = {}
    for row in rows:
        if row['OBJECT_PACKAGE'] == 'NONE' and row['DEPENDENCY_NAME']:
            dependency_dict[row['DEPENDENCY_NAME']] = row['DEPENDENCY_TYPE']

    # Step 3: Filter rows based on the dictionary
    filtered_rows = []
    for row in rows:
        if row['DEPENDENCY_NAME'] in dependency_dict:
            if row['DEPENDENCY_TYPE'] == dependency_dict[row['DEPENDENCY_NAME']]:
                filtered_rows.append(row)
        else:
            filtered_rows.append(row)

    # Step 4: Write the filtered rows back to a new CSV file
    write_csv_file(output_file=get_dependency_file_path(), data_to_write=filtered_rows, is_append=False)


def find_all_dependencies_manager(db_pool: OracleDBConnectionPool, database_environment: DatabaseEnvironment):
    last_remaining_objects = []
    while True:
        # Step 1: find and write all current dependencies
        dependencies_data = _extract_missing_dependencies_from_source_files(db_pool=db_pool)
        _write_dependencies_file(dependencies_data=dependencies_data)

        # Step 2: find missing dependencies by drill down
        remaining_objects = _find_missing_dependencies()
        if last_remaining_objects:
            temp_objects = []
            for item in remaining_objects:
                if not any(
                        obj['NAME'] == item['NAME'] and
                        obj['OWNER'] == item['OWNER'] and
                        obj['PACKAGE'] == item['PACKAGE']
                        for obj in last_remaining_objects
                ):
                    temp_objects.append(item)
            remaining_objects = temp_objects

        if not remaining_objects:
            logging.info("No remaining objects. Exiting loop.")
            break

        logging.info(f"Remaining functions to process: {remaining_objects}")

        # Step 3: Update the complete procedures file
        update_missing_procedures_to_add_manager(objects=remaining_objects,
                                                 db_pool=db_pool)

        # Step 4: Find the source code for missing objects
        create_source_code_manager(db_pool=db_pool, database_environment=database_environment)

        # Step 5: Maybe the remaining object doesn't have dependencies
        last_remaining_objects = remaining_objects

    append_package_dependencies()
    ## add sources for package specifications
    fix_dependencies_file()


def resolve_dependency(owners: list, obj_name: str) -> dict:
    """
    Determines the owner, package, and object name from an object name string.

    Args:
        owners (list): A list of valid owner names.
        obj_name (str): The object name string in the format 'OWNER.OBJECT' or 'OBJECT'.

    Returns:
        dict: A dictionary containing:
            - 'owner': The owner name if found in owners, else None.
            - 'package': The package name if the prefix is not an owner, else None.
            - 'object_name': The actual object name.
    """
    parsed_name = split_table_name_into_package_and_table_name(obj_name)
    dependency_prefix, dependency_name = parsed_name["prefix"], parsed_name["name"]

    if dependency_prefix in owners:
        return {"owner": dependency_prefix, "package": "NONE", "name": dependency_name}
    return {"owner": None, "package": dependency_prefix or "NONE", "name": dependency_name}


def _is_dependency_object_exist(data: list[dict], object_owner: str, object_package: str, object_name: str) -> bool:
    """
    Checks if an object exists in the given list of dictionaries based on OBJECT_OWNER, OBJECT_PACKAGE, and OBJECT_NAME.
    If OBJECT_OWNER is empty ('') or None, it is ignored in the comparison.

    Args:
        data (list[dict]): The list of dictionaries representing the CSV data.
        object_owner (str): The OBJECT_OWNER value to search for (ignored if empty or None).
        object_package (str): The OBJECT_PACKAGE value to search for.
        object_name (str): The OBJECT_NAME value to search for.

    Returns:
        bool: True if the object exists, False otherwise.
    """
    if not data:
        return False

    return any(
        (not object_owner or not row.get("OBJECT_OWNER") or row["OBJECT_OWNER"] == object_owner) and
        row.get("OBJECT_PACKAGE") == object_package and
        row.get("OBJECT_NAME") == object_name
        for row in data
    )


def _extract_missing_dependencies_from_source_files(db_pool: OracleDBConnectionPool,
                                                    database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER9) -> \
        list[dict]:
    """
    Extract dependencies from all SQL files in a source folder.

    Args:
        source_folder (str): Path to the folder containing SQL files.

    Returns:
        list[dict]: A list of dictionaries, each representing a dependency.
        :param db_pool:
    """

    dependencies = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, get_source_code_folder(database_environment=database_environment))
    current_owners = get_all_current_owners(db_pool=db_pool)
    dependencies_data = get_dependencies_data()

    incomplete_procedures = get_incomplete_procedures()
    installable_packages_list = set()
    for incomplete_procedure in incomplete_procedures:
        installable_packages_list.add(incomplete_procedure.get("Package"))

    for filename in os.listdir(source_folder):
        logging.info("Reading this source code file: %s", filename)

        # Determine the object type (PROCEDURE/FUNCTION) and object name
        object_owner = filename.split('.')[0]
        object_package = filename.split('.')[1]
        object_name = filename.split('.')[2]  # Assuming the file name is the object name

        # the package is a ROOT element don't look for dependencies
        if object_package == 'NONE':
            continue

        if _is_dependency_object_exist(data=dependencies_data,
                                       object_owner=object_owner,
                                       object_package=object_package,
                                       object_name=object_name):
            continue

        ## OBJECT TARGET TYPE ANALYSIS
        object_status = (
            ObjectTargetType.INSTALL.value
            if any(item in installable_packages_list for item in (object_package, object_name))
            else ObjectTargetType.SKIP.value
        )

        if filename.endswith(".sql"):
            file_path = os.path.join(source_folder, filename)

            # Read the source code from the SQL file
            with open(file_path, mode='r', encoding='utf-8') as file:
                source_code_lines = file.readlines()

            if source_code_lines:

                first_row_of_source_code_lines = source_code_lines[0].strip().upper()
                object_type = find_object_type_from_first_source_code_line(first_row_of_source_code_lines)

                if object_type == "UNKNOWN":
                    continue

                # Extract dependencies
                dependencies_map = extract_all_dependencies_from_one_source_code_data(source_code_lines)
                dependencies_map_has_values = any(item for item in dependencies_map.values())

                if dependencies_map_has_values:
                    # Store dependencies in a list of dictionaries
                    for dep_type, dep_names in dependencies_map.items():
                        for dep_name in dep_names:
                            resolved_dependencies = resolve_dependency(owners=current_owners, obj_name=dep_name)

                            dependency_package = resolved_dependencies["package"]
                            dependency_name = resolved_dependencies["name"]

                            ## DEPENDENCY OBJECT TARGET TYPE ANALYSIS
                            if object_status == ObjectTargetType.INSTALL.value:
                                if dependency_package != 'NONE':
                                    object_status = (
                                        ObjectTargetType.INSTALL.value
                                        if dependency_package in installable_packages_list
                                        else ObjectTargetType.SKIP.value
                                    )

                            dependencies.append({
                                "STATUS": object_status,
                                "OBJECT_OWNER": object_owner,
                                "OBJECT_TYPE": object_type,
                                "OBJECT_PACKAGE": object_package,
                                "OBJECT_NAME": object_name,
                                "DEPENDENCY_OWNER": resolved_dependencies["owner"],
                                "DEPENDENCY_TYPE": dep_type,
                                "DEPENDENCY_PACKAGE": dependency_package,
                                "DEPENDENCY_NAME": dependency_name,
                            })
                else:
                    dependencies.append({
                        "STATUS": object_status,
                        "OBJECT_OWNER": object_owner,
                        "OBJECT_TYPE": object_type,
                        "OBJECT_PACKAGE": object_package,
                        "OBJECT_NAME": object_name,
                        "DEPENDENCY_OWNER": None,
                        "DEPENDENCY_TYPE": None,
                        "DEPENDENCY_PACKAGE": None,
                        "DEPENDENCY_NAME": None,
                    })
            else:
                logging.info(f"data not found for file: {filename}, adding as missing dependency")
                dependencies.append({
                    "STATUS": ObjectTargetType.MISSING.value,
                    "OBJECT_OWNER": object_owner,
                    "OBJECT_TYPE": None,
                    "OBJECT_PACKAGE": object_package,
                    "OBJECT_NAME": object_name,
                    "DEPENDENCY_OWNER": None,
                    "DEPENDENCY_TYPE": None,
                    "DEPENDENCY_PACKAGE": None,
                    "DEPENDENCY_NAME": None,
                })
        elif filename.endswith(".missing"):

            object_owner = filename.split('.')[0]
            object_package = filename.split('.')[1]
            object_name = filename.split('.')[2]  # Assuming the file name is the object name
            logging.info(f"skipping empty sql file: {filename}, adding as missing dependency ")
            dependencies.append({
                "STATUS": ObjectTargetType.MISSING.value,
                "OBJECT_OWNER": object_owner,
                "OBJECT_TYPE": None,
                "OBJECT_PACKAGE": object_package,
                "OBJECT_NAME": object_name,
                "DEPENDENCY_OWNER": None,
                "DEPENDENCY_TYPE": None,
                "DEPENDENCY_PACKAGE": None,
                "DEPENDENCY_NAME": None,
            })
    return dependencies


def find_object_type_from_first_source_code_line(first_row_of_source_code_lines: str):
    valid_object_types = ("PROCEDURE", "FUNCTION")
    object_type = "UNKNOWN"
    for obj_type in valid_object_types:
        if obj_type in first_row_of_source_code_lines:
            object_type = obj_type
            break
    return object_type


def get_dependency_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, DEPENDENCIES_FILE_PATH)
    return source_folder


def get_missing_dependencies_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, MISSING_DEPENDENCIES_FILE_PATH)
    return source_folder


def _get_object_data_file_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, _OBJECT_DATA_JSON)


def _get_object_data() -> dict:
    config_file = _get_object_data_file_path()
    return read_json_file(config_file)


def complete_dependency_file():
    dependency_file = get_dependency_file_path()
    object_data = _get_object_data()

    try:

        # Step 1: Read the dependencies.csv file
        dependencies_df = pd.read_csv(dependency_file)

        # Step 2: Identify rows where DEPENDENCY_OWNER is missing
        missing_owner_rows = dependencies_df[dependencies_df['DEPENDENCY_OWNER'].isna()]

        # Create a dictionary to map name to owner
        name_to_owner_map = {}
        for root in object_data['root']:
            for obj in root['objects']:
                name_to_owner_map[obj['name']] = obj['owner']

        # Step 4: Update the missing DEPENDENCY_OWNER values
        for index, row in missing_owner_rows.iterrows():
            dependency_name = row['DEPENDENCY_NAME']
            if dependency_name in name_to_owner_map:
                dependencies_df.at[index, 'DEPENDENCY_OWNER'] = name_to_owner_map[dependency_name]

        # Step 5: Save the updated dependencies.csv file
        dependencies_df.to_csv(dependency_file, index=False)
    except FileNotFoundError:
        logging.error("Dependencies.csv file not found, please execute CreateAllBaseDependencies process first")


if __name__ == "__main__":
    complete_dependency_file()
    fix_dependencies_file()
