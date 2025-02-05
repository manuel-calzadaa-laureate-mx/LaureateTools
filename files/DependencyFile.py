import logging
import os

from db.DatabaseProperties import DatabaseEnvironment
from files.CompletedProceduresFile import update_missing_procedures_to_add_manager, create_source_code_manager
from files.SourceCodeFile import extract_all_dependencies_from_one_source_code_data, get_source_code_folder
from tools.CommonTools import get_all_current_owners, parse_object_name
from tools.FileTools import write_csv_file, read_csv_file

DEPENDENCIES_FILE_PATH = "../workfiles/dependencies.csv"
MISSING_DEPENDENCIES_FILE_PATH = "../workfiles/missing_dependencies.csv"

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
            if (_is_object_dependency_procedure_or_function(object=row,
                                                            object_name="DEPENDENCY_TYPE")
                    and _is_object_need_process(object=row, object_name="DEPENDENCY_NAME",
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


def _is_ok_object(object: dict, object_name: str = "STATUS") -> bool:
    return object[object_name] == "OK"


def _is_object_procedure_or_function_or_none(object: dict, object_name: str = "OBJECT_TYPE"):
    return (object[object_name].strip() == 'FUNCTION'
            or object[object_name].strip() == 'PROCEDURE' or (
                    object[object_name].strip() == '' and object["STATUS"] == "MISSING"))


def _is_object_dependency_procedure_or_function(object: dict, object_name: str = "DEPENDENCY_TYPE"):
    return (object[object_name].strip() == 'FUNCTION'
            or object[object_name].strip() == 'PROCEDURE')


def _is_object_need_process(object: dict, object_name: str, unique_names: list[dict]):
    return object[object_name].strip().upper() not in unique_names


def _extract_object_with_missing_status(dependencies_data: list[dict]) -> list[dict]:
    object_with_missing_status = []
    try:
        for row in dependencies_data:
            if not _is_ok_object(row):
                object_with_missing_status.append({
                    "STATUS": row["STATUS"],
                    "OBJECT_OWNER": row['OBJECT_OWNER'],
                    "OBJECT_PACKAGE": row['OBJECT_PACKAGE'],
                    "OBJECT_NAME": row['OBJECT_NAME'].strip().upper()}
                )
        return object_with_missing_status
    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []


def _extract_unique_existing_objects(dependency_data: list[dict]) -> list[dict]:
    """
    Reads a file with dependency information and extracts unique existing function names.

    Parameters:
        file_path (str): The path to the dependencies file.

    Returns:
        list: A sorted list of unique existing function names.
    """
    unique_existing_objects = []
    unique_object_names = []

    try:
        for row in dependency_data:
            if (_is_object_procedure_or_function_or_none(object=row, object_name="OBJECT_TYPE")
                    and _is_object_need_process(object=row, object_name="OBJECT_NAME",
                                                unique_names=unique_object_names)):
                unique_object_names.append(row['OBJECT_NAME'].strip().upper())
                unique_existing_objects.append(
                    {"STATUS": row['STATUS'],
                     "OBJECT_OWNER": row['OBJECT_OWNER'],
                     "OBJECT_PACKAGE": row['OBJECT_PACKAGE'],
                     "OBJECT_NAME": row['OBJECT_NAME'].strip().upper()})
        return unique_existing_objects

    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []


def _find_delta_of_missing_dependencies(all_unique_objects: list[dict],
                                        unique_dependency_objects: list[dict]) -> list[dict]:
    """
    Finds dependency objects that are not present in the all_objects list.

    Args:
        all_unique_objects (list[dict]): A list of dictionaries representing existing objects.
        unique_dependency_objects (list[dict]): A list of dependency objects to check.

    Returns:
        list[dict]: A list of missing dependency objects.
    """
    missing_dependencies = []

    for dep in unique_dependency_objects:
        found = False  # Track if this dependency is found

        for obj in all_unique_objects:
            # Skip if the object name matches but its status is "MISSING"
            if obj["OBJECT_NAME"] == dep["DEPENDENCY_NAME"] and obj["STATUS"] == "MISSING":
                found = True
                break

            # Check if the object fully matches an existing object
            if (obj["OBJECT_NAME"] == dep["DEPENDENCY_NAME"]
                    # and obj["OBJECT_OWNER"] == dep["DEPENDENCY_OWNER"]
                    and obj["OBJECT_PACKAGE"] == dep["DEPENDENCY_PACKAGE"]):
                found = True
                break  # No need to check further, as it already exists

        # If no match was found, add to missing dependencies
        if not found:
            missing_dependencies.append({"OWNER": dep["DEPENDENCY_OWNER"],
                                         "PACKAGE": dep["DEPENDENCY_PACKAGE"],
                                         "NAME": dep["DEPENDENCY_NAME"]})

    return missing_dependencies


def _filter_missing_status_dependencies(all_dependency_objects, all_missing_objects):
    # Normalize all_missing_objects and convert to a set for quick lookup
    missing_set = {
        (obj['OBJECT_NAME'], obj['OBJECT_OWNER'], "" if obj['OBJECT_PACKAGE'] == "NONE" else obj['OBJECT_PACKAGE'])
        for obj in all_missing_objects
    }

    # Filter out dependencies that exist in all_missing_objects
    filtered_dependencies = [
        obj for obj in all_dependency_objects
        if (obj['DEPENDENCY_NAME'], obj['DEPENDENCY_OWNER'], obj['DEPENDENCY_PACKAGE']) not in missing_set
    ]

    return filtered_dependencies


def _find_missing_dependencies() -> list:
    logging.info(f"Starting: find missing dependencies")

    dependencies_data = read_csv_file(get_dependency_file_path())
    # ALL OBJECTS IN DEPENDENCY DATA
    all_unique_existing_objects = _extract_unique_existing_objects(dependencies_data)

    # ALL DEPENDENCY OBJECTS IN DEPENDENCY DATA
    all_dependency_objects = _extract_unique_dependency_objects(dependencies_data)

    # OBJECTS WITH MISSING STATUS IN DEPENDENCY DATA
    all_missing_objects = _extract_object_with_missing_status(dependencies_data)

    # REMOVE OBJECTS WITH MISSING STATUS FROM ALL DEPENDENCY OBJECTS
    valid_dependency_objects_with_ok_status = _filter_missing_status_dependencies(all_dependency_objects,
                                                                                  all_missing_objects)

    remaining_objects = _find_delta_of_missing_dependencies(all_unique_existing_objects,
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


def find_all_dependencies_manager(database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7):
    while True:
        # Step 1: find and write all current dependencies
        dependencies_data = _extract_missing_dependencies_from_source_files()
        _write_dependencies_file(dependencies_data=dependencies_data)

        # Step 2: find missing dependencies by drill down
        remaining_objects = _find_missing_dependencies()
        if not remaining_objects:
            logging.info("No remaining objects. Exiting loop.")
            break

        logging.info(f"Remaining functions to process: {remaining_objects}")

        # Step 3: Update the complete procedures file
        update_missing_procedures_to_add_manager(objects=remaining_objects,
                                                 database_environment=DatabaseEnvironment.BANNER7)

        # Step 4: Find the source code for missing objects
        create_source_code_manager()

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
    parsed_name = parse_object_name(obj_name)
    dependency_prefix, dependency_name = parsed_name["prefix"], parsed_name["name"]

    if dependency_prefix in owners:
        return {"owner": dependency_prefix, "package": None, "name": dependency_name}
    return {"owner": None, "package": dependency_prefix or None, "name": dependency_name}


def _is_dependency_object_exist(data: list[dict], object_owner: str, object_package: str, object_name: str) -> bool:
    """
    Checks if an object exists in the given list of dictionaries based on OBJECT_OWNER, OBJECT_PACKAGE, and OBJECT_NAME.

    Args:
        data (list[dict]): The list of dictionaries representing the CSV data.
        object_owner (str): The OBJECT_OWNER value to search for.
        object_package (str): The OBJECT_PACKAGE value to search for.
        object_name (str): The OBJECT_NAME value to search for.

    Returns:
        bool: True if the object exists, False otherwise.
    """
    if data:
        return any(
            row["OBJECT_OWNER"] == object_owner and
            row["OBJECT_PACKAGE"] == object_package and
            row["OBJECT_NAME"] == object_name
            for row in data
        )
    else:
        return False


def _extract_missing_dependencies_from_source_files() -> list[dict]:
    """
    Extract dependencies from all SQL files in a source folder.

    Args:
        source_folder (str): Path to the folder containing SQL files.

    Returns:
        list[dict]: A list of dictionaries, each representing a dependency.
    """

    dependencies = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, get_source_code_folder())
    current_owners = get_all_current_owners()
    dependencies_data = get_dependencies_data()

    for filename in os.listdir(source_folder):
        logging.info("Reading this source code file: %s", filename)

        if filename.endswith(".sql"):
            file_path = os.path.join(source_folder, filename)

            # Determine the object type (PROCEDURE/FUNCTION) and object name
            object_owner = filename.split('.')[0]
            object_package = filename.split('.')[1]
            object_name = filename.split('.')[2]  # Assuming the file name is the object name
            if _is_dependency_object_exist(data=dependencies_data,
                                           object_owner=object_owner,
                                           object_package=object_package,
                                           object_name=object_name):
                continue

            # Read the source code from the SQL file
            with open(file_path, mode='r', encoding='utf-8') as file:
                source_code_lines = file.readlines()

            if source_code_lines:

                first_row_of_source_code_lines = source_code_lines[0].strip().upper()
                object_type = "PROCEDURE" if "PROCEDURE" in first_row_of_source_code_lines else "FUNCTION"

                # Extract dependencies
                dependencies_map = extract_all_dependencies_from_one_source_code_data(source_code_lines)

                # Store dependencies in a list of dictionaries
                for dep_type, dep_names in dependencies_map.items():
                    for dep_name in dep_names:
                        resolved_dependencies = resolve_dependency(owners=current_owners, obj_name=dep_name)

                        dependencies.append({
                            "STATUS": "OK",
                            "OBJECT_OWNER": object_owner,
                            "OBJECT_TYPE": object_type,
                            "OBJECT_PACKAGE": object_package,
                            "OBJECT_NAME": object_name,
                            "DEPENDENCY_OWNER": resolved_dependencies["owner"],
                            "DEPENDENCY_TYPE": dep_type,
                            "DEPENDENCY_PACKAGE": resolved_dependencies["package"],
                            "DEPENDENCY_NAME": resolved_dependencies["name"],
                        })
            else:
                logging.info(f"data not found for file: {filename}, adding as missing dependency")
                dependencies.append({
                    "STATUS": "MISSING",
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
                "STATUS": "MISSING",
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


def get_dependency_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, DEPENDENCIES_FILE_PATH)
    return source_folder


def get_missing_dependencies_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, MISSING_DEPENDENCIES_FILE_PATH)
    return source_folder


if __name__ == "__main__":
    find_all_dependencies_manager()
