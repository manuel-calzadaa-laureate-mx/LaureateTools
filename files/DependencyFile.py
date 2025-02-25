import logging


class DependencyFile:
    def __init__(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )



def is_ok_object(object: dict, object_name: str = "STATUS") -> bool:
    return object[object_name] == "OK"


def is_object_procedure_or_function_or_none(object: dict, object_name: str = "OBJECT_TYPE"):
    return (object[object_name].strip() == 'FUNCTION'
            or object[object_name].strip() == 'PROCEDURE' or (
                    object[object_name].strip() == '' and object["STATUS"] == "MISSING"))


def is_object_dependency_procedure_or_function(object: dict, object_name: str = "DEPENDENCY_TYPE"):
    return (object[object_name].strip() == 'FUNCTION'
            or object[object_name].strip() == 'PROCEDURE')


def is_object_need_process(object: dict, object_name: str, unique_names: list[dict]):
    return object[object_name].strip().upper() not in unique_names


def extract_object_with_missing_status(dependencies_data: list[dict]) -> list[dict]:
    object_with_missing_status = []
    try:
        for row in dependencies_data:
            if not is_ok_object(row):
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


def extract_unique_existing_objects(dependency_data: list[dict]) -> list[dict]:
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
            if (is_object_procedure_or_function_or_none(object=row, object_name="OBJECT_TYPE")
                    and is_object_need_process(object=row, object_name="OBJECT_NAME",
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


def find_delta_of_missing_dependencies(all_unique_objects: list[dict],
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


def filter_missing_status_dependencies(all_dependency_objects, all_missing_objects):
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