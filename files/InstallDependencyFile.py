import logging
import os

from tools.FileTools import read_csv_file, write_csv_file

INSTALL_DEPENDENCIES_FILE_PATH = "../workfiles/b9_install/install_dependencies.csv"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_install_dependency_data(migrated_object_data: list[dict]):
    migrated_dependencies_list = []

    for row in migrated_object_data:
        object_name = row.get("name")
        object_type = row.get("type")
        object_package = row.get("package")
        object_owner = row.get("owner")
        all_dependencies = row.get("dependencies")

        # Handle empty or invalid dependencies
        if not all_dependencies or not any(all_dependencies.values()):
            migrated_dependency = {
                "OBJECT_PACKAGE": object_package if object_package else 'NONE',
                "OBJECT_TYPE": object_type,
                "OBJECT_NAME": object_name,
                "OBJECT_OWNER": object_owner,
                "DEPENDENCY_PACKAGE": 'NONE',
                "DEPENDENCY_TYPE": 'NONE',
                "DEPENDENCY_NAME": 'NONE',
                "DEPENDENCY_OWNER": 'NONE',
            }
            migrated_dependencies_list.append(migrated_dependency)
            continue

        for dependency_key, dependency_values in all_dependencies.items():
            for dependency_value in dependency_values:
                if dependency_value:
                    dependency_package = dependency_value.get("package")
                    dependency_type = dependency_value.get("type")
                    dependency_name = dependency_value.get("name")
                    dependency_owner = dependency_value.get("owner")

                    migrated_dependency = {
                        "OBJECT_PACKAGE": object_package if object_package else 'NONE',
                        "OBJECT_TYPE": object_type,
                        "OBJECT_NAME": object_name,
                        "OBJECT_OWNER": object_owner,
                        "DEPENDENCY_PACKAGE": dependency_package,
                        "DEPENDENCY_TYPE": dependency_type,
                        "DEPENDENCY_NAME": dependency_name,
                        "DEPENDENCY_OWNER": dependency_owner,
                    }
                    migrated_dependencies_list.append(migrated_dependency)

    return migrated_dependencies_list


def get_install_dependencies_data() -> list[dict]:
    dependency_file_path = get_install_dependency_file_path()
    return read_csv_file(dependency_file_path)


def get_install_dependency_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, INSTALL_DEPENDENCIES_FILE_PATH)
    return source_folder


def write_install_dependencies_file(install_dependencies_data: list[dict]):
    """
    Writes dependency data to a CSV file, ensuring the correct headers are added.

    Args:
        install_dependencies_data (list[dict]): The dependency data to be written.
    """
    install_dependency_file = get_install_dependency_file_path()
    is_append = os.path.exists(install_dependency_file)  # Check if file exists

    write_csv_file(output_file=install_dependency_file, data_to_write=install_dependencies_data, is_append=is_append)
