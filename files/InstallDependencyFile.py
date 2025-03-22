import logging
import os

from tools.FileTools import read_csv_file, write_csv_file

INSTALL_DEPENDENCIES_FILE_PATH = "../workfiles/b9_install/install_dependencies.csv"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


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
