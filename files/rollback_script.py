import logging
import os

from tools.file_tools import read_csv_file, write_csv_file

ROLLBACK_SCRIPT_FILE_PATH = "../workfiles/b9_install/rollback_script.csv"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_rollback_script_data() -> list[dict]:
    dependency_file_path = get_rollback_script_file_path()
    return read_csv_file(dependency_file_path)


def get_rollback_script_file_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, ROLLBACK_SCRIPT_FILE_PATH)
    return source_folder


def write_rollback_script_file(rollback_script_data: list[dict]):
    """
    Writes script data to a CSV file, ensuring the correct headers are added.

    Args:
        rollback_script_data (list[dict]): The script data to be written.
    """
    rollback_script_file = get_rollback_script_file_path()

    write_csv_file(output_file=rollback_script_file, data_to_write=rollback_script_data)
