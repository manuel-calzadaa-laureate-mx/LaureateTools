from enum import Enum

from files.b9_sql_script_file import find_install_script_file_name
from files.install_dependency_ordered_file import get_install_dependencies_ordered_data
from files.install_script import write_install_script_file
from files.rollback_script import write_rollback_script_file


class ObjectAction(Enum):
    CREATE = "create"
    DELETE = "delete"


def create_install_script_manager():
    install_dependencies_ordered = get_install_dependencies_ordered_data()
    install_script_elements = []
    index = 1
    for install_dependency in install_dependencies_ordered[::-1]:
        object_type = install_dependency.get("object_type")
        object_name = install_dependency.get("object_name")
        script_file_data = find_install_script_file_name(object_action=ObjectAction.CREATE.value,
                                                         object_type=object_type, object_name=object_name)
        if script_file_data:
            filename = script_file_data.get("filename")

            base_name = filename[:-4]

            # Split the remaining parts by '.'
            parts = base_name.split('.')

            name = parts[0]
            owner = parts[1]
            postfix = '.'.join(parts[2:])

            install_script_element = {
                "object_filename": filename,
                "object_type": object_type,
                "object_owner": owner,
                "index": index,
            }
            index += 1
            install_script_elements.append(install_script_element)
    write_install_script_file(install_script_elements)


def create_rollback_script_manager():
    install_dependencies_ordered = get_install_dependencies_ordered_data()
    install_script_elements = []
    index = 1
    for install_dependency in install_dependencies_ordered:
        object_type = install_dependency.get("object_type")
        object_name = install_dependency.get("object_name")
        script_file_data = find_install_script_file_name(object_action=ObjectAction.DELETE.value,
                                                         object_type=object_type,
                                                         object_name=object_name)
        if script_file_data:
            filename = script_file_data.get("filename")

            base_name = filename[:-4]

            # Split the remaining parts by '.'
            parts = base_name.split('.')

            name = parts[0]
            owner = parts[1]
            postfix = '.'.join(parts[2:])

            install_script_element = {
                "object_filename": filename,
                "object_type": object_type,
                "object_owner": owner,
                "index": index,
            }
            index += 1
            install_script_elements.append(install_script_element)
    write_rollback_script_file(install_script_elements)


def create_setup_script_manager():
    return None
