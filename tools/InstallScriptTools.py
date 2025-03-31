from files.B9SqlScriptFile import find_script_file_name
from files.InstallDependencyOrdered import get_install_dependencies_ordered_data
from files.InstallScript import write_install_script_file


def create_install_script_manager():
    install_dependencies_ordered = get_install_dependencies_ordered_data()
    install_script_elements = []
    index = 1
    for install_dependency in install_dependencies_ordered[::-1]:
        object_type = install_dependency.get("object_type")
        object_name = install_dependency.get("object_name")
        script_file_data = find_script_file_name(object_type=object_type, object_name=object_name)
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
