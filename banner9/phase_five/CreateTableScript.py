import os

from tools.SqlScriptTools import build_create_table_script_data


def save_scripts_to_files(scripts_data: dict, output_directory: str):
    """
    Save SQL scripts to files with well-named filenames.

    Args:
        scripts_data (dict): A dictionary where keys are table names and values are SQL scripts.
        output_directory (str): The directory where the files will be saved.

    Raises:
        ValueError: If the output directory does not exist or is not writable.
    """
    if not os.path.exists(output_directory):
        raise ValueError(f"The output directory '{output_directory}' does not exist.")
    if not os.access(output_directory, os.W_OK):
        raise ValueError(f"The output directory '{output_directory}' is not writable.")

    for filename, script in scripts_data.items():
        # Construct the file name
        file_path = os.path.join(output_directory, filename)

        # Write the script to the file
        with open(file_path, 'w') as file:
            file.write(script)

        print(f"Saved script for table '{table_name}' to '{file_path}'")


if __name__ == "__main__":
    object_data = "../../object_data.json"
    output_dir = "../scripts"
    table_name = "TZTBRCIBK"
    scripts_data = build_create_table_script_data(object_data_file=object_data, table_names=[table_name])
    save_scripts_to_files(scripts_data=scripts_data, output_directory=output_dir)

