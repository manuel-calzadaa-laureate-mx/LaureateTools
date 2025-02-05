import os

from tools.SqlScriptTools import find_create_table_script_data
from tools.SqlScriptTools import write_table_scripts

if __name__ == "__main__":
    output_dir = "../scripts"
    scripts_data = find_create_table_script_data()

    for filename, script in scripts_data.items():
        # Construct the file name
        file_path = os.path.join(output_dir, filename)

        # Write the script to the file
        with open(file_path, 'w') as file:
            file.write(script)

        print(f"Saved script for table '{filename}' to '{file_path}'")