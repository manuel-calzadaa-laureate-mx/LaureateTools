import json
import os

from tools.FileTools import write_csv_file


def create_folders_and_subfolders(base_folder: str, subfolders: [str] = None)-> None:
    # Crear la carpeta base si no existe
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
        print(f"Carpeta creada: {base_folder}")
    if subfolders:
        for folder in subfolders:
            folder_path = os.path.join(base_folder, folder)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Carpeta creada: {folder_path}")
            else:
                print(f"La carpeta ya existe: {folder_path}")

def create_workfiles_folder():
    base_folder = "workfiles"
    subfolders = ["b7_sources", "b9_scripts", "b9_sources","b7_output"]
    create_folders_and_subfolders(base_folder=base_folder, subfolders=subfolders)


def create_input_folder():
    base_folder = "input"
    create_folders_and_subfolders(base_folder=base_folder)
    header = [["Owner","Package","Procedure"]]
    incomplete_procedure = "input/b7_incomplete_procedure.csv"
    write_csv_file(output_file=incomplete_procedure,data_to_write=header, is_append=False)


def create_config_json():
    config_data = [
        {
            "databaseName": "banner7",
            "config": {
                "username": "***",
                "password": "***",
                "host": "***",
                "port": "***",
                "service_name": "***",
                "environment": "dev",
                "schema": "***"
            }
        },
        {
            "databaseName": "banner9",
            "config": {
                "username": "***",
                "password": "***",
                "host": "***",
                "port": "***",
                "service_name": "***",
                "environment": "dev",
                "schema": "***"
            }
        }
    ]

    config_path = os.path.join("config", "db_config.json")
    with open(config_path, "w") as json_file:
        json.dump(config_data, json_file, indent=2)
    print(f"Archivo JSON creado: {config_path}")

if __name__ == "__main__":
    create_workfiles_folder()
    create_input_folder()
    create_config_json()