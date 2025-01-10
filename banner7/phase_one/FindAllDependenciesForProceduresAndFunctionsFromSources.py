from logging import Logger

import logging

from FindAllObjectsSourceCode import extract_source_code_from_procedures
from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import get_connection
from tools.DependencyFileTools import extract_unique_existing_functions, extract_unique_dependency_function, \
    extract_unique_existing_objects, extract_unique_dependency_objects
from tools.ExtractTools import extract_all_dependencies_from_source_file_folder
from tools.ProcedureFileTools import update_procedures_file, normalize_object_names

logging.basicConfig(level=logging.INFO)


def main():
    output_csv = "../dependencies.out"
    source_code_output = "src"
    procedures_out_file = "../procedures.out"
    config_file = "../../db_config.json"
    database_name = DatabaseEnvironment.BANNER7

    while True:
        # Step 1: Execute FindAllDependenciesForProceduresAndFunctions functionality
        extract_all_dependencies_from_source_file_folder(source_code_output, output_csv)

        # Extract and normalize objects
        existing_objects = extract_unique_existing_objects(output_csv)
        print(existing_objects)
        objects = extract_unique_dependency_objects(output_csv)
        print(objects)
        normalized_objects = normalize_object_names(existing_objects)
        print(normalized_objects)

        # Find the remaining functions
        remaining_objects = [func for func in objects if func.split('.')[-1] not in normalized_objects]
        print(remaining_objects)

        if not remaining_objects:
            print("No remaining objects. Exiting loop.")
            break

        print(f"Remaining functions to process: {remaining_objects}")

        # Step 2: Update the procedures file
        db_connection = get_connection(config_file=config_file, database_name=database_name)
        update_procedures_file(remaining_objects, db_connection, procedures_out_file)

        # Step 3: Execute FindAllObjectsSourceCode functionality
        extract_source_code_from_procedures(db_connection, procedures_out_file, source_code_output)

        db_connection.close()


if __name__ == "__main__":
    main()
