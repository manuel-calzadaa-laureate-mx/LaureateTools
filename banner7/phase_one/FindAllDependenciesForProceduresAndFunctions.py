from FindAllObjectsSourceCode import extract_source_code_from_procedures
from database.DatabaseProperties import DatabaseEnvironment
from database.OracleDatabaseTools import get_connection
from tools.DependencyFileTools import extract_unique_existing_functions, extract_unique_dependency_function
from tools.ExtractTools import read_source_code_folder_and_find_all_dependencies
from tools.ProcedureFileTools import normalize_function_names, update_procedures_file


def main():
    output_csv = "../dependencies.out"
    source_code_output = "src"
    procedures_out_file = "../procedures.out"
    config_file = "../../db_config.json"
    database_name = DatabaseEnvironment.BANNER7

    while True:
        # Step 1: Execute FindAllDependenciesForProceduresAndFunctions functionality
        read_source_code_folder_and_find_all_dependencies(source_code_output, output_csv)

        db_connection = get_connection(config_file=config_file, database_name=database_name)

        # Extract and normalize functions
        existing_functions = extract_unique_existing_functions(output_csv)
        functions = extract_unique_dependency_function(output_csv)
        normalized_existing_functions = normalize_function_names(existing_functions)

        # Find the remaining functions
        remaining_functions = [func for func in functions if func.split('.')[-1] not in normalized_existing_functions]

        if not remaining_functions:
            print("No remaining functions. Exiting loop.")
            db_connection.close()
            break

        print(f"Remaining functions to process: {remaining_functions}")

        # Step 2: Update the procedures file
        update_procedures_file(remaining_functions, db_connection, procedures_out_file)

        # Step 3: Execute FindAllObjectsSourceCode functionality
        extract_source_code_from_procedures(db_connection, procedures_out_file, source_code_output)

        db_connection.close()


if __name__ == "__main__":
    main()
