from files.CompletedProceduresFile import extract_source_code_from_completed_procedures_file
from files.IncompleteProceduresFile import find_missing_procedures_from_input_file


if __name__ == "__main__":
    find_missing_procedures_from_input_file()
    extract_source_code_from_completed_procedures_file()
    ##find drill down dependencies