from db.OracleDatabaseTools import get_connection
from tools.ExtractTools import extract_source_code_from_procedures

if __name__ == "__main__":
    procedures_list = "../procedures.out"
    source_code_output = "src"

    # Extract source code of procedures
    extract_source_code_from_procedures(procedures_list, source_code_output)
