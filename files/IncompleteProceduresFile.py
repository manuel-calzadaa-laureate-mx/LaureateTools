import os

from db.datasource.ProceduresDatasource import query_all_procedures_by_owner_and_package
from tools.FileTools import read_csv_file, write_csv_file

OUTPUT_CSV_FILE = "../workfiles/completed_procedures.csv"

INPUT_CSV_FILE = "../input/incomplete_procedures.csv"


def find_missing_procedures_from_input_file():
    """Read, process, and write missing procedures."""
    script_dir = os.path.dirname(os.path.abspath(__file__))

    input_csv_file_path = os.path.join(script_dir, INPUT_CSV_FILE)
    csv_rows = read_csv_file(input_csv_file_path)

    processed_data = _process_missing_procedures(csv_rows)

    output_csv_file_path = os.path.join(script_dir, OUTPUT_CSV_FILE)
    write_csv_file(output_csv_file_path, processed_data)


def _process_missing_procedures(rows):
    """Process the data, querying missing procedures where needed."""
    processed_data = [['Owner', 'Package', 'Procedure', 'Function']]  # Include header

    for row in rows:
        owner = row["Owner"].strip()
        package = row['Package'].strip() if row['Package'] else None
        procedure = row["Procedure"].strip() if row['Procedure'] else None

        if not procedure:
            procedures = query_all_procedures_by_owner_and_package(owner, package)
            for proc in procedures:
                processed_data.append([owner, package, proc, ""])
        else:
            processed_data.append([owner, package, procedure, ""])

    return processed_data

if __name__ == "__main__":
    find_missing_procedures_from_input_file()