import cx_Oracle

from database.FunctionsDatasource import get_packaged_function_owner, get_independent_function_owners


def update_procedures_file(functions: list, db_connection: cx_Oracle.Connection, output_file: str):
    """Update the procedures file based on whether the function is packaged or independent."""
    with open(output_file, 'a', newline='') as file:
        writer = csv.writer(file)

        for func in functions:
            if is_packaged_function(func):
                owner, package, procedure = get_packaged_function_owner(func, db_connection)
                writer.writerow([owner, package, procedure, ''])
            else:
                owners = get_independent_function_owners(func, db_connection)
                for owner, _ in owners:
                    writer.writerow([owner, '', '', func])

def is_packaged_function(function: str) -> bool:
    """Check if the function follows the PACKAGE.FUNCTION_NAME pattern."""
    return '.' in function

def normalize_function_names(functions: list) -> set:
    """
    Normalize function names to ensure consistency.
    Extracts only the function name from PACKAGE.FUNCTION patterns.
    """
    return {func.split('.')[-1] for func in functions}  # Take the last part after the dot