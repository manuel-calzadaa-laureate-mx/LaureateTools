import logging
import re

from db.OracleDatabaseTools import is_oracle_built_in_object


def extract_select_tables(source_code: str) -> set[str]:
    # Matches table names in SELECT statements
    select_pattern = re.compile(r"select.*?from\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in select_pattern.finditer(source_code)]


def extract_insert_tables(source_code: str) -> list[str]:
    # Matches table names in INSERT statements
    insert_pattern = re.compile(r"insert\s+into\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in insert_pattern.finditer(source_code)]


def extract_update_tables(source_code: str) -> list[str]:
    # Matches table names in UPDATE statements
    update_pattern = re.compile(r"update\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in update_pattern.finditer(source_code)]


def extract_delete_tables(source_code: str) -> list[str]:
    # Matches table names in DELETE statements
    delete_pattern = re.compile(r"delete\s+from\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in delete_pattern.finditer(source_code)]


def extract_type_declarations(source_code: str) -> list[str]:
    # Matches table names used in `%TYPE` declarations
    type_pattern = re.compile(r"([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+%type", re.IGNORECASE)
    return [match.group(1) for match in type_pattern.finditer(source_code)]


def extract_procedures(source_code: str) -> list[str]:
    """
       Extract valid procedure names from Oracle source code with exclusions.

       Args:
           source_code (str): Oracle source code.

       Returns:
           list: A list of valid procedure names.
       """

    potential_matches = match_general_pattern(source_code=source_code)

    valid_procedures = set()

    for match in potential_matches:
        starting_word, optional_package_name, object_name = match

        # Step 1: Exclude invalid packages
        if len(optional_package_name.strip()) == 1:
            logging.info(f"Excluding due to invalid PACKAGE length: {optional_package_name}")
            continue

        if not is_valid_package_name(optional_package_name):
            logging.info(f"Excluding due to invalid PACKAGE name: {optional_package_name}")
            continue

        # Combine optional_package_name and object_name into package_name.procedure_name if optional_package_name exists
        full_name = f"{optional_package_name.strip()}.{object_name.upper()}" if optional_package_name else object_name

        # Step 2: Exclusion for WORD1 (Reject PROCEDURE, FUNCTION, CURSOR, :=, etc.)
        if starting_word.upper() in {"PROCEDURE", "FUNCTION", "CURSOR", ":=", "INTO"}:
            logging.info(f"Excluding due to WORD: {starting_word}")
            continue

        # Step 3: Exclusion for names starting with "F_" (indicating a function)
        valid_function_prefix = ["F_", "FN_"]
        if object_name.upper().startswith(tuple(valid_function_prefix)):
            logging.info(f"Excluding due to 'F_' prefix: {full_name}")
            continue

        # Step 4: Validation for names starting with "P_"
        valid_procedure_prefix = ["P_", "PR_"]
        if object_name.upper().startswith(tuple(valid_procedure_prefix)):
            valid_procedures.add(full_name)
            logging.info(f"Immediately adding due to starting with 'P_': {full_name}")
            continue

        # Step 5: Exclusion for Oracle built-in functions
        if is_oracle_built_in_object(object_name):
            logging.info(f"Excluding Oracle built-in object: {full_name}")
            continue

        # Step 6: Add valid procedure name
        valid_procedures.add(full_name.upper())
        logging.info(f"Valid procedure found: {full_name.upper()}")

    return valid_procedures


def extract_insert_into(source_code: str) -> set[str]:
    insert_pattern = re.compile(r"\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return set(match.group(1) for match in insert_pattern.finditer(source_code))


def extract_procedures_names_at_first_line(source_code: str) -> set[str]:
    procedure_pattern = re.compile(r"\bPROCEDURE\s+([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in procedure_pattern.finditer(source_code))


def extract_cursors(source_code: str) -> set[str]:
    cursor_pattern = re.compile(r"\bCURSOR\s+([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in cursor_pattern.finditer(source_code))


def extract_generic_functions(source_code: str) -> set[str]:
    function_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in function_pattern.finditer(source_code))


def extract_local_functions(source_code: str) -> set[str]:
    """
    Extracts local function names from the source code, excluding standalone function declarations.

    Parameters:
        source_code (str): The source code to analyze.

    Returns:
        set: A set of local function names.
    """
    # Regex to match all FUNCTION keywords with potential names
    local_function_pattern = re.compile(r"\bFUNCTION\s+([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)

    local_functions = set()

    for match in local_function_pattern.finditer(source_code):
        start_index = match.start()  # Get the start index of the match
        preceding_text = source_code[max(0, start_index - 20):start_index]  # Check up to 20 chars behind

        # Determine if 'FUNCTION' is a local function (not at the start of a standalone function)
        if not preceding_text.strip():  # No significant preceding text (likely standalone function)
            continue

        local_functions.add(match.group(1))  # Add the function name to the set

    return local_functions


def extract_sequences(source_code: str) -> list[str]:
    """Extracts sequences used in the source code."""
    sequence_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\.NEXTVAL", re.IGNORECASE)
    return sequence_pattern.findall(source_code)


def is_valid_package_name(package_name):
    if len(package_name) <= 1:
        return False
    # Regex to match only alphanumeric characters and underscores
    pattern = r'^[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, package_name))


def extract_functions(source_code: str) -> set[str]:
    """
           Extract valid procedure names from Oracle source code with exclusions.

           Args:
               source_code (str): Oracle source code.

           Returns:
               list: A list of valid procedure names.
           """
    potential_matches = match_general_pattern(source_code)

    valid_functions = set()

    for match in potential_matches:
        starting_word, optional_package_name, object_name = match

        # Step 1: Exclude invalid packages
        if len(optional_package_name.strip()) == 1:
            logging.info(f"Excluding due to invalid PACKAGE: {optional_package_name}")
            continue

        # Exclude packages that have symbols
        if not is_valid_package_name(optional_package_name):
            logging.info(f"Excluding due to invalid PACKAGE: {optional_package_name}")
            continue

        # Combine optional_package_name and object_name into package_name.procedure_name if optional_package_name exists
        full_name = f"{optional_package_name.strip().upper()}.{object_name.upper()}" if optional_package_name else object_name.upper()

        # Step 2: Exclusion for WORD1 (Reject PROCEDURE, FUNCTION, CURSOR, etc.)
        if starting_word.upper() in {"PROCEDURE", "FUNCTION", "CURSOR", "INTO", "AND"}:
            logging.info(f"Excluding due to WORD: {starting_word}")
            continue

        # Step 3: Exclusion for names starting with "P_" (indicating a procedure)
        valid_procedure_prefix = ["P_", "PR_"]
        if object_name.upper().startswith(tuple(valid_procedure_prefix)):
            logging.info(f"Excluding due to 'P_' prefix: {full_name}")
            continue

        # Step 4: Validation for names starting with "F_"
        valid_function_prefix = ["F_", "FN_"]
        if object_name.upper().startswith(tuple(valid_function_prefix)):
            valid_functions.add(full_name)
            logging.info(f"Immediately adding due to starting with 'F_': {full_name}")
            continue

        # Step 5: Exclusion for Oracle built-in functions
        if is_oracle_built_in_object(object_name):
            logging.info(f"Excluding Oracle built-in object: {full_name}")
            continue

        # Step 6: Add valid procedure name
        valid_functions.add(full_name.upper())
        logging.info(f"Valid function found: {full_name.upper()}")

    return valid_functions


def match_general_pattern(source_code: str):
    pattern = r"""
               \b(\w+\s+)?               # WORD (first word, can be rejected later)
               ([\w$]+)                  # PACKAGE AZaz09_$
               \.                        # dot
               (\w+)\b                   # OBJECT AZaz09_
               \s*\(                     # Opening parenthesis
               [^;]*?\)                  # Parameters (anything up to the closing parenthesis)
               \s*;                      # Semicolon
           """
    potential_matches = re.findall(pattern, source_code, re.IGNORECASE | re.VERBOSE)
    return potential_matches


if __name__ == "__main__":
    source_code = """
        CREATE OR REPLACE PROCEDURE some_proc IS BEGIN NULL; END;
        := P_MY_PROCEDURE(a, b, c);
        P_OTHER_PROCEDURE(x, y, z);
        PROCEDURE P_IGNORE_ME(a, b, c);
        P_NOT_A_BUILT_IN(a, b := 5);
        GOOD_PROCEDURE(a,b);
        INSERT into NOT_A_PROCEDURE(BLA, BLA, BLA) VALUES (BLA, BLA, BLA);
    """
    procedures = extract_procedures(source_code)
    print(procedures)
