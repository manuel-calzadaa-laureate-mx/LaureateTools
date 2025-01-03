import re


def extract_select_tables(source_code):
    # Matches table names in SELECT statements
    select_pattern = re.compile(r"select.*?from\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in select_pattern.finditer(source_code)]


def extract_insert_tables(source_code):
    # Matches table names in INSERT statements
    insert_pattern = re.compile(r"insert\s+into\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in insert_pattern.finditer(source_code)]


def extract_update_tables(source_code):
    # Matches table names in UPDATE statements
    update_pattern = re.compile(r"update\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in update_pattern.finditer(source_code)]

def extract_delete_tables(source_code):
    # Matches table names in DELETE statements
    delete_pattern = re.compile(r"delete\s+from\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return [match.group(1) for match in delete_pattern.finditer(source_code)]

def extract_type_declarations(source_code):
    # Matches table names used in `%TYPE` declarations
    type_pattern = re.compile(r"([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+%type", re.IGNORECASE)
    return [match.group(1) for match in type_pattern.finditer(source_code)]

def extract_procedures(source_code):
    # Matches procedure names that have a prefix "P_"
    procedure_prefix = re.compile(r"([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+%type", re.IGNORECASE)
    return [match.group(1) for match in procedure_prefix.finditer(source_code)]

def exclude_insert_into_not_functions(source_code):
    insert_pattern = re.compile(r"\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return set(match.group(1) for match in insert_pattern.finditer(source_code))


def exclude_procedure_not_functions(source_code):
    procedure_pattern = re.compile(r"\bPROCEDURE\s+([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in procedure_pattern.finditer(source_code))

def exclude_cursor_not_functions(source_code):
    cursor_pattern = re.compile(r"\bCURSOR\s+([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in cursor_pattern.finditer(source_code))

def extract_generic_functions(source_code):
    function_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in function_pattern.finditer(source_code))


import re


def extract_local_functions(source_code):
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

def extract_sequences(source_code):
    """Extracts sequences used in the source code."""
    sequence_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\.NEXTVAL", re.IGNORECASE)
    return sequence_pattern.findall(source_code)

def extract_functions(source_code):
    """
    Extracts function names with assignments from the source code.
    Functions can be standalone or part of a package.

    Parameters:
        source_code (str): The source code to analyze.

    Returns:
        list: A list of function names (with optional package prefixes).
    """
    # Regex pattern to match function names with assignments
    function_pattern = re.compile(
        r":=\s*([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)?)\s*\([^)]*\);",
        re.IGNORECASE
    )

    # Find and return all matched function names
    return [match.group(1) for match in function_pattern.finditer(source_code)]