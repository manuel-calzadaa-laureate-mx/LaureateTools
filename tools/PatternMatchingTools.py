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


def exclude_insert_into_not_functions(source_code):
    insert_pattern = re.compile(r"\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
    return set(match.group(1) for match in insert_pattern.finditer(source_code))


def exclude_procedure_not_functions(source_code):
    procedure_pattern = re.compile(r"\bPROCEDURE\s+([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in procedure_pattern.finditer(source_code))


def extract_generic_functions(source_code):
    function_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\s*\(", re.IGNORECASE)
    return set(match.group(1) for match in function_pattern.finditer(source_code))

def extract_sequences(source_code):
    """Extracts sequences used in the source code."""
    sequence_pattern = re.compile(r"\b([a-zA-Z0-9_]+)\.NEXTVAL", re.IGNORECASE)
    return sequence_pattern.findall(source_code)