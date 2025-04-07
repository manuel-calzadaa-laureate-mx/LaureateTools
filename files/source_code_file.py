import re

from db.database_properties import DatabaseEnvironment
from db.oracle_database_tools import is_oracle_built_in_object
from tools.pattern_matching_tools import extract_select_tables, extract_insert_tables, extract_update_tables, \
    extract_delete_tables, extract_type_declarations, extract_functions, extract_local_functions, extract_procedures, \
    extract_sequences

B7_SOURCE_CODE_FOLDER = "../workfiles/b7_sources"
B9_SOURCE_CODE_FOLDER = "../workfiles/b9_sources"


def get_source_code_folder(database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> str:
    if database_environment == DatabaseEnvironment.BANNER7:
        return B7_SOURCE_CODE_FOLDER
    elif database_environment == DatabaseEnvironment.BANNER9:
        return B9_SOURCE_CODE_FOLDER
    else:
        return ""


def clean_comments_and_whitespace(source_code_text: str) -> str:
    source_code = "\n".join(source_code_text)
    source_code = _remove_single_lines_comments(source_code)
    source_code = _remove_multiline_comments(source_code)
    source_code = _normalize_spaces(source_code)
    return source_code


def _normalize_spaces(source_code):
    return re.sub(r"\s{2,}", " ", source_code)


def _remove_multiline_comments(source_code):
    return re.sub(r"/\*.*?\*/", "", source_code, flags=re.DOTALL)


def _remove_single_lines_comments(source_code):
    return re.sub(r"--.*", "", source_code)


def extract_all_dependencies_from_one_source_code_data(source_code_lines: [str]) -> dict:
    source_code = clean_comments_and_whitespace(source_code_lines)

    # Find all tables
    select_tables = extract_select_tables(source_code)
    insert_tables = extract_insert_tables(source_code)
    update_tables = extract_update_tables(source_code)
    delete_tables = extract_delete_tables(source_code)
    type_tables = extract_type_declarations(source_code)

    # Combine all unique table names
    all_tables = set(select_tables + insert_tables + update_tables + delete_tables + type_tables)
    user_defined_tables = {table.upper() for table in all_tables if not is_oracle_built_in_object(table)}

    # Find all global functions
    all_functions = extract_functions(source_code)
    local_functions = extract_local_functions(source_code)

    # Find all procedures
    procedures = extract_procedures(source_code)

    # Find all sequences
    sequences = extract_sequences(source_code)

    return {
        "TABLE": sorted(user_defined_tables),
        "FUNCTION": sorted(all_functions),
        "LOCAL_FUNCTION": sorted(local_functions),
        "SEQUENCE": sorted(sequences),
        "PROCEDURE": sorted(procedures)
    }
