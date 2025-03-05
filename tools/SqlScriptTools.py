from enum import Enum

import sqlparse


class SqlScriptFilenamePrefix(Enum):
    SEQUENCE = "CREATE_SEQUENCE_"
    CREATE_TABLE = "CREATE_TABLE_"
    ALTER_TABLE = "ALTER_TABLE_"
    PACKAGE = "CREATE_PACKAGE_"
    SYNONYM = "CREATE_SYNONYM_"
    GRANT = "CREATE_GRANT_"
    FUNCTION = "CREATE_FUNCTION_"
    PROCEDURE = "CREATE_PROCEDURE_"
    TRIGGER = "CREATE_TRIGGER_"


def format_sql_by_steps(sql_text: str) -> str:
    return add_indentation(format_sql(sql_script=sql_text))


def format_sql(sql_script: str) -> str:
    """
    Formats an Oracle SQL script using the sqlparse library.

    Args:
        sql_script (str): The raw SQL script.

    Returns:
        str: The formatted SQL script.
    """
    formatted_script = sqlparse.format(
        sql_script,
        reindent=True,  # Add indentation
        keyword_case="upper",  # Convert keywords to uppercase
        indent_width=4,  # Use 4 spaces for indentation
        comma_first=True,  # Place commas at the beginning of new lines
        strip_comments=True  # Preserve comments
    )
    return formatted_script


def add_indentation(formatted_script: str) -> str:
    """
    Adds an extra level of indentation to procedures and functions in the SQL script.

    Args:
        formatted_script (str): The formatted SQL script.

    Returns:
        str: The script with additional indentation for procedures and functions.
    """
    lines = formatted_script.splitlines()
    indented_lines = []

    for line in lines:
        stripped_line = line.strip()
        # Add an extra tab for PROCEDURE and FUNCTION lines
        if stripped_line.startswith(("PROCEDURE", "FUNCTION")):
            indented_lines.append("\t" + line)
        else:
            indented_lines.append(line)

    return "\n".join(indented_lines)
