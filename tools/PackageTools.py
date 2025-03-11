import sqlparse

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool
from db.datasource.PackagesDatasource import get_package_records, get_package_record
from tools.SqlScriptTools import format_sql_by_steps


def package_specification_extract_and_format(lines: list[dict]) -> list[dict]:
    """
    Extracts and formats objects (PACKAGE, PROCEDURE, FUNCTION, TYPE, etc.) from a list of lines.

    Args:
        lines (list[dict]): A list of dictionaries, each containing 'line' and 'text' keys.

    Returns:
        list[dict]: A list of dictionaries with formatted objects.
    """
    formatted_objects = []
    in_comment_block = False
    in_procedure_or_function = False
    current_object_lines = []

    for line_dict in lines:
        line_text = line_dict["text"].strip()

        # Skip empty lines
        if not line_text:
            continue

        # Handle comment blocks
        if line_text.startswith("/*"):
            in_comment_block = True
            comment_block = [line_dict["text"].rstrip()]  # Start the comment block
            continue
        if in_comment_block:
            comment_block.append(line_dict["text"].rstrip())  # Add lines to the comment block
            if line_text.endswith("*/"):
                in_comment_block = False
                # Join the comment block into a single object
                formatted_objects.append({"object": "\n".join(comment_block)})
            continue

        # Handle PACKAGE and END PACKAGE (don't format, uppercase)
        if line_text.upper().startswith(("PACKAGE", "END PACKAGE")):
            formatted_objects.append({"object": line_text.upper()})
            continue

        # Handle PROCEDURE, FUNCTION, and TYPE (format with sqlparse)
        if line_text.upper().startswith(("PROCEDURE", "FUNCTION", "TYPE")):
            in_procedure_or_function = True
            current_object_lines = [line_text]  # Start collecting lines for the object
            continue

        # If we're inside a PROCEDURE or FUNCTION, collect lines until we reach a semicolon
        if in_procedure_or_function:
            current_object_lines.append(line_text)
            if line_text.endswith(";"):
                in_procedure_or_function = False
                # Join the lines and format with sqlparse
                object_text = "\n".join(current_object_lines)
                formatted_text = format_sql_by_steps(object_text)
                formatted_objects.append({"object": formatted_text})
            continue

        # Handle other lines (e.g., standalone comments, declarations)
        formatted_objects.append({"object": line_text})

    return formatted_objects


def _group_package_dictionary_by_name_and_type(package_dictionary: list[dict]) -> dict:
    """
    Groups the list of dictionaries by package name, owner, and type.

    Args:
        package_dictionary (list): A list of dictionaries representing the package rows.

    Returns:
        dict: A dictionary where the key is the package name, and the value is a nested dictionary
              containing the name, owner, and code (with types and lines).
    """
    grouped_packages = {}

    for record in package_dictionary:
        name = record["name"]
        owner = record["owner"]
        package_type = record["type"]
        line = record["line"]
        text = record["text"]

        # Initialize the package entry if it doesn't exist
        if name not in grouped_packages:
            grouped_packages[name] = {
                "name": name,
                "owner": owner,
                "code": {}
            }

        # Initialize the type entry if it doesn't exist
        if package_type not in grouped_packages[name]["code"]:
            grouped_packages[name]["code"][package_type] = {
                "lines": []
            }

        # Append the line to the corresponding type
        grouped_packages[name]["code"][package_type]["lines"].append({
            "line": line,
            "text": text
        })

    return grouped_packages


def get_package_as_list(package_owner: str, package_name: str,
                        database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER9) -> dict:
    package_records = get_package_record(package_owner=package_owner, package_name=package_name,
                                         database_environment=database_environment)

    package_dictionary = _convert_package_records_to_dictionary(package_rows=package_records)
    grouped_package = _group_package_dictionary_by_name_and_type(package_dictionary=package_dictionary)

    return grouped_package


def extract_and_format_objects(lines: list[dict]) -> list[dict]:
    """
    Extracts and formats objects (PACKAGE, PROCEDURE, FUNCTION, TYPE, etc.) from a list of lines.

    Args:
        lines (list[dict]): A list of dictionaries, each containing 'line' and 'text' keys.

    Returns:
        list[dict]: A list of dictionaries with formatted objects.
    """
    formatted_objects = []
    current_object = []
    in_comment_block = False
    comment_block = []

    for line_dict in lines:
        line_text = line_dict["text"].strip()

        # Skip empty lines
        if not line_text:
            continue

        # Handle comment blocks
        if line_text.startswith("/*"):
            in_comment_block = True
            comment_block = [line_dict["text"].rstrip()]  # Start the comment block
            continue
        if in_comment_block:
            comment_block.append(line_dict["text"].rstrip())  # Add lines to the comment block
            if line_text.endswith("*/"):
                in_comment_block = False
                # Join the comment block into a single object
                formatted_objects.append({"object": "\n".join(comment_block)})
            continue

        # Handle PACKAGE and END PACKAGE (don't format, uppercase)
        if line_text.upper().startswith(("PACKAGE", "END PACKAGE")):
            formatted_objects.append({"object": line_text.upper()})
            continue

        # Handle PROCEDURE, FUNCTION, and TYPE (format with sqlparse)
        if line_text.upper().startswith(("PROCEDURE", "FUNCTION", "TYPE")):
            # Collect all lines of the object
            object_lines = [line_text]
            for next_line_dict in lines[line_dict["line"]:]:
                next_line_text = next_line_dict["text"].strip()
                if not next_line_text:
                    continue
                object_lines.append(next_line_text)
                if next_line_text.endswith(";"):
                    break

            # Join the lines and format with sqlparse
            object_text = "\n".join(object_lines)
            formatted_text = sqlparse.format(
                object_text,
                reindent=True,
                keyword_case="upper",
                indent_width=4,
                strip_comments=False
            )
            formatted_objects.append({"object": formatted_text})
            continue

        # Handle other lines (e.g., standalone comments, declarations)
        formatted_objects.append({"object": line_text})

    return formatted_objects


def get_packages_as_list(package_owner: str, package_names: list[str],
                         db_pool: OracleDBConnectionPool) -> dict:
    package_records = get_package_records(package_owner=package_owner, package_names=package_names,
                                          db_pool=db_pool)
    package_dictionary = _convert_package_records_to_dictionary(package_rows=package_records)
    grouped_package = _group_package_dictionary_by_name_and_type(package_dictionary=package_dictionary)
    return grouped_package


def _convert_package_records_to_dictionary(package_rows):
    """
    Converts the rows returned by get_package into a list of dictionaries.

    Returns:
        list of dict: A list of dictionaries where each dictionary represents a row.
    """
    result = []
    for row in package_rows:
        owner, name, package_type, line, text = row
        result.append({
            "name": name,
            "owner": owner,
            "type": package_type,
            "line": line,
            "text": text
        })
    return result


if __name__ == "__main__":
    package = get_packages_as_list(package_names=['FZPKC_CONTA_ELECT_UVM'], package_owner="UVM",
                                   database_environment=DatabaseEnvironment.BANNER9)
    print(package)
