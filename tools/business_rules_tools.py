import re


def is_custom_table(table_name: str) -> bool:
    return len(table_name.upper()) > 1 and table_name.upper()[1] == 'Z'


def validate_component(component_name: str) -> bool:
    if len(component_name) >= 9:
        if component_name[1].upper() == 'Z':
            return True
    return False


def generate_new_b9_name(b7_type: str, b7_name: str = None):
    """
    Generate the B9_NAME value based on the given B7_TYPE and optionally B7_NAME.

    Parameters:
        b7_type (str): The type of the object (B7_TYPE).
        b7_name (str, optional): The name of the object (B7_NAME) to transform if needed.

    Returns:
        str: The B9_NAME value based on the rules.
    """
    if b7_type == "SEQUENCE":
        return "TZSEAAAA"
    elif b7_type == "TABLE":
        if b7_name and re.match(r"^[GTZ]Z[A-Z]+", b7_name):
            return "TZTB" + b7_name[2:]
        return "TZTBAAAA"
    else:
        return "none"
