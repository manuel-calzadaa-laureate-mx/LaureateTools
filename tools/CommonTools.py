from db.DatabaseProperties import DatabaseEnvironment
from db.datasource.AllSourcesDatasource import get_all_owners


def parse_object_name(obj_name: str) -> dict:
    """
    Parses an object name string and returns a dictionary with '
    package' or 'owner' and 'object name'.

    Args:
        obj_name (str): The object name string, expected format 'PACKAGE.OBJECT' or 'OBJECT'.

    Returns:
        dict: A dictionary with:
            - 'prefix' or : The package name (None if not present).
            - 'object': The object name (None if empty).
    """
    if not obj_name:  # Handle empty input
        return {"prefix": None, "name": None}

    parts = obj_name.split(".", 1)  # Split only at the first dot
    return {"prefix": parts[0] if len(parts) > 1 else None, "name": parts[-1]}

def get_all_current_owners(database_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER7) -> list:
    all_owners_fetchall = get_all_owners(database_environment=database_environment)
    return [row[0] for row in all_owners_fetchall]