from db.DatabaseProperties import DatabaseEnvironment
from db.datasource.AllSourcesDatasource import get_all_owners


def split_table_name_into_package_and_table_name(obj_name: str) -> dict:
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

class MultiCounter:
    def __init__(self):
        self.counters = {}

    def next(self, key: str) -> str:
        """Returns the next count for the given key as a zero-padded string."""
        if key not in self.counters:
            self.counters[key] = 0  # Initialize if not present
        self.counters[key] += 1
        return f"{self.counters[key]:03d}"

    def reset(self, key: str = None):
        """Resets the counter for a specific key or all counters if no key is given."""
        if key:
            self.counters[key] = 0
        else:
            self.counters.clear()

    def set_value(self, key: str, value: int):
        """Sets the counter for a specific key to a given value."""
        if value < 0:
            raise ValueError("Counter value cannot be negative")
        self.counters[key] = value


def extract_table_info(table_name: str) -> dict:
    # Extract prefix: first two characters, where the second character must be "Z"
    if len(table_name) >= 2 and table_name[1] == "Z":
        prefix = table_name[:2]
    else:
        raise ValueError(f"Invalid table name: {table_name} does not meet the criteria.")

    # Extract base: check if "TB" exists, and use the part after it
    if "TB" in table_name:
        base_start = table_name.index("TB") + 2
        base = table_name[base_start:]
    else:
        # If "TB" is not present, use the part after the prefix
        base = table_name[2:]

    # Return the extracted values
    return {
        "prefix": prefix,
        "base": base,
        "table_name": table_name
    }
