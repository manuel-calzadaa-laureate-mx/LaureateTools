from db.OracleDatabaseTools import OracleDBConnectionPool
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


def get_all_current_owners(db_pool: OracleDBConnectionPool) -> list:
    all_owners_fetchall = get_all_owners(db_pool=db_pool)
    return [row[0] for row in all_owners_fetchall]


class MultiCounter:
    def __init__(self):
        self.counters = {}

    def _get_counter(self, key: str) -> int:
        """
        Internal helper to get the counter value, initializing it if it doesn't exist.
        """
        if key not in self.counters:
            self.counters[key] = 0
        return self.counters[key]

    def _set_counter(self, key: str, value: int):
        """
        Internal helper to set the counter value.
        """
        if value < 0:
            raise ValueError("Counter value cannot be negative")
        self.counters[key] = value

    def back(self, key: str) -> str:
        """Returns the previous count for the given key as a zero-padded string."""
        current_value = self._get_counter(key)
        self._set_counter(key, current_value - 1)
        return f"{self.counters[key]:03d}"

    def repeat(self, key: str) -> str:
        """Returns the current count for the given key as a zero-padded string."""
        current_value = self._get_counter(key)
        return f"{current_value:03d}"

    def next(self, key: str) -> str:
        """Returns the next count for the given key as a zero-padded string."""
        current_value = self._get_counter(key)
        self._set_counter(key, current_value + 1)
        return f"{self.counters[key]:03d}"

    def reset(self, key: str = None):
        """Resets the counter for a specific key or all counters if no key is given."""
        if key:
            self._set_counter(key, 0)
        else:
            self.counters.clear()

    def set_value(self, key: str, value: int):
        """Sets the counter for a specific key to a given value."""
        self._set_counter(key, value)


def extract_object_structure(object_name: str) -> dict:
    # Check if the table name is at least 5 characters long
    if len(object_name) < 5:
        raise ValueError(f"Invalid table name: {object_name} is too short.")

    # Extract department (first character)
    department = object_name[0]

    # Extract custom (second character, must be 'Z')
    custom = object_name[1]
    if custom != "Z":
        raise ValueError(f"Invalid table name: {object_name} does not have 'Z' as the second character.")

    # Extract object identification (third and fourth characters)
    object_identification = object_name[2:4]

    # Extract module (fifth character)
    module = object_name[4]

    # Extract base (sixth character to the end)
    base = object_name[5:]

    # Return the extracted values
    return {
        "department": department,
        "custom": custom,
        "prefix": department + custom,
        "object_identification": object_identification,
        "module": module,
        "base": base + module,
        "only_base": base,
        "object_name": object_name
    }


def refactor_tagged_text(original_text: str, tags: list[str], replacement_text: list[str]) -> str:
    if len(tags) != len(replacement_text):
        raise ValueError("Tags and replacement_text lists must have the same length")

    for tag, replacement in zip(tags, replacement_text):
        original_text = original_text.replace(tag, replacement)

    return original_text
