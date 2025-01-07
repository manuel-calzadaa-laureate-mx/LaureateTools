import csv

def extract_unique_dependency_objects(file_path: str) -> list:
    """
    Reads a file with dependency information and extracts unique function dependency names.

    Parameters:
        file_path (str): The path to the dependencies file.

    Returns:
        list: A sorted list of unique function dependency names.
    """
    unique_objects = set()

    try:
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                if row['DEPENDENCY_TYPE'].strip() == 'FUNCTION' or row['DEPENDENCY_TYPE'].strip() == 'PROCEDURE':
                    unique_objects.add(row['DEPENDENCY_NAME'].strip())

        return sorted(unique_objects)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []

def extract_unique_dependency_function(file_path: str) -> list:
    """
    Reads a file with dependency information and extracts unique function dependency names.

    Parameters:
        file_path (str): The path to the dependencies file.

    Returns:
        list: A sorted list of unique function dependency names.
    """
    unique_functions = set()

    try:
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                if row['DEPENDENCY_TYPE'].strip() == 'FUNCTION':
                    unique_functions.add(row['DEPENDENCY_NAME'].strip())

        return sorted(unique_functions)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []

def extract_unique_existing_objects(file_path: str) -> list:
    """
    Reads a file with dependency information and extracts unique existing function names.

    Parameters:
        file_path (str): The path to the dependencies file.

    Returns:
        list: A sorted list of unique existing function names.
    """
    unique_existing_objects = set()

    try:
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                if row['OBJECT_TYPE'].strip() == 'FUNCTION' or row['OBJECT_TYPE'].strip() == 'PROCEDURE':
                    unique_existing_objects.add(row['OBJECT_NAME'].strip())

        return sorted(unique_existing_objects)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []

def extract_unique_existing_functions(file_path: str) -> list:
    """
    Reads a file with dependency information and extracts unique existing function names.

    Parameters:
        file_path (str): The path to the dependencies file.

    Returns:
        list: A sorted list of unique existing function names.
    """
    unique_functions = set()

    try:
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                if row['OBJECT_TYPE'].strip() == 'FUNCTION':
                    unique_functions.add(row['OBJECT_NAME'].strip())

        return sorted(unique_functions)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return []
    except KeyError as e:
        print(f"Error: Missing expected column in file - {e}")
        return []