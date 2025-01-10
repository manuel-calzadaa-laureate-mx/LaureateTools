import json

from db.DatabaseProperties import DatabaseEnvironment


def build_create_table_script(object_data_file: str, environment: DatabaseEnvironment, table_name: str):
    try:
        with open(object_data_file, 'r') as file:
            json_data = json.load(file)
        # Find the correct environment and table in the JSON data
        for env in json_data["root"]:
            if env["environment"] == environment.value:
                for obj in env["objects"]:
                    if obj["name"] == table_name and obj["type"] == "TABLE":

                        # Start building the CREATE TABLE script
                        script = f"CREATE TABLE {obj['owner']}.{obj['name']}\n(\n"

                        # Add columns to the script
                        for column in obj["columns"]:
                            col_def = f"  {column['name']} {column['type']}"
                            if column['type'] == "NUMBER":
                                # Include precision and scale for NUMBER type
                                if column['precision'] is not None and column['scale'] is not None:
                                    col_def += f"({column['precision']}, {column['scale']})"
                                elif column['precision'] is not None:
                                    col_def += f"({column['precision']})"
                            elif column['type'] == "VARCHAR2":
                                # Include length for VARCHAR2 type
                                col_def += f"({column['length']} CHAR)"
                            # Add nullability constraint
                            col_def += " NOT NULL" if not column['nullable'] else ""
                            col_def += ",\n"
                            script += col_def

                        # Remove trailing comma and newline, close parentheses
                        script = script.rstrip(",\n") + "\n)"
                        return script

        # If no matching environment or table is found
        return f"Table {table_name} in environment {environment} not found."
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{object_data_file}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{object_data_file}' is not a valid JSON file.")