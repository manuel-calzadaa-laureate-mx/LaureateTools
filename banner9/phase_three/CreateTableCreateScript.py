from db.DatabaseProperties import DatabaseEnvironment
from tools.SqlScriptTools import build_create_table_script

if __name__ == "__main__":
    object_data = "../../object_data.json"
    table_name = "TZRCIBK"
    environment = DatabaseEnvironment.BANNER7
    script = build_create_table_script(object_data_file=object_data, table_name=table_name, environment=environment)
    print(script)

