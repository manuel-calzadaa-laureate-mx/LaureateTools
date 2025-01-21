import json
from typing import Dict

from db.DatabaseProperties import DatabaseEnvironment


def build_header_section(filename: str):
    return (f"Prompt>>>\n"
            f"Prompt>>> [INI] ** Ejecutando {filename}\n"
            f"Prompt>>>\n")


def build_create_table_section(obj: Dict) -> str:
    """
    Construye la sección CREATE TABLE del script.
    """
    script = (f"-- Creaciones\n\n"
              f"CREATE TABLE {obj['owner']}.{obj['name']}\n(\n")
    for column in obj["columns"]:
        col_def = f"  {column['name']} {column['type']}"
        if column['type'] == "NUMBER":
            if column['precision'] is not None and column['scale'] is not None:
                col_def += f"({column['precision']}, {column['scale']})"
            elif column['precision'] is not None:
                col_def += f"({column['precision']})"
        elif column['type'] == "VARCHAR2":
            col_def += f"({column['length']} CHAR)"
        col_def += " NOT NULL" if not column['nullable'] else ""
        col_def += ",\n"
        script += col_def

    script = script.rstrip(",\n") + "\n)"
    return script


def build_tablespace_section(attributes: Dict) -> str:
    """
    Construye la sección de atributos de tablespace.
    """
    tablespace = attributes.get("tablespace", "")
    pct_free = attributes.get("pct_free", 0)
    pct_used = attributes.get("pct_used", 0)
    ini_trans = attributes.get("ini_trans", 1)
    max_trans = attributes.get("max_trans", 255)
    logging = attributes.get("logging", "YES")

    storage = (
        "STORAGE    (\n"
        "            INITIAL          65536\n"
        "            NEXT             1048576\n"
        "            MINEXTENTS       1\n"
        "            MAXEXTENTS       UNLIMITED\n"
        "            PCTINCREASE      0\n"
        "            BUFFER_POOL      DEFAULT\n"
        "           )\n"
    )

    logging_clause = "LOGGING" if logging == "YES" else "NOLOGGING"

    tablespace_script = (
        f"TABLESPACE {tablespace}\n"
        f"PCTUSED    {pct_used}\n"
        f"PCTFREE    {pct_free}\n"
        f"INITRANS   {ini_trans}\n"
        f"MAXTRANS   {max_trans}\n"
        f"{storage}"
        f"{logging_clause} \n"
        "NOCOMPRESS \n"
        "NOCACHE\n"
        "NOPARALLEL\n"
        "MONITORING;"
    )
    return tablespace_script


def build_comments_section(comments: Dict, table_owner: str, table_name: str) -> str:
    """
    Construye la sección de comentarios del script.
    """

    comment_script = "-- Descripcion de los Campos\n\n"
    for comment in comments:
        if comment["comment"]:
            comment_script += f"   COMMENT ON COLUMN {table_owner}.{table_name}.{comment["name"]} IS '{comment["comment"]}';\n"
    return comment_script


def build_drop_section(object_type: str, object_owner: str, object_name: str):
    return (f"-- Eliminaciones\n"
            f"-- DROP TABLE {object_owner}.{object_name}\n")

def build_footer_section(filename):
    return (f"COMMIT;\n"
            f"\n"
            f"Prompt>>>\n"
            f"Prompt>>> [FIN] ** Creando {filename}\n"
            f"Prompt>>>\n"
            f"\n"
            f"Begin\n"
            f"  Null;\n"
            f"End;\n"
            f"\n"
            f"/\n"
            f"\n"
            f"Show Errors;\n")


def build_create_table_script(object_data_file: str, environment: DatabaseEnvironment, table_names: [str]) -> dict:
    try:
        with open(object_data_file, 'r') as file:
            json_data = json.load(file)

        scripts = {}

        for env in json_data["root"]:
            if env["environment"] == environment.value:
                for obj in env["objects"]:
                    if obj["name"] in table_names and obj["type"] == "TABLE":
                        create_table_section = build_create_table_section(obj)
                        tablespace_section = build_tablespace_section(obj.get("attributes", {}))
                        comments_section = build_comments_section(
                            obj.get("comments", {}), obj['owner'], obj['name']
                        )
                        index_section = build_indexes_and_primary_key_section(obj.get("indexes", {}), obj['owner'],
                                                                              obj["name"])
                        #custom_sequence_section = build_custom_sequence(obj.get("sequences",{})
                        #custom_trigger_section = build_custom_trigger(obj.get("triggers",{})

                        filename = "CREATE_TABLE_" + obj["name"] + "." + obj['owner'] + ".TB.IDX." + "sql"
                        header_section = build_header_section(filename)
                        drop_object_section = build_drop_section(obj["type"], obj['owner'], obj["name"])
                        footer_section = build_footer_section(filename)

                        script = (f"{header_section}"
                                  f"\n"
                                  f"{drop_object_section}"
                                  f"\n"
                                  f"{create_table_section}\n"
                                  f"{tablespace_section}\n"
                                  f"\n"
                                  f"{comments_section}\n"
                                  f"\n"
                                  f"{index_section}\n"
                                  f"\n"
                                  f"{footer_section}")

                        # Add the script to the dictionary with the table name as the key
                        scripts[filename] = script

        return scripts

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{object_data_file}' was not found.")
    except json.JSONDecodeError:
        raise ValueError(f"The file '{object_data_file}' is not a valid JSON file.")


def build_indexes_and_primary_key_section(indexes: list, table_owner: str, table_name: str) -> str:
    """
    Generates the SQL script for primary keys and indexes based on the JSON definition.

    :param indexes: List of index definitions from the JSON file.
    :param table_owner: Owner of the table.
    :param table_name: Name of the table.
    :return: SQL script string.
    """
    script = "-- Primary Key\n\n"
    primary_key = None
    index_scripts = []

    for index in indexes:

        # Check if it's the primary key
        if index.get("uniqueness") == "UNIQUE":
            primary_key = index
            continue

        # Skip system-generated indexes
        if index["name"].startswith("SYS_"):
            continue

        # Build the column list or expressions
        columns = []
        for col in index["columns"]:
            if col["index_type"] in ["FUNCTION-BASED NORMAL", "FUNCTION-BASED DOMAIN"]:
                # Use the column_expression if available for function-based indexes
                if col["column_expression"]:
                    columns.append(f"{col['column_expression']} {col['descend']}")
                else:
                    # Fallback to the column name if expression is missing
                    columns.append(f"{col['column_name']} {col['descend']}")
            else:
                # Normal column reference for other index types
                columns.append(f"{col['column_name']} {col['descend']}")

        columns_str = ", ".join(columns)

        # Build the CREATE INDEX statement
        index_script = (
            f"CREATE {'UNIQUE ' if index.get('uniqueness') == 'UNIQUE' else ''}INDEX {table_owner}.{index['name']} "
            f"ON {table_owner}.{table_name}\n"
            f"       ({columns_str})\n"
            f"       LOGGING\n"
            f"       TABLESPACE {index['tablespace']}\n"
            f"       PCTFREE    {index['pct_free']}\n"
            f"       INITRANS   {index['ini_trans']}\n"
            f"       MAXTRANS   {index['max_trans']}\n"
            f"       STORAGE    (\n"
            f"                   INITIAL          65536\n"
            f"                   NEXT             1048576\n"
            f"                   MINEXTENTS       1\n"
            f"                   MAXEXTENTS       UNLIMITED\n"
            f"                   PCTINCREASE      0\n"
            f"                   BUFFER_POOL      DEFAULT\n"
            f"                  )\n"
            f"    NOPARALLEL;\n"
        )
        index_scripts.append(index_script)

    # Add the primary key constraint if defined
    if primary_key:
        pk_columns = ", ".join([col["column_name"] for col in primary_key["columns"]])
        pk_name = primary_key["name"]
        script += f"ALTER TABLE {table_owner}.{table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});\n\n"

    # Add the index scripts
    if index_scripts:
        script += "-- Indices\n\n" + "\n".join(index_scripts)

    script += (f"\n"
               f"SHOW ERRORS;\n")
    return script


def build_create_trigger_script(trigger_info: dict) -> str:
    trigger_script = f"""
    CREATE OR REPLACE TRIGGER {trigger_info['owner']}.{trigger_info['name']}
    {trigger_info['trigger_type']}
    {trigger_info['triggering_event']}
    ON {trigger_info['table_name']}
    {trigger_info['referencing_names']}
    {f"WHEN ({trigger_info['when_clause']})" if trigger_info['when_clause'] else ""}
    BEGIN
    {trigger_info['trigger_body']}
    END;
    /

    ALTER TRIGGER {trigger_info['owner']}.{trigger_info['name']} {trigger_info['status']};
    """
    return trigger_script.strip()


if __name__ == "__main__":
    # Trigger details
    trigger_info = {
        "owner": "SATURN",
        "name": "SZTR_STDN_AOL",
        "type": "TRIGGER",
        "table_name": "SGBSTDN",
        "trigger_type": "AFTER EACH ROW",
        "triggering_event": "INSERT OR UPDATE OR DELETE",
        "referencing_names": "REFERENCING NEW AS NEW OLD AS OLD",
        "when_clause": None,
        "status": "ENABLED",
        "description": "SATURN.SZTR_STDN_AOL \nAFTER INSERT OR UPDATE OR DELETE ON SGBSTDN\nFOR EACH ROW\n",
        "trigger_body": """DECLARE
    
       VL_NUM_ERROR   VARCHAR2(100);
       VL_MENSAJE     VARCHAR2(500);
       VL_GET_CAMPUS  VARCHAR2(5);
       VL_VALOR       VARCHAR2(5);
    
    BEGIN
    
       SELECT F_GET_CAMPUS 
         INTO VL_GET_CAMPUS 
         FROM DUAL;
    
       SELECT NVL(ZSFNVALOR('CONF_AOL', 'CAMPONLN'), '53')
         INTO VL_VALOR
         FROM DUAL;
    
       IF VL_GET_CAMPUS <> VL_VALOR THEN
    
          IF INSERTING THEN
    
             SZPK_AOL_EV.P_REGISTRA_EVENTOS('CURRICULA', :NEW.SGBSTDN_TERM_CODE_EFF, :NEW.SGBSTDN_PIDM, NULL, 'R', VL_NUM_ERROR, VL_MENSAJE);
             SZPK_AOL_EV.P_REGISTRA_EVENTOS('PERSONAS', :NEW.SGBSTDN_PIDM, NULL, NULL, 'R', VL_NUM_ERROR, VL_MENSAJE);
    
          ELSIF UPDATING THEN
    
             SZPK_AOL_EV.P_REGISTRA_EVENTOS('CURRICULA', :NEW.SGBSTDN_TERM_CODE_EFF, :NEW.SGBSTDN_PIDM, NULL, 'R', VL_NUM_ERROR, VL_MENSAJE);
             SZPK_AOL_EV.P_REGISTRA_EVENTOS('PERSONAS', :NEW.SGBSTDN_PIDM, NULL, NULL, 'R', VL_NUM_ERROR, VL_MENSAJE);
    
          ELSE 
    
             SZPK_AOL_EV.P_REGISTRA_EVENTOS('CURRICULA', :OLD.SGBSTDN_TERM_CODE_EFF, :OLD.SGBSTDN_PIDM, NULL, 'R', VL_NUM_ERROR, VL_MENSAJE);
             SZPK_AOL_EV.P_REGISTRA_EVENTOS('PERSONAS', :OLD.SGBSTDN_PIDM, NULL, NULL, 'R', VL_NUM_ERROR, VL_MENSAJE);
    
          END IF;
    
       END IF;
    
    END;"""
    }

    print(type(trigger_info))

    # Generate the script
    # print(build_create_trigger_script(trigger_info))
