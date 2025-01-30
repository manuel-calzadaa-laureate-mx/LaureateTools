from typing import Dict

from db.DatabaseProperties import DatabaseEnvironment
from files.MappingFile import read_mapping_data
from files.ObjectDataFile import load_object_data_to_json


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
            f"BEGIN\n"
            f"  NULL;\n"
            f"END;\n"
            f"\n"
            f"/\n"
            f"\n"
            f"SHOW ERRORS;\n")


def build_sequence_section(sequences: list) -> str:
    """
    Builds the section of the script for creating sequences.
    """
    sequences_script = "-- Secuencias asignadas a esta tabla\n\n"
    for sequence in sequences:
        sequences_script += (
            f"CREATE SEQUENCE {sequence['name']}\n"
            f"   INCREMENT BY {sequence['increment_by']}\n"
            f"   START WITH {sequence['start_with']}\n"
            f"   MAXVALUE {sequence['max_value']}\n"
            f"   {'CYCLE' if sequence['cycle'] else 'NOCYCLE'}\n"
            f"   CACHE {sequence['cache']};\n\n"
        )
    sequences_script += "\nSHOW ERRORS;\n"
    return sequences_script


def build_trigger_section(triggers: list) -> str:
    """
    Builds the section of the script for creating triggers.
    """
    triggers_script = "-- TRIGGERS ASIGNADOS A ESTA TABLA\n\n"
    for trigger in triggers:
        triggers_script += (
            f"CREATE OR REPLACE TRIGGER {trigger['name']}\n"
            f"   {trigger['event']}\n"
            f"   ON {trigger['table']}\n"
            f"   FOR EACH ROW\n"
            f"BEGIN\n"
            f"{trigger['body']}\n"
            f"END;\n/\n\n"
        )
    triggers_script += "\nSHOW ERRORS;\n"
    return triggers_script


def build_grant_section(grants: list) -> str:
    """
    Builds the section of the script for creating grants.
    """
    grants_script = "-- GRANTS ASIGNADOS A ESTA TABLA\n\n"
    for grant in grants:
        grants_script += (
            f"{grant}\n"
        )
    grants_script += "\nSHOW ERRORS;\n"
    return grants_script


def build_synonym_section(synonym: str) -> str:
    """
    Builds the section of the script for creating synonym.
    """
    synonym_script = "-- SYNONYM ASIGNADO A ESTA TABLA\n\n"
    synonym_script += (
        f"{synonym}\n"
        f"\nSHOW ERRORS;\n"
    )
    return synonym_script


def build_create_table_script_data(requested_environment: DatabaseEnvironment = DatabaseEnvironment.BANNER9) -> dict:
    mapping_data = read_mapping_data()
    table_names = [
        value["B9_NOMBRE"]
        for value in mapping_data.values()
        if value.get("B9_NOMBRE") not in (None, "", [])
    ]

    json_data = load_object_data_to_json()
    scripts = {}

    for env in json_data["root"]:
        environment = env["environment"]
        if environment == requested_environment.value:
            for obj in env["objects"]:
                if obj["name"] in table_names and obj["type"] == "TABLE":
                    table_name = obj["name"]
                    object_type = obj["type"]
                    object_owner = obj['owner']
                    create_table_section = build_create_table_section(obj)
                    tablespace_section = build_tablespace_section(obj.get("attributes", {}))
                    comments_section = build_comments_section(
                        obj.get("comments", {}), object_owner, table_name
                    )
                    index_section = build_indexes_and_primary_key_section(obj.get("indexes", {}),
                                                                          object_owner,
                                                                          table_name)
                    custom_sequences_section = build_sequence_section(sequences=obj.get("sequences", {}))
                    custom_trigger_section = build_trigger_section(triggers=obj.get("triggers", {}))

                    custom_grant_section = build_grant_section(grants=obj.get("grants", {}))
                    custom_synonym_section = build_synonym_section(synonym=obj.get("synonym"))

                    # Start with the fixed parts of the filename
                    filename_parts = [f"CREATE_TABLE_{table_name}", object_owner, "TB"]

                    # Conditionally add "IDX", "SEQ", and "TR" based on sections
                    if index_section.strip():  # Ensure there's meaningful content in the index section
                        filename_parts.append("IDX")
                    if custom_sequences_section.strip():  # Check if the sequences section has data
                        filename_parts.append("SEQ")
                    if custom_trigger_section.strip():  # Check if the triggers section has data
                        filename_parts.append("TR")
                    if custom_grant_section.strip():
                        filename_parts.append("GNT")
                    if custom_synonym_section.strip():
                        filename_parts.append("SYN")

                    # Join all parts with a dot and add the file extension
                    filename = ".".join(filename_parts) + ".sql"

                    header_section = build_header_section(filename)
                    drop_object_section = build_drop_section(object_type, object_owner, table_name)
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
                              f"{custom_sequences_section}\n"
                              f"\n"
                              f"{custom_trigger_section}\n"
                              f"\n"
                              f"{custom_grant_section}\n"
                              f"\n"
                              f"{custom_synonym_section}\n"
                              f"\n"                              
                              f"{footer_section}")

                    # Add the script to the dictionary with the table name as the key
                    scripts[filename] = script

    return scripts


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
        # Determine if this is the primary key
        if index.get("uniqueness") == "UNIQUE" and index.get("constraint_type") == "P" and primary_key is None:
            primary_key = index
            continue  # Skip processing as a regular index

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
                    columns.append(f"{col['column_name']} {col['descend']}")
            else:
                columns.append(f"{col['column_name']} {col['descend']}")

        columns_str = ", ".join(columns)

        # Build the CREATE INDEX statement
        index_script = (
            f"CREATE {'UNIQUE ' if index.get('uniqueness') == 'UNIQUE' else ''}INDEX {table_owner}.{index['name']} "
            f"ON {table_owner}.{table_name}\n"
            f"       ({columns_str})\n"
            f"       LOGGING\n"
            f"       TABLESPACE {index['tablespace']}\n"
            f"       PCTFREE    {index.get('pct_free', 0)}\n"
            f"       INITRANS   {index.get('ini_trans', 0)}\n"
            f"       MAXTRANS   {index.get('max_trans', 0)}\n"
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

    script += "\nSHOW ERRORS;\n"
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
