import logging
import os
import re
from enum import Enum
from typing import Dict, Optional

from db.database_properties import DatabaseEnvironment, DatabaseObject
from files.b7_sql_script_file import get_scripts_folder_path
from files.object_addons_file import read_custom_data, GrantType, ObjectAddonType
from files.object_data_file import ObjectDataTypes, \
    get_migrated_object_data_mapped_by_names_by_environment_and_type, \
    get_object_data_mapped_by_names_by_environment_and_type
from files.source_code_file import get_source_code_folder
from tools.common_tools import ObjectTargetType
from tools.sql_script_tools import SqlScriptFilenamePrefix


class ScriptType(Enum):
    MIGRATED = "migrated"
    INSTALL = "install"
    ROLLBACK = "rollback"
    SETUP = "setup"


SCRIPT_FOLDER_PATH = "../workfiles/b9_scripts"

LINEFEED = "\n"
END_OF_SENTENCE = f";{LINEFEED}"
PROMPT = "Prompt >>>"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def find_install_script_file_name(script_type: str, object_action: str, object_type: str, object_name: str) -> Dict:
    scripts_folder_path = get_scripts_folder_path()
    full_script_folder_path = os.path.join(scripts_folder_path, script_type)
    target_pattern = f"{object_action}_{object_type}_{object_name}".lower()

    for filename in os.listdir(full_script_folder_path):
        if target_pattern in filename.lower() and filename.lower().endswith('.sql'):
            return {"full_path": os.path.join(full_script_folder_path, filename), "filename": filename}

    return None


def get_show_errors():
    return f"SHOW ERRORS{END_OF_SENTENCE}"


def get_show_errors_block():
    return f"/{LINEFEED}SHOW ERRORS{END_OF_SENTENCE}"


def build_header_section(filename: str):
    return (f"{PROMPT}{LINEFEED}"
            f"{PROMPT} [INI] ** Ejecutando {filename}{LINEFEED}"
            f"{PROMPT}{LINEFEED}"
            f"SET SERVEROUTPUT ON{LINEFEED}")


def get_package_opening_statement():
    return (f"SELECT TO_CHAR(SYSDATE,'DD/MON/YYYY HH24:MI') Fecha_Hora_Inicio from dual;"
            f"{LINEFEED}")


def get_package_body_closing_statement(package_name: str):
    return (f"select text "
            f"from user_errors "
            f"where upper(name) = '{package_name}'"
            f"and upper(type) = 'PACKAGE BODY';"
            f""
            f"select TO_CHAR(SYSDATE,'DD/MON/YYYY HH24:MI') Fecha_Hora_Fin from dual;")


def build_footer_section(filename):
    return (f"{PROMPT}{LINEFEED}"
            f"{PROMPT} [FIN] ** Creando {filename}{LINEFEED}"
            f"{PROMPT}{LINEFEED}"
            f"{LINEFEED}"
            f"BEGIN{LINEFEED}"
            f"  NULL;{LINEFEED}"
            f"END{END_OF_SENTENCE}"
            f"{get_show_errors_block()}")


def build_create_table_section(obj: Dict) -> str:
    """
    Construye la sección CREATE TABLE del script.
    """
    script = (f"-- Create table statement{LINEFEED}"
              f"{LINEFEED}"
              f"CREATE TABLE {obj['owner']}.{obj['name']}{LINEFEED}"
              f"({LINEFEED}")

    # Build column definitions
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
        col_def += f",{LINEFEED}"
        script += col_def

    # Remove trailing comma and close parentheses
    script = script.rstrip(f",{LINEFEED}") + f"{LINEFEED})"

    # Add TABLESPACE clause
    script += f"{LINEFEED}TABLESPACE USERS{END_OF_SENTENCE}"

    return script


def build_delete_table_section(obj: Dict) -> str:
    """
    Construye la sección DELETE TABLE del script.
    """
    script = (f"-- Drop table section{LINEFEED}"
              f"DROP TABLE {obj['owner']}.{obj['name']} CASCADE CONSTRAINTS;{LINEFEED}")

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
        f"STORAGE    ({LINEFEED}"
        f"            INITIAL          65536{LINEFEED}"
        f"            NEXT             1048576{LINEFEED}"
        f"            MINEXTENTS       1{LINEFEED}"
        f"            MAXEXTENTS       UNLIMITED{LINEFEED}"
        f"            PCTINCREASE      0{LINEFEED}"
        f"            BUFFER_POOL      DEFAULT{LINEFEED}"
        f"           ){LINEFEED}"
    )

    logging_clause = "LOGGING" if logging == "YES" else "NOLOGGING"

    tablespace_script = (
        f"{LINEFEED}"
        f"TABLESPACE {tablespace}{LINEFEED}"
        f"PCTUSED    {pct_used}{LINEFEED}"
        f"PCTFREE    {pct_free}{LINEFEED}"
        f"INITRANS   {ini_trans}{LINEFEED}"
        f"MAXTRANS   {max_trans}{LINEFEED}"
        f"{storage}"
        f"{logging_clause}{LINEFEED}"
        f"NOCOMPRESS{LINEFEED}"
        f"NOCACHE{LINEFEED}"
        f"NOPARALLEL{LINEFEED}"
        f"MONITORING{END_OF_SENTENCE}"
    )
    return tablespace_script


def build_comments_section(comments: Dict, table_owner: str, table_name: str) -> str:
    """
    Construye la sección de comentarios del script.
    """

    comment_script = f"-- Add table comments{LINEFEED}"
    for comment in comments:
        if comment["comment"]:
            comment_script += f"   COMMENT ON COLUMN {table_owner}.{table_name}.{comment["name"]} IS '{comment["comment"]}'{END_OF_SENTENCE}"
    return comment_script


def _convert_drop_objects_to_map_by_type(drop_objects: list[dict]) -> dict:
    """
    Converts a list of drop objects into a dictionary grouped by type.

    :param drop_objects: List of dictionaries containing object metadata.
    :return: Dictionary with keys as object types and formatted SQL strings as values.
    """
    drop_objects_map = {}

    for obj in drop_objects:
        obj_type = _extract_value(obj.get("type"))
        obj_name = _extract_value(obj.get("name"))
        obj_owner = _extract_value(obj.get("owner"))

        if obj_type == "TABLE":
            drop_objects_map["TABLE"] = f"{obj_owner}.{obj_name} CASCADE CONSTRAINTS"
        elif obj_type == "SEQUENCE":
            drop_objects_map["SEQUENCE"] = f"{obj_name}"
        elif obj_type == "SYNONYM":
            drop_objects_map["SYNONYM"] = f"{obj_name}"

    return drop_objects_map


def build_drop_section(drop_objects: list[dict]) -> str:
    """
    Builds an Oracle drop script using a map for efficient retrieval.

    :param drop_objects: List of dictionaries containing object metadata.
    :return: A formatted SQL script as a string.
    """
    drop_objects_map_by_type = _convert_drop_objects_to_map_by_type(drop_objects)

    # List of object types to consider for drop statements
    object_types = ['TABLE', 'SEQUENCE', 'SYNONYM']

    # Initialize the drop section with the header
    drop_section = f"-- Drop objects{LINEFEED}"

    # Loop through each object type and append the drop statement if the key exists
    for obj_type in object_types:
        if obj_type in drop_objects_map_by_type:
            drop_section += f"-- DROP {obj_type} {drop_objects_map_by_type[obj_type]}{END_OF_SENTENCE}"

    return drop_section


def _extract_value(value):
    """Extracts a single value from a set or returns the value if it's a string."""
    if isinstance(value, set) and value:
        return next(iter(value))  # Extract first element from set
    return value  # Return string directly if not a set


def build_sequence_section(sequences: list) -> str:
    """
    Builds the section of the script for creating sequences.
    """
    sequences_script = (f"-- Create sequences{LINEFEED}"
                        f"{LINEFEED}")
    for sequence in sequences:
        sequences_script += (
            f"CREATE SEQUENCE UVM.{sequence['name']}{LINEFEED}"
            f"   INCREMENT BY {sequence['increment_by']}{LINEFEED}"
            f"   START WITH {sequence['start_with']}{LINEFEED}"
            f"   MAXVALUE {sequence['max_value']}{LINEFEED}"
            f"   {'CYCLE' if sequence['cycle'] else 'NOCYCLE'}{LINEFEED}"
            f"   CACHE {sequence['cache']}{END_OF_SENTENCE}"
        )
    sequences_script += get_show_errors()
    return sequences_script


def build_drop_trigger_section(trigger: dict) -> str:
    """
    Builds the section of the script for creating triggers.
    """
    trigger_name = {trigger['name']}
    trigger_script = (
        f"BEGIN{LINEFEED}"
        f"   EXECUTE IMMEDIATE 'DROP TRIGGER UVM.{trigger_name}';{LINEFEED}"
        f"   DBMS_OUTPUT.PUT_LINE('Trigger UVM.{trigger_name} dropped successfully.');{LINEFEED}"
        f"EXCEPTION{LINEFEED}"
        f"   WHEN OTHERS THEN{LINEFEED}"
        f"      IF SQLCODE = -4080 THEN  -- Trigger does not exist{LINEFEED}"
        f"         DBMS_OUTPUT.PUT_LINE('Trigger UVM.{trigger_name} does not exist, skipping...');{LINEFEED}"
        f"      ELSE{LINEFEED}"
        f"         DBMS_OUTPUT.PUT_LINE('Error dropping trigger UVM.{trigger_name}: ' || SQLERRM);{LINEFEED}"
        f"      END IF;{LINEFEED}"
        f"END{END_OF_SENTENCE}")

    trigger_script += get_show_errors_block()
    return trigger_script


def build_trigger_section(trigger: dict) -> str:
    """
    Builds the section of the script for creating triggers.
    """
    trigger_script = (
        f"CREATE OR REPLACE TRIGGER UVM.{trigger['name']}{LINEFEED}"
        f"   {trigger['event']}{LINEFEED}"
        f"   ON {trigger['table']}{LINEFEED}"
        f"   FOR EACH ROW{LINEFEED}"
        f"BEGIN{LINEFEED}"
        f"{trigger['body']}{LINEFEED}"
        f"END{END_OF_SENTENCE}")

    trigger_script += get_show_errors_block()
    return trigger_script


def build_grant_section(grants: list) -> str:
    """
    Builds the section of the script for creating grants.
    """
    grants_script = f"-- Grant permissions{LINEFEED}"
    for grant in grants:
        grants_script += (
            f"{grant}{LINEFEED}"
        )
    grants_script += get_show_errors()
    return grants_script


def build_revoke_section(revoke_grants: str) -> str:
    """
    Builds the section of the script for revoking grants.
    """
    revoke_grants_script = (f"-- Revoke grant permissions{LINEFEED}"
                            f"{revoke_grants}{LINEFEED}"
                            f"{get_show_errors_block()}")
    return revoke_grants_script


def build_synonym_section(synonym: str) -> str:
    """
    Builds the section of the script for creating synonym.
    """
    synonym_script = f"-- Create synonyms{LINEFEED}"
    synonym_script += (
        f"{synonym}{LINEFEED}"
        f"{get_show_errors()}"
    )
    return synonym_script


def build_drop_synonym_section(drop_synonyms: str) -> str:
    """
    Builds the section of the script for drop synonym.
    """
    drop_synonym_script = (f"-- Drop synonyms{LINEFEED}"
                           f"{drop_synonyms}{LINEFEED}")
    return drop_synonym_script


def build_delete_table_script_data(requested_environment: DatabaseEnvironment) -> list[
    dict]:
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.TABLE.value)
    scripts = []

    for key, value in object_data.items():
        if value["name"]:
            drop_elements = []
            table_name = value["name"]
            object_type = value["type"]
            object_owner = value['owner']
            delete_table_section = build_delete_table_section(value)

            drop_index_section = build_indexes_and_primary_key_drop_section(value.get("indexes", {}),
                                                                            object_owner,
                                                                            table_name)

            custom_revoke_section = build_revoke_section(revoke_grants=value.get("revokes"))
            custom_drop_synonym_section = build_drop_synonym_section(drop_synonyms=value.get("drop_synonyms"))

            # Start with the fixed parts of the filename
            filename_parts = [f"{SqlScriptFilenamePrefix.DELETE_TABLE.value}{table_name}", object_owner, "TBL"]

            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            header_section = build_header_section(filename)
            footer_section = build_footer_section(filename)

            script = (f"{header_section}"
                      f"{LINEFEED}"
                      f"{custom_drop_synonym_section}"
                      f"{LINEFEED}"
                      f"{drop_index_section}"
                      f"{LINEFEED}"
                      f"{custom_revoke_section}"
                      f"{LINEFEED}"
                      f"{delete_table_section}"
                      f"{LINEFEED}"
                      f"{footer_section}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def create_formatted_setup_filename_prefix(index: int, priority: int) -> str:
    """
    Creates a formatted string in the format XYYY where:
    - X is the priority (1-9)
    - YYY is the index (0-100) left-padded with zeros to 3 digits.

    Args:
        index: An integer between 0 and 100.
        priority: An integer between 1 and 9.

    Returns:
        A string in the format XYYY (e.g., "2004" for priority=2, index=4).
    """
    if not 0 <= index <= 100:
        raise ValueError("Index must be between 0 and 100.")
    if not 1 <= priority <= 9:
        raise ValueError("Priority must be between 1 and 9.")

    # Format index as 3 digits with leading zeros
    formatted_index = f"{index:03d}"
    return f"{priority}{formatted_index}"


def build_create_setup_table_script_data(requested_environment: DatabaseEnvironment) -> list[dict]:
    object_data = get_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.TABLE.value)
    scripts = []
    index = 0
    for key, value in object_data.items():
        if value["name"]:
            custom = value["custom"]
            object_status = value["object_status"]
            if custom:
                continue
            table_name = value["name"]
            object_type = value["type"]
            object_owner = value['owner']

            create_table_section = build_create_table_section(value)
            create_setup_grants = read_custom_data(b9_object_name=table_name,
                                                   object_addon_type=ObjectAddonType.SETUP_GRANTS,
                                                   grant_type=GrantType.TABLE,
                                                   b9_object_owner=object_owner)
            grant_section = build_grant_section(create_setup_grants["grants"])

            create_setup_synonyms = read_custom_data(b9_object_name=table_name,
                                                     object_addon_type=ObjectAddonType.SETUP_SYNONYMS,
                                                     b9_object_owner=object_owner)

            # Start with the fixed parts of the filename
            filename_prefix = create_formatted_setup_filename_prefix(priority=1, index=index)
            index += 1
            filename_parts = [f"{filename_prefix}_{SqlScriptFilenamePrefix.CREATE_SETUP_TABLE.value}{table_name}",
                              object_owner, "TBL"]

            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            script = (f"{create_table_section}"
                      f"{LINEFEED}"
                      f"{grant_section}"
                      f"{LINEFEED}"
                      f"{create_setup_synonyms}"
                      f"{LINEFEED}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def build_create_table_script_data(requested_environment: DatabaseEnvironment) -> list[
    dict]:
    """
    :param requested_environment:
    :return:
    """
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.TABLE.value)
    scripts = []

    for key, value in object_data.items():
        if value["name"]:
            drop_elements = []
            table_name = value["name"]
            object_type = value["type"]
            object_owner = value['owner']
            create_table_section = build_create_table_section(value)
            if create_table_section:
                drop_elements.append({"type": {object_type},
                                      "owner": {object_owner},
                                      "table": {table_name},
                                      "name": {table_name}})
            tablespace_section = build_tablespace_section(value.get("attributes", {}))
            comments_section = build_comments_section(
                value.get("comments", {}), object_owner, table_name
            )
            index_section = build_indexes_and_primary_key_section(value.get("indexes", {}),
                                                                  object_owner,
                                                                  table_name)

            custom_grant_section = build_grant_section(grants=value.get("grants", {}))
            custom_synonym_section = build_synonym_section(synonym=value.get("synonyms"))
            if custom_synonym_section:
                drop_elements.append({"type": {DatabaseObject.SYNONYM.name},
                                      "owner": {object_owner},
                                      "table": {table_name},
                                      "name": {table_name}})
            # Start with the fixed parts of the filename
            filename_parts = [f"{SqlScriptFilenamePrefix.CREATE_TABLE.value}{table_name}", object_owner, "TBL"]

            # Conditionally add "IDX", "SEQ", and "TR" based on sections
            if index_section.strip():  # Ensure there's meaningful content in the index section
                filename_parts.append("IDX")
            if custom_grant_section.strip():
                filename_parts.append("GNT")
            if custom_synonym_section.strip():
                filename_parts.append("SYN")

            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            header_section = build_header_section(filename)
            drop_object_section = build_drop_section(drop_elements)
            footer_section = build_footer_section(filename)

            script = (f"{header_section}"
                      f"{LINEFEED}"
                      f"{drop_object_section}"
                      f"{LINEFEED}"
                      f"{create_table_section}"
                      f"{LINEFEED}"
                      f"{comments_section}"
                      f"{LINEFEED}"
                      f"{index_section}"
                      f"{LINEFEED}"
                      f"{custom_grant_section}"
                      f"{LINEFEED}"
                      f"{custom_synonym_section}"
                      f"{LINEFEED}"
                      f"{footer_section}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def build_indexes_and_primary_key_drop_section(indexes: list, table_owner: str, table_name: str,
                                               primary_key: dict = None) -> str:
    """
    Generates the SQL script for dropping primary keys and indexes based on the JSON definition.

    :param indexes: List of index definitions from the JSON file.
    :param table_owner: Owner of the table.
    :param table_name: Name of the table.
    :param primary_key: Dictionary containing primary key definition (optional).
    :return: drop SQL script string.
    """
    script_parts = []

    # Add primary key drop if defined
    if primary_key and not primary_key["name"].startswith("SYS_"):
        pk_drop = f"ALTER TABLE {table_owner}.{table_name} DROP CONSTRAINT {primary_key['name']}{END_OF_SENTENCE}"
        script_parts.append(f"-- Drop primary key\n\n{pk_drop}")

    # Process indexes
    index_scripts = []
    for index in indexes:
        # Skip system-generated indexes
        if index["name"].startswith("SYS_"):
            continue

        # Build the DROP INDEX statement
        index_scripts.append(f"DROP INDEX {table_owner}.{index['name']}{END_OF_SENTENCE}")

    # Add the index scripts if any
    if index_scripts:
        script_parts.append("-- Drop indexes\n\n" + "\n".join(index_scripts))

    # Join all script parts with double line breaks
    return "\n\n".join(script_parts) if script_parts else ""


def build_indexes_and_primary_key_section(indexes: list, table_owner: str, table_name: str) -> str:
    """
    Generates the SQL script for primary keys and indexes based on the JSON definition.

    :param indexes: List of index definitions from the JSON file.
    :param table_owner: Owner of the table.
    :param table_name: Name of the table.
    :return: SQL script string.
    """
    script = ""
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
            f"ON {table_owner}.{table_name}{LINEFEED}"
            f"       ({columns_str}){END_OF_SENTENCE}"
            # f"       LOGGING{LINEFEED}"
            # f"       TABLESPACE {index['tablespace']}{LINEFEED}"
            # f"       PCTFREE    {index.get('pct_free', 10)}{LINEFEED}"
            # f"       INITRANS   {index.get('ini_trans', 2)}{LINEFEED}"
            # f"       MAXTRANS   {index.get('max_trans', 255)}{LINEFEED}"
            # f"       STORAGE    ({LINEFEED}"
            # f"                   INITIAL          65536{LINEFEED}"
            # f"                   NEXT             1048576{LINEFEED}"
            # f"                   MINEXTENTS       1{LINEFEED}"
            # f"                   MAXEXTENTS       UNLIMITED{LINEFEED}"
            # f"                   PCTINCREASE      0{LINEFEED}"
            # f"                   BUFFER_POOL      DEFAULT{LINEFEED}"
            # f"                  ){LINEFEED}"
            # f"    NOPARALLEL"
            # f"{END_OF_SENTENCE}"
        )
        index_scripts.append(index_script)

    # Add the primary key constraint if defined
    if primary_key:
        pk_columns = ", ".join([col["column_name"] for col in primary_key["columns"]])
        pk_name = primary_key["name"]
        script += f"-- Primary Key{LINEFEED}{LINEFEED}"
        script += f"ALTER TABLE {table_owner}.{table_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns}){END_OF_SENTENCE}{LINEFEED}"

    # Add the index scripts
    if index_scripts:
        script += "-- Indices\n\n" + "\n".join(index_scripts)

    script += get_show_errors()
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


def create_setup_table_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: create test table script generator")
    scripts_data = build_create_setup_table_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.SETUP.value)
    logging.info("Ending: create test table script generator")


def create_table_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: create table script generator")
    scripts_data = build_create_table_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.INSTALL.value)
    logging.info("Ending: create table script generator")


def delete_table_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: delete table script generator")
    scripts_data = build_delete_table_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.ROLLBACK.value)
    logging.info("Ending: delete table script generator")


def get_scripts_folder_path() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, SCRIPT_FOLDER_PATH)
    return source_folder


def build_delete_sequences_script_data(requested_environment: DatabaseEnvironment) -> \
        list[dict]:
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.SEQUENCE.value)
    scripts = []

    for key, value in object_data.items():
        if value["name"]:
            drop_elements = []
            object_name = value["name"]
            object_type = value["type"]
            object_owner = value['owner']

            sequences_script = (
                f"-- Drop Sequence{LINEFEED}"
                f"DROP SEQUENCE UVM.{value['name']}{END_OF_SENTENCE}"
            )

            filename_parts = [f"{SqlScriptFilenamePrefix.DELETE_SEQUENCE.value}{object_name}", object_owner, "SEQ"]

            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            header_section = build_header_section(filename)

            revoke = build_revoke_section(revoke_grants=value["revokes"])
            drop_synonym = build_drop_synonym_section(drop_synonyms=value["synonyms"])
            footer_section = build_footer_section(filename)

            script = (f"{header_section}"
                      f"{LINEFEED}"
                      f"{drop_synonym}"
                      f"{LINEFEED}"
                      f"{revoke}"
                      f"{LINEFEED}"
                      f"{sequences_script}"
                      f"{LINEFEED}"
                      f"{footer_section}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def build_create_sequences_script_data(requested_environment: DatabaseEnvironment) -> \
        list[dict]:
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.SEQUENCE.value)
    scripts = []

    for key, value in object_data.items():
        if value["name"]:
            drop_elements = []
            object_name = value["name"]
            object_type = value["type"]
            object_owner = value['owner']

            sequences_script = (f"-- Secuencia{LINEFEED}"
                                f"{LINEFEED}")

            sequences_script += (
                f"CREATE SEQUENCE UVM.{value['name']}{LINEFEED}"
                f"   INCREMENT BY {value['increment_by']}{LINEFEED}"
                f"   START WITH {value['min_value']}{LINEFEED}"
                f"   MAXVALUE {value['max_value']}{LINEFEED}"
                f"   {'CYCLE' if value['cycle_flag'] else 'NOCYCLE'}{LINEFEED}"
                f"   CACHE {value['cache_size']}"
                f"{END_OF_SENTENCE}"
            )

            ## CREATE COMMENTED DROP SEQUENCE STATEMENT FOR THIS SEQUENCE OBJECT
            drop_elements.append({"type": DatabaseObject.SEQUENCE.name,
                                  "owner": {object_owner},
                                  "table": None,
                                  "name": value.get("name")})

            sequences_script += get_show_errors()
            sequences_script += LINEFEED

            filename_parts = [f"{SqlScriptFilenamePrefix.CREATE_SEQUENCE.value}{object_name}", object_owner, "SEQ",
                              "GNT",
                              "SYN"]
            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            header_section = build_header_section(filename)

            drop_object_section = f"-- Eliminaciones{LINEFEED}{LINEFEED}"
            for drop_element in drop_elements:
                drop_object_section += f"-- DROP SEQUENCE {drop_element["name"]}{END_OF_SENTENCE}"

            grants = build_grant_section(grants=value["grants"])
            synonym = build_synonym_section(synonym=value["synonyms"])
            footer_section = build_footer_section(filename)

            script = (f"{header_section}"
                      f"{LINEFEED}"
                      f"{drop_object_section}"
                      f"{LINEFEED}"
                      f"{sequences_script}"
                      f"{LINEFEED}"
                      f"{grants}"
                      f"{LINEFEED}"
                      f"{synonym}"
                      f"{LINEFEED}"
                      f"{footer_section}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def create_sequence_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: create sequence script generator")
    scripts_data = build_create_sequences_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.INSTALL.value)
    logging.info("Ending: create sequence script generator")


def delete_sequence_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: delete sequence script generator")
    scripts_data = build_delete_sequences_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.ROLLBACK.value)
    logging.info("Ending: delete sequence script generator")


def replace_package_header(text, package_name, owner):
    pattern = re.compile(rf'.*?\b{re.escape(package_name)}\b', re.IGNORECASE)
    match = pattern.search(text)

    if match:
        start_pos = match.end() - len(package_name)
        new_text = text[start_pos:]
        new_text = f"CREATE OR REPLACE PACKAGE {owner}.{new_text}"

        return new_text
    else:
        return text


def build_delete_package_script_data(requested_environment: DatabaseEnvironment):
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.PACKAGE.value)

    scripts = []
    source_folder_path = get_source_code_folder(database_environment=requested_environment)

    # Get the directory of the current script (b9_sql_script_file.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the workfiles/b9_scripts folder
    workfiles_dir = os.path.join(script_dir, source_folder_path)

    # Normalize the path to resolve '..' and other relative components
    workfiles_dir = os.path.normpath(workfiles_dir)

    ## loop object_data get file path for PACKAGE SPECS
    for key, value in object_data.items():
        package_owner = value.get("owner")
        package_name = value.get("name")
        root = "NONE"

        ## create PACKAGE filename
        ## "CREATE_PACKAGE_nameOfPackage.{package_owner}.{PK}.{GNT}.{SYN}.sql

        filename_parts = [f"{SqlScriptFilenamePrefix.DELETE_PACKAGE.value}{package_name}", package_owner, "PK"]

        # Join all parts with a dot and add the file extension
        filename = ".".join(filename_parts) + ".sql"

        ## HEADER SECTION
        header_section = build_header_section(filename)

        ## DROP SECTION
        drop_object_section = f"-- Drop package{LINEFEED}{LINEFEED}"
        drop_object_section += f"DROP PACKAGE {package_name}{END_OF_SENTENCE}"

        ## FOOTER SECTION
        footer_section = build_footer_section(filename)

        script = (f"{header_section}"
                  f"{LINEFEED}"
                  f"{drop_object_section}"
                  f"{LINEFEED}"
                  f"{footer_section}")

        scripts.append({
            "file_name": filename,
            "script": script
        })

    return scripts


def build_create_setup_package_script_data(requested_environment: DatabaseEnvironment):
    object_data = get_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.PACKAGE.value)

    scripts = []
    source_folder_path = get_source_code_folder(database_environment=requested_environment)

    # Get the directory of the current script (b9_sql_script_file.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the workfiles/b9_scripts folder
    workfiles_dir = os.path.join(script_dir, source_folder_path)

    # Normalize the path to resolve '..' and other relative components
    workfiles_dir = os.path.normpath(workfiles_dir)

    ## loop object_data get file path for PACKAGE SPECS
    index = 0
    for key, value in object_data.items():
        object_status = value.get("object_status")
        if object_status != ObjectTargetType.SKIP.value:
            continue

        package_owner = value.get("owner")
        package_name = value.get("name")
        root = "NONE"

        ## search {owner}.{package_name}.NONE.sql
        package_specification_file_name = f"{package_owner}.{package_name}.{root}.sql"
        package_specificacion_file_path = os.path.join(workfiles_dir, package_specification_file_name)

        # Read package specification file
        package_specification_list = [f"---------------------------- "
                                      f"CREATE PACKAGE {package_name} SPECIFICATION"
                                      f"---------------------------- "]

        with open(package_specificacion_file_path, 'r', encoding='utf-8') as file:
            package_specification_list.append(LINEFEED)
            package_specification_list.append(
                replace_package_header(text=file.read(), package_name=package_name, owner=package_owner))
            package_specification_list.append(LINEFEED)
            package_specification_list.append(get_show_errors_block())
            package_specification_list.append(LINEFEED)

        # Initialize package body
        package_body_list = [f"---------------------------- "
                             f"CREATE PACKAGE {package_name} BODY"
                             f"---------------------------- ", LINEFEED,
                             f"CREATE OR REPLACE PACKAGE {package_name} BODY IS", LINEFEED]

        dependencies = value.get("dependencies")
        for function in dependencies.get("functions"):
            function_name = function.get("name")
            function_file_name = f"{package_owner}.{package_name}.{function_name}.sql"
            function_file_path = os.path.join(workfiles_dir, function_file_name)

            with open(function_file_path, 'r', encoding='utf-8') as file:
                package_body_list.append(f"-- FUNCTION {function_name}")
                package_body_list.append(LINEFEED)
                package_body_list.append(file.read())
                package_body_list.append(LINEFEED)

        for procedure in dependencies.get("procedures"):
            procedure_name = procedure.get("name")
            procedure_file_name = f"{package_owner}.{package_name}.{procedure_name}.sql"
            procedure_file_path = os.path.join(workfiles_dir, procedure_file_name)

            try:
                with open(procedure_file_path, 'r', encoding='utf-8') as file:
                    package_body_list.append(f"-- PROCEDURE {procedure_name}")
                    package_body_list.append(LINEFEED)
                    package_body_list.append(file.read())
                    package_body_list.append(LINEFEED)
            except UnicodeError as e:
                logging.info(f"Procedure {procedure_name} has an undefined character {e}")

        package_body_list.append(f"END {package_name};")
        package_body_list.append(LINEFEED)
        package_body_list.append(get_show_errors_block())

        # Start with the fixed parts of the filename
        filename_prefix = create_formatted_setup_filename_prefix(priority=9, index=index)
        index += 1
        filename_parts = [f"{filename_prefix}_{SqlScriptFilenamePrefix.CREATE_SETUP_PACKAGE.value}{package_name}",
                          package_owner, "PK"]

        # Join all parts with a dot and add the file extension
        filename = ".".join(filename_parts) + ".sql"

        ## PACKAGE SPECIFICATION SECTION
        package_specifications = ''.join(
            one_package_specification for one_package_specification in package_specification_list)

        ## PACKAGE BODY SECTION
        package_body = ''.join(one_package_body for one_package_body in package_body_list)

        script = (f"{package_specifications}"
                  f"{LINEFEED}"
                  f"{package_body}"
                  f"{LINEFEED}"
                  f"{get_package_body_closing_statement(package_name)}")

        scripts.append({
            "file_name": filename,
            "script": script
        })

    return scripts


def build_create_package_script_data(requested_environment: DatabaseEnvironment):
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.PACKAGE.value)

    scripts = []
    source_folder_path = get_source_code_folder(database_environment=requested_environment)

    # Get the directory of the current script (b9_sql_script_file.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Navigate to the workfiles/b9_scripts folder
    workfiles_dir = os.path.join(script_dir, source_folder_path)

    # Normalize the path to resolve '..' and other relative components
    workfiles_dir = os.path.normpath(workfiles_dir)

    ## loop object_data get file path for PACKAGE SPECS
    for key, value in object_data.items():
        package_owner = value.get("owner")
        package_name = value.get("name")
        root = "NONE"
        ## search {owner}.{package_name}.NONE.sql
        package_specification_file_name = f"{package_owner}.{package_name}.{root}.sql"
        package_specificacion_file_path = os.path.join(workfiles_dir, package_specification_file_name)

        # Read package specification file
        package_specification_list = [f"---------------------------- "
                                      f"CREATE PACKAGE {package_name} SPECIFICATION"
                                      f"---------------------------- "]

        with open(package_specificacion_file_path, 'r', encoding='utf-8') as file:
            package_specification_list.append(LINEFEED)
            package_specification_list.append(
                replace_package_header(text=file.read(), package_name=package_name, owner=package_owner))
            package_specification_list.append(LINEFEED)
            package_specification_list.append(get_show_errors_block())
            package_specification_list.append(LINEFEED)

        # Initialize package body
        package_body_list = [f"---------------------------- "
                             f"CREATE PACKAGE {package_name} BODY"
                             f"---------------------------- "]

        package_body_list.append(LINEFEED)
        package_body_list.append(f"CREATE OR REPLACE PACKAGE BODY {package_name} AS")
        package_body_list.append(LINEFEED)

        dependencies = value.get("dependencies")
        for function in dependencies.get("functions"):
            function_name = function.get("name")
            function_file_name = f"{package_owner}.{package_name}.{function_name}.sql"
            function_file_path = os.path.join(workfiles_dir, function_file_name)

            with open(function_file_path, 'r', encoding='utf-8') as file:
                package_body_list.append(f"-- FUNCTION {function_name}")
                package_body_list.append(LINEFEED)
                package_body_list.append(file.read())
                package_body_list.append(LINEFEED)

        for procedure in dependencies.get("procedures"):
            procedure_name = procedure.get("name")
            procedure_file_name = f"{package_owner}.{package_name}.{procedure_name}.sql"
            procedure_file_path = os.path.join(workfiles_dir, procedure_file_name)

            try:
                with open(procedure_file_path, 'r', encoding='utf-8') as file:
                    package_body_list.append(f"-- PROCEDURE {procedure_name}")
                    package_body_list.append(LINEFEED)
                    package_body_list.append(file.read())
                    package_body_list.append(LINEFEED)
            except UnicodeError as e:
                logging.info(f"Procedure {procedure_name} has an undefined character {e}")

        package_body_list.append(f"END {package_name};")
        package_body_list.append(LINEFEED)
        package_body_list.append(get_show_errors_block())

        ## create PACKAGE filename
        ## "CREATE_PACKAGE_nameOfPackage.{package_owner}.{PK}.{GNT}.{SYN}.sql

        filename_parts = [f"{SqlScriptFilenamePrefix.CREATE_PACKAGE.value}{package_name}", package_owner, "PK"]

        # Join all parts with a dot and add the file extension
        filename = ".".join(filename_parts) + ".sql"

        ## PACKAGE SPECIFICATION SECTION
        package_specifications = ''.join(
            one_package_specification for one_package_specification in package_specification_list)

        ## PACKAGE BODY SECTION
        package_body = ''.join(one_package_body for one_package_body in package_body_list)

        ## HEADER SECTION
        header_section = build_header_section(filename)

        ## DROP SECTION
        drop_object_section = f"-- REMOVALS {LINEFEED}{LINEFEED}"
        drop_object_section += f"-- DROP PACKAGE {package_name}{END_OF_SENTENCE}"

        ## GRANTS SECTION
        grants = build_grant_section(grants=value["grants"])

        ## SYNONYMS SECTION
        synonyms = build_synonym_section(synonym=value["synonyms"])

        ## FOOTER SECTION
        footer_section = build_footer_section(filename)

        script = (f"{get_package_opening_statement()}"
                  f"{LINEFEED}"
                  f"{header_section}"
                  f"{LINEFEED}"
                  f"{drop_object_section}"
                  f"{LINEFEED}"
                  f"{package_specifications}"
                  f"{LINEFEED}"
                  f"{package_body}"
                  f"{LINEFEED}"
                  f"{get_package_body_closing_statement(package_name)}"
                  f"{LINEFEED}"
                  f"{footer_section}")

        scripts.append({
            "file_name": filename,
            "script": script
        })

    return scripts


def create_setup_package_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: create base packages script generator")
    scripts_data = build_create_setup_package_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.SETUP.value)
    logging.info("Ending: create base packages script generator")


def create_packages_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: create packages script generator")
    scripts_data = build_create_package_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.INSTALL.value)
    logging.info("Starting: create packages script generator")


def delete_packages_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: delete packages script generator")
    scripts_data = build_delete_package_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.ROLLBACK.value)
    logging.info("Starting: delete packages script generator")


def build_delete_trigger_script_data(requested_environment):
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.TRIGGER.value)
    scripts = []

    for key, value in object_data.items():
        if value["name"]:
            drop_elements = []
            trigger_owner = value.get('owner')
            trigger_name = value.get("name")

            custom_trigger_section = build_drop_trigger_section(trigger=value.get("trigger", {}))

            # Start with the fixed parts of the filename
            filename_parts = [f"{SqlScriptFilenamePrefix.DELETE_TRIGGER.value}{trigger_name}", trigger_owner, "TR"]

            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            header_section = build_header_section(filename)
            footer_section = build_footer_section(filename)

            script = (f"{header_section}"
                      f"{LINEFEED}"
                      f"{custom_trigger_section}"
                      f"{LINEFEED}"
                      f"{footer_section}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def build_create_trigger_script_data(requested_environment):
    object_data = get_migrated_object_data_mapped_by_names_by_environment_and_type(
        database_environment=requested_environment,
        object_data_type=ObjectDataTypes.TRIGGER.value)
    scripts = []

    for key, value in object_data.items():
        if value["name"]:
            drop_elements = []
            trigger_owner = value.get('owner')
            trigger_name = value.get("name")

            custom_trigger_section = build_trigger_section(trigger=value.get("trigger", {}))

            # Start with the fixed parts of the filename
            filename_parts = [f"{SqlScriptFilenamePrefix.CREATE_TRIGGER.value}{trigger_name}", trigger_owner, "TR"]

            # Join all parts with a dot and add the file extension
            filename = ".".join(filename_parts) + ".sql"

            header_section = build_header_section(filename)
            drop_object_section = build_drop_section(drop_elements)
            footer_section = build_footer_section(filename)

            script = (f"{header_section}"
                      f"{LINEFEED}"
                      f"{drop_object_section}"
                      f"{LINEFEED}"
                      f"{custom_trigger_section}"
                      f"{LINEFEED}"
                      f"{footer_section}")

            scripts.append({
                "file_name": filename,
                "script": script
            })

    return scripts


def create_trigger_scripts_manager(database_environment: DatabaseEnvironment):
    logging.info("Starting: create trigger script generator")
    scripts_data = build_create_trigger_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.INSTALL.value)
    logging.info("Ending: create trigger script generator")


def delete_trigger_scripts_manager(database_environment=DatabaseEnvironment):
    logging.info("Starting: delete trigger script generator")
    scripts_data = build_delete_trigger_script_data(requested_environment=database_environment)
    _write_script_files(scripts_data=scripts_data, script_type=ScriptType.ROLLBACK.value)
    logging.info("Ending: create trigger script generator")


def _write_script_files(scripts_data, script_type: Optional[str] = ""):
    scripts_folder_path = get_scripts_folder_path()
    for script_info in scripts_data:
        file_name = script_info["file_name"]
        script_content = script_info["script"]
        # Construct the file name
        file_path = os.path.join(scripts_folder_path, script_type, file_name)

        # Write the script to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(script_content)

        logging.info(f"Saved script for table '{file_name}' to '{file_path}'")


if __name__ == "__main__":
    build_create_package_script_data(requested_environment=DatabaseEnvironment.BANNER9)
