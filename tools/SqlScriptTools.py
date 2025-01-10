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
