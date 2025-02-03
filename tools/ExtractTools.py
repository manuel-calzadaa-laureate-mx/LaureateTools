import csv
import json
import logging

import cx_Oracle

from db.datasource.TablesDatasource import fetch_table_columns_for_tables_grouped_by_schema_and_table_name, \
    fetch_table_attributes_for_tables_grouped_by_schema_and_table_name, \
    fetch_column_comments_for_tables_grouped_by_schema_and_table_name, \
    fetch_full_indexes_for_tables_grouped_by_schema_and_table_name
from db.datasource.TriggersDatasource import fetch_triggers_for_tables
from tools.BusinessRulesTools import is_custom_table

logging.basicConfig(level=logging.INFO)







# def get_trigger_names_and_status(triggers: dict, schema: str, table_name: str):
#     """
#     Extract trigger names and their statuses for a given owner and table name.
#
#     Args:
#         triggers (dict): The nested dictionary of triggers grouped by owner and table name.
#         schema (str): The schema/owner of the table.
#         table_name (str): The name of the table.
#
#     Returns:
#         list: A list of dictionaries containing trigger names and statuses.
#               Example: [{"trigger_name": "TRIGGER1", "status": "ENABLED"}, ...]
#     """
#     # Get the triggers for the specified owner and table name, defaulting to an empty list
#     table_triggers = triggers.get(schema, {}).get(table_name, [])
#
#     # Extract the trigger name and status for each trigger
#     return [
#         {"name": trigger["trigger_name"], "status": trigger["status"], "deployment": "external"}
#         for trigger in table_triggers
#     ]
