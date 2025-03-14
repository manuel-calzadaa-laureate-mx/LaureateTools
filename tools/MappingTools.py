from enum import Enum
from itertools import chain

from db.DatabaseProperties import DatabaseEnvironment
from db.OracleDatabaseTools import OracleDBConnectionPool
from db.datasource.MappingDatasource import query_mapping_table, query_mapping_table_by_object_type
from files.B7ObjectDataFile import get_object_data_mapped_by_names_by_environment


class MappingObjectTypes(Enum):
    TRIGGER = 'TRIGGER'
    PACKAGE = 'PACKAGE'
    PROCEDURE = 'PROCEDURE'
    FUNCTION = 'FUNCTION'
    SEQUENCE = 'SEQUENCE'
    TYPE = 'TYPE'
    VIEW = 'VIEW'
    TABLE = 'TABLE'


def get_mapping_data_for_one_type_mapped_by_b7_object_name(mapping_object_type: MappingObjectTypes) -> dict:
    mapped_data = {}
    if mapping_object_type:
        mapping_records = query_mapping_table_by_object_type(mapping_object_type.value)
        for mapping_record in mapping_records:
            b7_name = mapping_record.get('GZTBTMPEO_B7_NOMBRE', '')
            if b7_name:
                mapped_data[b7_name] = mapping_record
        return mapped_data
    return {}


def _get_mapping_data_mapped_by_b7_object_name(db_pool: OracleDBConnectionPool) -> dict:
    mapped_data = {}
    mapping_records = query_mapping_table(db_pool=db_pool)
    for mapping_record in mapping_records:
        b7_name = mapping_record.get('GZTBTMPEO_B7_NOMBRE', '')
        if b7_name:
            mapped_data[b7_name] = mapping_record
    return mapped_data


def _get_mapping_data_mapped_by_b9_object_name(db_pool: OracleDBConnectionPool) -> dict:
    mapped_data = {}
    mapping_records = query_mapping_table(db_pool=db_pool)
    for mapping_record in mapping_records:
        b9_name = mapping_record.get('GZTBTMPEO_B9_NOMBRE', '')
        if b9_name:
            mapped_data[b9_name] = mapping_record
    return mapped_data


def _extract_banner7_mapping_data(db_pool: OracleDBConnectionPool):
    object_data_mapped_by_names = get_object_data_mapped_by_names_by_environment(
        database_environment=DatabaseEnvironment.BANNER7)
    mapping_records_mapped_by_b7_names = _get_mapping_data_mapped_by_b7_object_name(db_pool=db_pool)

    mapping_data = []

    for object_data_name in object_data_mapped_by_names:
        mapping_record = mapping_records_mapped_by_b7_names.get(object_data_name, {})

        if mapping_record:
            mapping_data.append({
                "IS_MAPPED": "true",
                "B7_TIPO": mapping_record.get("GZTBTMPEO_B7_TIPO", ""),
                "B7_ESQUEMA": mapping_record.get("GZTBTMPEO_B7_ESQUEMA", ""),
                "B7_PAQUETE": normalize_value(mapping_record.get("GZTBTMPEO_B7_PAQUETE", "")),
                "B7_NOMBRE": mapping_record.get("GZTBTMPEO_B7_NOMBRE", ""),
                "B9_TIPO": mapping_record.get("GZTBTMPEO_B9_TIPO", ""),
                "B9_ESQUEMA": mapping_record.get("GZTBTMPEO_B9_ESQUEMA", ""),
                "B9_PAQUETE": normalize_value(mapping_record.get("GZTBTMPEO_B9_PAQUETE", "")),
                "B9_NOMBRE": normalize_value(mapping_record.get("GZTBTMPEO_B9_NOMBRE", ""))
            })
        else:
            object_data = object_data_mapped_by_names[object_data_name]
            mapping_data.append({
                "IS_MAPPED": "false",
                "B7_TIPO": object_data.get("type", None),
                "B7_ESQUEMA": object_data.get("owner", ""),
                "B7_PAQUETE": normalize_value(object_data.get("package", "")),
                "B7_NOMBRE": object_data.get("name", ""),
                "B9_TIPO": object_data.get("type", ""),
                "B9_ESQUEMA": "UVM",
                "B9_PAQUETE": "none",
                "B9_NOMBRE": "none"
            })

    return mapping_data


def _extract_banner9_mapping_data(db_pool: OracleDBConnectionPool):
    object_data_mapped_by_names = get_object_data_mapped_by_names_by_environment(
        database_environment=DatabaseEnvironment.BANNER9)
    mapping_records_mapped_by_b9_names = _get_mapping_data_mapped_by_b9_object_name(db_pool=db_pool)

    mapping_data = []

    for object_data_name in object_data_mapped_by_names:
        mapping_record = mapping_records_mapped_by_b9_names.get(object_data_name, {})

        if mapping_record:
            mapping_data.append({
                "IS_MAPPED": "true",
                "B7_TIPO": mapping_record.get("GZTBTMPEO_B7_TIPO", ""),
                "B7_ESQUEMA": normalize_value(mapping_record.get("GZTBTMPEO_B7_ESQUEMA", "")),
                "B7_PAQUETE": normalize_value(mapping_record.get("GZTBTMPEO_B7_PAQUETE", "")),
                "B7_NOMBRE": normalize_value(mapping_record.get("GZTBTMPEO_B7_NOMBRE", "")),
                "B9_TIPO": mapping_record.get("GZTBTMPEO_B9_TIPO", ""),
                "B9_ESQUEMA": mapping_record.get("GZTBTMPEO_B9_ESQUEMA", ""),
                "B9_PAQUETE": normalize_value(mapping_record.get("GZTBTMPEO_B9_PAQUETE", "")),
                "B9_NOMBRE": normalize_value(mapping_record.get("GZTBTMPEO_B9_NOMBRE", ""))
            })
        else:
            object_data = object_data_mapped_by_names[object_data_name]
            mapping_data.append({
                "IS_MAPPED": "false",
                "B7_TIPO": object_data.get("type", None),
                "B7_ESQUEMA": "none",
                "B7_PAQUETE": "none",
                "B7_NOMBRE": "none",
                "B9_TIPO": object_data.get("type", ""),
                "B9_ESQUEMA": object_data.get("owner", ""),
                "B9_PAQUETE": normalize_value(object_data.get("package", "")),
                "B9_NOMBRE": object_data.get("name", ""),
            })

    return mapping_data


def build_mapping_data(db_pool: OracleDBConnectionPool) -> list[dict]:
    banner7_mapping_data = _extract_banner7_mapping_data(db_pool=db_pool)
    banner9_mapping_data = _extract_banner9_mapping_data(db_pool=db_pool)
    mapping_data = list(chain(banner7_mapping_data, banner9_mapping_data))
    return mapping_data


def normalize_value(value: str):
    """
    Returns 'none' if the input value is 'N/A' or '[ SE ELIMINA ]',
    otherwise returns the original value.
    """
    if value in {"N/A", "[ SE ELIMINA ]", None, "", 'null'}:
        return "none"
    return value
