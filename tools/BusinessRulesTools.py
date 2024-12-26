def is_custom_table(table_name: str) -> bool:
    return len(table_name.upper()) > 1 and table_name.upper()[1] == 'Z'
