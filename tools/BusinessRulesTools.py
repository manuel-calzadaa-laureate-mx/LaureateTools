def is_custom_table(table_name: str) -> bool:
    return len(table_name.upper()) > 1 and table_name.upper()[1] == 'Z'

def validate_component(component_name: str) -> bool:
    if len(component_name) >= 9:
        if component_name[1].upper() == 'Z':
            return True
    return False


