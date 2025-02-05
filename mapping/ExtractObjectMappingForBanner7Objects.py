from files.MappingFile import write_mapping_file
from tools.MappingTools import build_mapping_data

if __name__ == "__main__":
    mapping_data = build_mapping_data()
    write_mapping_file(mapping_data=mapping_data)
