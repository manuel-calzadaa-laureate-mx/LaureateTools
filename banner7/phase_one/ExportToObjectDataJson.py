from tools.ExtractTools import convert_dependencies_file_to_json_object
from tools.FileTools import write_json_to_file

if __name__ == "__main__":
    # Find all the elements for the procedures
    input_file = "../dependencies.out"
    output_file = "../../object_data.json"
    json_object = convert_dependencies_file_to_json_object(input_filename=input_file)

    write_json_to_file(json_data=json_object, output_filename=output_file)

