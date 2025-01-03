from tools.ExtractTools import export_data_to_object_json

if __name__ == "__main__":
    # Find all the elements for the procedures
    input_file = "../dependencies.out"
    output_file = "../../object_data.json"
    export_data_to_object_json(input_filename=input_file, output_filename=output_file)
