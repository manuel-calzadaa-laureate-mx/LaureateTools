from tools.ExtractTools import export_data_to_object_json

if __name__ == "__main__":

    # Find all the elements for the procedures
    input = "../dependencies.out"
    output = "../../object_data.json"
    export_data_to_object_json(input, output)

