from tools.ExtractTools import process_source_folder

if __name__ == "__main__":

    # Find all the elements for the procedures
    output_csv = "../dependencies.out"
    source_code_output = "src"
    process_source_folder(source_code_output, output_csv)