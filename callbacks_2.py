import os
from datetime import datetime
from rdflib import Graph
import shutil

plugin_interface_version = 2



def create_blank_files(ttl, input_dir):
    try:
        # Create a new directory for JSON-LD files if it doesn't exist
        jsonld_directory = os.path.join(input_dir, "JSON_LD")
        os.makedirs(jsonld_directory, exist_ok=True)

        # Define the output file path with the same name but with ".jsonld" extension
        jsonld_filename = os.path.splitext(os.path.basename(ttl))[0] + ".jsonld"
        jsonld_file_path = os.path.join(jsonld_directory, jsonld_filename)

        # Create an empty JSON-LD file
        with open(jsonld_file_path, "w") as f:
            pass  # Just create the file without writing anything to it

        print(f"Created blank JSON-LD file: {jsonld_file_path}")
    except Exception as e:
        print(f"Error creating JSON-LD file for {ttl}: {e}")


def convert_ttl_to_jsonld(ttl_file, dir):
    """
    Convert a Turtle (.ttl) file to JSON-LD format.

    :param ttl_file: Path to the input Turtle file.
    :param jsonld_file: Path to the output JSON-LD file.
    """
    print(f"Converting file {ttl_file}")
    try:
        # Create a new directory for JSON-LD files if it doesn't exist
        jsonld_directory = os.path.join(dir, "JSON_LD")
        os.makedirs(jsonld_directory, exist_ok=True)

        # Define the output file path with the same name but with ".jsonld" extension
        jsonld_filename = os.path.splitext(os.path.basename(ttl_file))[0] + ".jsonld"
        jsonld_file_path = os.path.join(jsonld_directory, jsonld_filename)

        # Load the Turtle file
        g = Graph()
        g.parse(ttl_file, format="turtle")

        # Serialize to JSON-LD
        jsonld_output = g.serialize(format="json-ld", indent=4)

        # Write the JSON-LD to a file
        with open(jsonld_file_path, "w") as f:
            f.write(jsonld_output)

        print(f"{ttl_file} has been converted to json-ld at {jsonld_file_path}.")
        return jsonld_file_path

    except Exception as e:
        print(f"Error converting {ttl_file}: {e}")
        return None

def meta_data():
    return {
        'id': 'ttl_to_json_ld',
        'name': 'TTL to JSON-LD Converter',
        'description': 'Convert exported RDF TTL files to JSON-LD',
        'type-id': 'event-processor',
    }


def session_options_definition():
    return [
        {
            'content_type': 'bool',
            'id': 'new_dated_directory',
            'name': 'Save to new directory',
            'description': 'Enable this if you would like to save the converted files to a new directory labeled with date and time. Default is set to overwrite the current files.',
            'default': True,
        },
        {
            'content_type': 'bool',
            'id': 'create_blanks',
            'name': 'Create empty jsonld files',
            'description': 'Creates empty files for the converted ones to overwite',
            'default': True,
        },
        {
            'content_type': 'text',
            'id': 'input_directory',
            'name': 'Input Directory',
            'description': 'Name of the TTL Export directory.'
        },
        {
            'content_type': 'text',
            'id': 'output_directory',
            'name': 'Output Directory',
            'description': 'Name of the JSON-LD Output directory.'
        }
    ]

def process_events(events,
                   options_values,
                   log,
                   **kwargs
                   ):

    input_directory = options_values['input_directory']
    output_directory = options_values['output_directory']

    if options_values.get("create_blanks", True):
        for filename in os.listdir(input_directory):
            # Check if the file has a .ttl extension
            if filename.endswith(".ttl"):
                if filename != "configuration.ttl":
                    # Get the full path of the file
                    ttl_file_path = os.path.join(input_directory, filename)

                    # Perform the function on the file
                    create_blank_files(ttl_file_path, input_directory)

    print("Starting conversion of TTL files to JSON-LD")
    if options_values.get('new_dated_directory', True):

        # Get the current date and time
        current_datetime = datetime.now()

        # Format the date and time as a string
        folder_name = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

        # Create a new directory with the formatted date and time
        new_directory = os.path.join(input_directory, folder_name)
        os.makedirs(new_directory, exist_ok=True)

        for filename in os.listdir(input_directory):
            # Check if the file has a .ttl extension
            if filename.endswith(".ttl"):
                # Get the full path of the file
                ttl_file_path = os.path.join(input_directory, filename)

                # Perform the function on the file
                convert_ttl_to_jsonld(ttl_file_path, new_directory)
        log("Done")
    else:
        for filename in os.listdir(input_directory):
            # Check if the file has a .ttl extension
            if filename.endswith(".ttl"):
                if filename != "configuration.ttl":
                    # Get the full path of the file
                    ttl_file_path = os.path.join(input_directory, filename)

                    # Perform the function on the file
                    jsonld_fpath = convert_ttl_to_jsonld(ttl_file_path, input_directory)

                    if jsonld_fpath:  # Only copy the file if conversion was successful
                        # Copy the new converted file to the destination directory
                        new_file_name = os.path.basename(jsonld_fpath)
                        destination_path = os.path.join(output_directory, new_file_name)
                        shutil.copy(jsonld_fpath, destination_path)
                        print(f"Copied {jsonld_fpath} to {destination_path}")

                        # Remove the original file if it was copied successfully
                        if os.path.exists(destination_path):
                            os.remove(jsonld_fpath)
                            print(f"Removed the original file {jsonld_fpath}")


