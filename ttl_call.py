import os
import io
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from rdflib import Graph
import shutil

plugin_interface_version = 2

storage_account_name = "dlsrdhlandingdev"
sas_token = "sp=racwdlmeop&st=2024-07-11T21:14:54Z&se=2024-12-31T06:14:54Z&spr=https&sv=2022-11-02&sr=d&sig=FzSJ5iSW7mnQUgkmMkEheBCXLAP11EBK3QrUaAs%2F8WU%3D&sdd=1"


def convert_ttl_to_jsonld(ttl_file, out_dir, new_dated_directory = False):
    """
    Convert a Turtle (.ttl) file to JSON-LD format.

    :param ttl_file: Path to the input Turtle file.
    :param jsonld_file: Path to the output JSON-LD file.
    """
    print(f"Converting file {ttl_file}")

    # Create a new directory structure
    base_dir = "/data/source_data/blob"

    try:

        if new_dated_directory:
            # Get the current date and time
            current_datetime = datetime.now()

            # Format the date and time as a string
            folder_name = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

            # Remove everything before the first backslash
            relative_path = out_dir.split('\\', 1)[-1]

            new_directory = os.path.join(base_dir, relative_path, folder_name)

            # Create the JSON_LD subdirectory within the new_directory
            jsonld_directory = os.path.join(new_directory, "JSON_LD")
            os.makedirs(jsonld_directory, exist_ok=True)

        else:

            #Remove everything before the first backslash
            relative_path = out_dir.split('\\', 1)[-1]

            # Create a new directory for JSON-LD files if it doesn't exist
            jsonld_directory = os.path.join(base_dir, relative_path, "JSON_LD")
            os.makedirs(jsonld_directory, exist_ok=True)

    except Exception as e:
        print(f"Error in directory setup: {e}")
        return None

    try:

        # Define the output file path with the same name but with ".jsonld" extension
        jsonld_filename = os.path.splitext(os.path.basename(ttl_file))[0] + ".jsonld"
        jsonld_file_path = os.path.join(jsonld_directory, jsonld_filename)

        # Load the Turtle file
        g = Graph()
        g.parse(ttl_file, format="turtle")

        # Serialize to JSON-LD
        jsonld_output = g.serialize(format="json-ld", indent=4).encode("utf-8")

        # Create a blob client using the local file name as the name for the blob
        account_url = f"https://{storage_account_name}.blob.core.windows.net"
        credential = sas_token
        blob_service_client = BlobServiceClient(account_url, credential=credential)

        container_name = "ontoforce"
        #directory_name = "ontoforce\outbound\TTL"

        #blob_client = blob_service_client.get_blob_client(container=container_name, blob=jsonld_filename)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.join(jsonld_directory, os.path.basename(jsonld_filename)))
        blob_client.upload_blob(io.BytesIO(jsonld_output), overwrite=True)

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

    print("Starting conversion of TTL files to JSON-LD")

    if options_values.get('new_dated_directory', True):
        for filename in os.listdir(input_directory):
            # Check if the file has a .ttl extension
            if filename.endswith(".ttl"):
                # Get the full path of the file
                ttl_file_path = os.path.join(input_directory, filename)

                # Perform the function on the file
                convert_ttl_to_jsonld(ttl_file_path, output_directory, True)
        log("Done")

    else:

        for filename in os.listdir(input_directory):
            # Check if the file has a .ttl extension
            if filename.endswith(".ttl"):
                if filename != "configuration.ttl":
                    # Get the full path of the file
                    ttl_file_path = os.path.join(input_directory, filename)

                    # Perform the function on the file
                    convert_ttl_to_jsonld(ttl_file_path, output_directory)





