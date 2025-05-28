from disqover import *
import time
import os
import json
import csv
from datetime import datetime

# Function to read CSV and generate choices
def get_use_case_choices(csv_file_path):
    choices = []
    with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            use_case_name = row[0]
            #use_case_id = use_case_name.lower().replace(' ', '_')  # Convert name to lowercase and replace spaces
            team_uri = row.get('teamuri','').strip()
            organization = row.get('organization', '').strip()
            choices.append({
                'name': use_case_name,
                'teamuri': team_uri,
                'organization': organization
            })
    return choices

plugin_interface_version = 2

#Meta_data

def meta_data():
    return {
        'id': 'cohort_builder',
        'name': 'Cohort Builder',
        'description': 'Creates a cohort based off instances and exports as JSON.',
        'type-id': 'instance-processor',
    }


# def global_options_definition():
#     return [
#         {
#             'content_type': 'text',
#             'id': 'ct_uri',
#             'name': 'Canonical type',
#             'default': '',
#             'description': 'You can optionally specify a canonical type uri to filter the results of the plugin. This improves the speed of the plugin.'
#         }
#     ]

#Set user for protected access
def override_user_credentials() -> dict[str, str]:
    return {"username": "plugin@ontoforce.com", "password": "adminplugin"}

#Session Options
def session_options_definition():
    # Path to your CSV file containing use case names
    csv_file_path = '/plugin/external/cohort_builder/use_case/use_case.csv'  # Update this path as needed

    # Get the dynamic choices from the CSV
    use_case_choices = get_use_case_choices(csv_file_path)
    use_case_list = [choice['name'] for choice in use_case_choices]

    return [
        {
            'content_type': 'single_choice',
            'id': 'use_case',
            'name': 'Use Case',
            'description': 'Please select the use case.',
            'choices': use_case_list
        },
        {
            'content_type': 'text',
            'id': 'username',
            'name': 'Name',
            'description': 'Please enter your full name.'
        },
        {
            'content_type': 'text',
            'id': 'dq_url',
            'name': 'DISQOVER URL',
            'description': 'Please enter the DISQOVER search URL.'
        }
    ]

#Plugin Code


def process_instances(ct_uri, property_uris, instances, options_values, session_id, log, **kwargs):

    #Save session option values for form
    use_case = options_values['use_case']
    username = options_values['username']
    search_url = options_values['dq_url']

    #header info
    team_uri = None
    organization = None

    for choice in use_case_choices:
        if choice['name'] == use_case:
            team_uri = choice['teamuri']
            organization = choice['organization']
            break


    # Get the current date and time as part of the filename
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{current_time}.json"

    # Set the export directory
    export_dir = "/plugin/external/cohort_builder"
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    # Define the path for the local file with the current date and time
    export_path = os.path.join(export_dir, filename)

    # Property keys
    studyid_key = "http://data.boehringer.com/ontology/property/clinical_subject/clinical_study_identifier"
    subjid_key = "http://ns.ontoforce.com/2013/disqover#preferredLabel"

    # Collect cohort data by STUDYID


    cohort_selection = []
    for instance_uri, properties in instances.items():
        study_ids = properties.get(studyid_key, [])
        subjids = properties.get(subjid_key, [])
        print(subjids)

    # Format cohort data into list
        if study_ids or subjids:
            cohort_selection.append({
                "STUDYID": study_ids,
                "SUBJID:": subjids})

    # Write to file
    with open(export_path, 'w') as stream:
        # Write header (line 1)
        header = {
            'Use Case': use_case,
            'Teamuri': team_uri,
            'Organization': organization,
            'Requested by': username,
            'DISQOVER URL' : search_url
        }
        stream.write(json.dumps(header) + '\n')

        # Write COHORT_SELECTION block (line 2)
        result = {
            "COHORT_SELECTION": cohort_selection
        }
        json.dump(result, stream, indent=2)

    log(f"Exported {len(cohort_selection)} cohort entries to {export_path}")

    return {
        'message': 'File exported and saved locally.',
        'file_location': export_path
    }

