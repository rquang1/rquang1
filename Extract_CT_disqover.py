import random
import time
import os
import json
from disqover import InlineCredentialsProvider, DisqoverSession
from disqover import DataQueryAPI
from disqover import FromDataSource, CategoricalFacetParameters, FacetType
from disqover import BelongsToCanonicalType


cred = InlineCredentialsProvider("https://federation.disqover.com/", "customerdatadev@ontoforce.com",
                            "JX03CeRiYXejes/q5GywSQ==")

with DisqoverSession(cred) as session:
    try:
        api = DataQueryAPI(session)
        configuration = api.configuration

        dir_result = "/Users/ryan.quang/Desktop/rex"
        ct_uri = "http://ns.ontoforce.com/ontologies/integration_ontology#ClinicalStudy"
        query = BelongsToCanonicalType(ct_uri)
        
        count = api.get_instances(query=query).get_count()
        print(count)
        size = 1000
        page_end = (count/size)+1
        print(page_end)
        print(int(page_end))
        offset_list = [i for i in range(1, int(page_end), 1)]
        for offset in offset_list:
            print(offset)
            #writefile = os.path.join("","instance_extract_{}.json".format(offset))
            instance_list = []
            for instance in api.get_instances(query, server_side_cursor=False, include_all_properties=True, data_location="remote").get_page(offset, page_size=size):
                export_instance = dict()
                export_instance["uri"]= instance.uri
                export_instance["label"]= instance.label
                export_instance["synonyms"]= instance.synonyms
                export_instance["alternative_uris"]= instance.alternative_uris
                export_instance["properties"] = dict()
                for label, values in instance.properties.items():
                    values_label = list()
                    for value in values:        
                        values_label.append(value.label)
                    export_instance["properties"][label] = values_label
                instance_list.append(export_instance)
            print(offset)

            with open("instance_extract_{}.json".format(offset), 'w') as writefile:
                json.dump(instance_list, writefile, indent=1)

    except Exception as e:
        print(e)






