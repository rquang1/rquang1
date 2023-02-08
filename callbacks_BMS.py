import json
from disqover import DataQueryAPI
from disqover import BelongsToCanonicalType
import os
from disqover.type_hints import SegmentOrGUID, DataSourceURI, ResourceTypeURI, PropertyURI, FacetURI, CanonicalTypeURI, \
    PipelineGUID, ComponentGUID, ComponentOrGUID, JSONDict
from disqover.constants import DEFAULT_SCAN_TIMEOUT
from disqover import DataIngestionAPI, FileType, ScanResult, Component, Segment, Pipelines
from disqover import InlineCredentialsProvider, M2MAuthenticator
from disqover import DisqoverSession, FacetDataType, PropertyDataType, FacetViewType, FacetEqual, LinksTo, InstanceURIs
from disqover.data_ingestion.models.canonical_type_component import add_property, add_facet, set_property_as_facet
from typing import List, Dict, Optional, Tuple, Collection
from pathlib import Path
from collections import defaultdict


plugin_interface_version = 2


def start_session(domain, user, password):
    """
    Start the DISQOVER session for the API
    :param domain:
    :param email:
    :param password:
    :return:
    """
    cred_prov = InlineCredentialsProvider(domain, user, password)
    session = DisqoverSession(cred_prov)
    session._authenticator.authenticate(session._http_session, session._logger)
    session._retrieve_application_info()

    return session


def save_ontology(api, ct_uri, config_dir):
    ontology = api.ontology
    data_sources = list()
    data_source_lookup_dict = dict()
    for data_source in ontology.data_sources:
        modification_date_dict = dict()
        data_source_dict = dict()
        if ct_uri in data_source.instance_counts_per_canonical_type.keys():
            data_source_lookup_dict[data_source.uri] = data_source.label
            data_source_dict["uri"] = data_source.uri
            data_source_dict["label"] = data_source.label
            data_source_dict["last_modified"] = data_source.last_modified.strftime("%Y-%m-%d")
            modification_date_dict["date_modified"] = data_source.last_modified.strftime("%Y-%m-%d")
            data_source_dict["description"] = data_source.description
            data_source_dict["homepage"] = data_source.homepage
            data_source_dict["short_label"] = data_source.short_label
            data_source_dict["ct_instance_count"] = data_source.instance_counts_per_canonical_type[ct_uri]
            data_sources.append(data_source_dict)

            with open(os.path.join(config_dir, "modification_date_{}.json".format(data_source.label)), "w") as writefile:
                json.dump(modification_date_dict, writefile, indent=1)

    with open(os.path.join(config_dir, "ct_data_sources.json"), "w") as writefile:
        json.dump(data_sources, writefile, indent=1)

    return data_source_lookup_dict, data_sources


def save_configuration(api, ct_uri, config_dir):
    configuration = api.configuration
    ct_uri_list = list()
    for ct in configuration.canonical_types:
        ct_uri_list.append(ct.uri)

    ct_config = configuration.get_canonical_type(ct_uri)
    ct_info = dict()
    ct_info["uri"] = ct_config.uri
    ct_info["label"] = ct_config.label
    ct_info["icon"] = ct_config.icon
    ct_info["description"] = ct_config.description
    with open(os.path.join(config_dir, "ct_config.json"), "w") as writefile:
        json.dump(ct_info, writefile, indent=1)

    properties = list()
    property_lookup_dict = dict()
    for property in ct_config.properties:
        property_dict = dict()
        property_lookup_dict[property.uri] = property.label
        property_dict["uri"] = property.uri
        property_dict["label"] = property.label
        property_dict["description"] = property.description
        property_dict["renderer"] = property.renderer
        properties.append(property_dict)
    with open(os.path.join(config_dir, "ct_config_properties.json"), "w") as writefile:
        json.dump(properties, writefile, indent=1)

    facets = list()
    for facet in ct_config.facets:
        facet_dict = dict()
        facet_dict["uri"] = facet.uri
        facet_dict["label"] = facet.label
        facet_dict["description"] = facet.description
        facet_dict["facet_type"] = facet.facet_type
        facets.append(facet_dict)
    with open(os.path.join(config_dir, "ct_config_facets.json"), "w") as writefile:
        json.dump(facets, writefile, indent=1)

    return property_lookup_dict, ct_uri_list, ct_info, properties, facets

def download_and_save_instances(api, query, offset, page_size, property_lookup_dict, data_source_lookup_dict, data_dir):
    # Make a single API call to get the instances and the linked instances for each instance
    instances = api.get_instances(query=query, include_all_properties=True).get_page(offset,page_size=page_size)

    # Create a dictionary to store the data for each data source
    data_source_instances_dict = {}

    # Loop over the instances
    for instance in instances:
        # Get the instance properties
        instance_uri = instance.uri
        instance_label = instance.label
        alternative_uri = instance.alternative_uris
        synonyms = instance.synonyms
        properties = {label.split("/")[-1]: [value.label for value in values] for label, values in
                      instance.properties.items()}

        # Loop over the properties of the instance
        for prop_uri, values in instance.properties.items():
            # Check if the property URI is in the lookup dictionary
            if prop_uri in property_lookup_dict:
                # Loop over the values of the property
                for value in values:
                    # Get the data source URI for the value
                    data_source_uri = value.data_source_uri

                    # Check if the data source URI is in the dictionary of data sources
                    if data_source_uri not in data_source_instances_dict:
                        # If not, create a new dictionary for the data source
                        data_source_instances_dict[data_source_uri] = {}

                    # Check if the instance URI is in the dictionary of instances for the data source
                    if instance_uri not in data_source_instances_dict[data_source_uri]:
                        # If not, create a new dictionary for the instance
                        instance_prop_dict = {}

                        # Add the preferred URI, label, and alternative URIs for the instance
                        instance_prop_dict["preferred_uri"] = instance_uri
                        instance_prop_dict["preferred_label"] = instance_label
                        instance_prop_dict["alternative_uris"] = alternative_uri
                        instance_prop_dict["synonyms"] = synonyms
                        instance_prop_dict["properties"] = properties

                        # Add the instance dictionary to the dictionary of instances for the data source
                        data_source_instances_dict[data_source_uri][instance_uri] = instance_prop_dict
                    else:
                        # If the instance URI is already in the dictionary of instances for the data source
                        # Check if the property is already in the dictionary for the instance
                        if property_lookup_dict[prop_uri] in data_source_instances_dict[data_source_uri][instance_uri]:
                            # If the property is already in the dictionary, append the value to the list
                            if value.value == value.label:
                                # If the value and label are the same, append the label as a string
                                data_source_instances_dict[data_source_uri][instance_uri][
                                    property_lookup_dict[prop_uri]].append(value.label)
                            else:
                                # If the value and label are different, create a dictionary with the URI and label and append it to the list
                                property_instance = {"uri": value.value, "label": value.label}
                                data_source_instances_dict[data_source_uri][instance_uri][
                                    property_lookup_dict[prop_uri]].append(property_instance)
                        else:
                            # If the property is not in the dictionary, add it
                            if value.value == value.label:
                                # If the value and label are the same, add the label as a string
                                data_source_instances_dict[data_source_uri][instance_uri][
                                    property_lookup_dict[prop_uri]] = [value.label]
                            else:
                                # If the value and label are different, create a dictionary with the URI and label
                                property_instance = {"uri": value.value, "label": value.label}
                                data_source_instances_dict[data_source_uri][instance_uri][
                                    property_lookup_dict[prop_uri]] = [property_instance]

    # Save the data for each data source to a separate file
    for data_source_uri in data_source_instances_dict.keys():
        if data_source_uri:
            data_source_dir = os.path.join(data_dir, data_source_lookup_dict[data_source_uri])
            instances_data_source_path_dir = os.path.join(data_source_dir, f"instances_{offset*page_size}_to_{(offset*page_size)+page_size}.json".format(offset, offset + page_size))
            if not os.path.exists(data_source_dir):
                os.mkdir(data_source_dir)
            instance_list = list()
            for instance_uri in data_source_instances_dict[data_source_uri].keys():
                instance = data_source_instances_dict[data_source_uri][instance_uri]
                for key in instance.keys():
                    if isinstance(instance[key], list):
                        if len(list(set([type(prop_format) for prop_format in instance[key]]))) > 1:
                            reformatted_property = list()
                            for property_value in instance[key]:
                                if isinstance(property_value, str):
                                    property_instance = dict()
                                    property_instance["uri"] = None
                                    property_instance["label"] = property_value
                                    reformatted_property.append(property_instance)
                                else:
                                    reformatted_property.append(property_value)
                            data_source_instances_dict[data_source_uri][instance_uri][key] = reformatted_property
                instance_list.append(data_source_instances_dict[data_source_uri][instance_uri])
            with open(instances_data_source_path_dir, "w") as writefile:
                json.dump(instance_list, writefile, indent=1)



def meta_data():
    return {
        'id': 'BMS_download_2',
        'name': 'BMS Downloader 2',
        'description': 'Execute a full download of a canonical type to re-integrate and provide as e.g. RDS',
        'type-id': 'event-processor',
    }


def session_options_definition():
    return [
        {
            'content_type': 'text',
            'id': 'federation_server',
            'name': 'Federation Server',
            'description': 'The server to federate to and download the CT from'
        },
        {
            'content_type': 'text',
            'id': 'federation_user',
            'name': 'Federation user name',
            'description': 'The Federation user'
        },
        {
            'content_type': 'text',
            'id': 'federation_pwd',
            'name': 'Federation Password',
            'description': 'The Federation user password'
        },
        {
            'content_type': 'text',
            'id': 'canonical_type',
            'name': 'Canonical Type URI',
            'description': 'The URI Canonical Type to fully download'
        },
        {
            'content_type': 'text',
            'id': 'data_source_uri_list',
            'name': 'Data Source URI List (fill "all" for all data sources, use format \"<data_soource_uri_1>,<data_soource_uri_2>,<data_soource_uri_3>\" to filter)',
            'description': 'Add data source URIs to filter the download to. Format is as follows: \"<data_soource_uri_1>,<data_soource_uri_2>,<data_soource_uri_3>\". Limited to filter on max 10 data sources.',
            'default': "all"
        },
        {
            'content_type': 'bool',
            'id': 'create_pipeline_bool',
            'name': 'Create Pipeline',
            'description': 'Toggle this option to automatically build a pipeline, merging the data from the different sources and generating a configuration.',
            'default': True
        }
    ]


class PipelineBuilderBase:

    def __init__(self, pipeline):
        self.pipeline = pipeline  # must have method 'add_component' that returns either Component object or a GUID

    @staticmethod
    def get_prop_uri(type_id: str, prop_id: str) -> PropertyURI:
        return f'http://ns.ontoforce.com/2013/{type_id}/prop/{prop_id}'

    @staticmethod
    def get_facet_uri(type_id: str, facet_id: str) -> FacetURI:
        return f'http://ns.ontoforce.com/2013/{type_id}/facet/{facet_id}'

    def make_create_datasource(self,
                               ds_uri: DataSourceURI,
                               ds_name: str,
                               description: Optional[str] = None,
                               homepage: Optional[str] = None,
                               info_file_path: Optional[Path] = None) -> ComponentOrGUID:
        options = {'uri': ds_uri,
                   'label': ds_name,
                   'description': description,
                   'homepage': homepage,
                   'info_file_path': str(info_file_path)}
        return self.pipeline.add_component('create_datasource', name=ds_name, options=options)

    def make_importer(self,
                      importer_type_id: str,
                      class_name: str,
                      ds_uri: DataSourceURI,
                      file_paths: List[Path],
                      options: Optional[Dict] = None,
                      name: Optional[str] = None,
                      resource_type: Optional[ResourceTypeURI] = None,
                      predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = options or {}
        all_options = {'alignment': class_name,
                       'files': [str(fp) for fp in file_paths],
                       'data_source': ds_uri,
                       **options}
        if resource_type:
            all_options['rdf_type'] = resource_type
        return self.pipeline.add_component(importer_type_id, name=name, options=all_options) #, predecessors=predecessors

    def make_transform_literals(self,
                                class_name: str,
                                expression: str,
                                test_expression: str = '',
                                filter_expression: Optional[str] = None,
                                data_sources: Optional[List[DataSourceURI]] = None,
                                name: Optional[str] = None,
                                predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {'operation': {'tests': test_expression,
                                 'expression': expression},
                   'alignment': class_name}
        if filter_expression:
            options['filter'] = {'expression': filter_expression,
                                 'tests': ''}
        if data_sources:
            options['data_sources'] = data_sources
        return self.pipeline.add_component('swim_lane_action', name=name, options=options, predecessors=predecessors)

    def make_add_uri(self,
                     class_name: str,
                     predicate_name: str,
                     prefix: str,
                     predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {'alignment': class_name,
                   'source_predicate': predicate_name,
                   'prefix': prefix}
        return self.pipeline.add_component('set_subject_uri', options=options) #, predecessors=predecessors

    def make_add_label(self,
                       class_name: str,
                       predicate_name: str,
                       new_preferred: bool = True,
                       predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {'alignment': class_name,
                   'source_predicate': predicate_name,
                   'new_preferred': new_preferred}
        return self.pipeline.add_component('set_subject_label', options=options) #, predecessors=predecessors

    def make_no_operation(self,
                     predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {}
        return self.pipeline.add_component('no_operation', options=options) #, predecessors=predecessors

    def make_merge_classes(self,
                           source_class_name: str,
                           target_class_name: str,
                           name: Optional[str] = None,
                           predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {'source_alignment': source_class_name,
                   'destination_alignment': target_class_name}
        return self.pipeline.add_component('merge_alignments', name=name, options=options) #, predecessors=predecessors)

    def make_ct_configuration(self,
                              ct_uri: CanonicalTypeURI,
                              ct_name: str,
                              icon: str,
                              resource_types: Optional[List[ResourceTypeURI]] = None,
                              class_names: Optional[List[str]] = None,
                              description: Optional[str] = None,
                              properties: Optional[List[JSONDict]] = None,
                              facets: Optional[List[JSONDict]] = None,
                              predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {'uri': ct_uri,
                   'icon': icon,
                   'label': ct_name,
                   'properties': properties or [],
                   'facets': facets or []}
        if resource_types:
            options['types'] = resource_types
        if class_names:
            options['classes'] = class_names
        if description:
            options['description'] = description
        return self.pipeline.add_component('create_canonical_type', name=ct_name, options=options) #, predecessors=predecessors

    def make_indexer(self,
                     auto_drop_predicates: Optional[bool] = None,
                     predecessors: Optional[List[ComponentGUID]] = None) -> ComponentOrGUID:
        options = {}
        if auto_drop_predicates is not None:
            options['drop_predicates'] = auto_drop_predicates
        return self.pipeline.add_component('indexing', options=options) #, predecessors=predecessors


class PipelineBuilder(PipelineBuilderBase):
    def __init__(self,
                 session: DisqoverSession,
                 pipeline_guid: PipelineGUID):
        self._session = session
        api = DataIngestionAPI(self._session)
        pipeline = api.pipelines[pipeline_guid]
        super().__init__(pipeline)
        self.component_types = api.component_types
        self.source_data = api.source_data

    def make_importer(self,
                      file_type: FileType,
                      class_name: str,
                      ds_uri: DataSourceURI,
                      file_paths: List[Path],
                      options: Optional[Dict] = None,
                      name: Optional[str] = None,
                      resource_type: Optional[ResourceTypeURI] = None) -> Component:
        importer_type = self.component_types.get_importer(file_type)
        return super().make_importer(importer_type.identifier,
                                     class_name,
                                     ds_uri,
                                     file_paths,
                                     options=options,
                                     name=name,
                                     resource_type=resource_type)

    def scan_files(self,
                   file_type: FileType,
                   file_paths: List[Path],
                   predicate_prefix: str = '',
                   max_scan_time: int = DEFAULT_SCAN_TIMEOUT) -> ScanResult:
        return self.source_data.scan_files(file_paths, file_type,
                                           max_seconds=max_scan_time,
                                           predicate_prefix=predicate_prefix)

    def add_ct_facet(self,
                     ct_comp,
                     type_id: str,
                     facet_id: str,
                     label: str,
                     predicates: List[str],
                     data_type: Optional[FacetDataType] = None,
                     view_type: Optional[FacetViewType] = None):
        facet_uri = self.get_facet_uri(type_id, facet_id)
        add_facet(ct_comp,
                  facet_uri,
                  label,
                  predicates,
                  data_type=data_type,
                  view_type=view_type)

    def add_ct_property(self,
                        ct_comp,
                        type_id: str,
                        prop_id: str,
                        label: str,
                        predicates: List[str],
                        data_type: Optional[PropertyDataType] = None,
                        sortable: bool = False,
                        description: Optional[str] = None,
                        as_facet: bool = False,
                        facet_data_type: Optional[FacetDataType] = None,
                        facet_view_type: Optional[FacetViewType] = None) -> None:
        prop_uri = self.get_prop_uri(type_id, prop_id)
        add_property(ct_comp,
                     prop_uri,
                     label,
                     predicates,
                     data_type=data_type,
                     sortable=sortable,
                     description=description)
        if as_facet:
            if facet_data_type is None and data_type is not None:
                facet_data_type = FacetDataType(data_type.value)  # facet data type from property data type
            facet_uri = self.get_facet_uri(type_id, prop_id)
            set_property_as_facet(ct_comp, prop_uri, facet_uri,
                                  data_type=facet_data_type,
                                  view_type=facet_view_type)

    @staticmethod
    def add_segment_as_predecessor(component: Component,
                                   segment: SegmentOrGUID) -> None:
        component._retrieve()
        if not isinstance(segment, Segment):
            segment = Segment(component._session, component.pipeline_guid, segment)
        for segment_component in PipelineBuilder.get_segment_last_components(segment):
            component._predecessors_guids.add(segment_component.guid)
        component._save()

    @staticmethod
    def get_segment_first_components(segment: Segment) -> Tuple[Component, ...]:
        segment._retrieve()
        in_segment_guids = segment._component_guids
        result = []
        for guid in in_segment_guids:
            comp = Component(segment._session, segment.pipeline_guid, guid)
            pred_guids = set(c.guid for c in comp.predecessors)
            if len(in_segment_guids & pred_guids) == 0:
                result.append(comp)
        return tuple(result)

    @staticmethod
    def get_segment_last_components(segment: Segment) -> Tuple[Component, ...]:
        segment._retrieve()
        in_segment_guids = segment._component_guids
        guids_with_internal_successors = set()
        for guid in in_segment_guids:
            comp = Component(segment._session, segment.pipeline_guid, guid)
            for pred in comp.predecessors:
                if pred.guid in in_segment_guids:
                    guids_with_internal_successors.add(pred.guid)
        guids_without_internal_successors = in_segment_guids - guids_with_internal_successors
        return tuple(Component(segment._session, segment.pipeline_guid, guid) for guid in guids_without_internal_successors)


def make_datasource_segment(builder: PipelineBuilder, ct_name, ds_uri, ds_label, data_source_info):
    ds_class_name = ds_label.lower().replace(" ", "_") + ct_name
    ds_data_file_path = Path("federation_download_plugin/" + ct_name + "/data/" + ds_label + "/instances*.json")
    ds_info_file_path = Path("federation_download_plugin/" + ct_name + "/config/" + "modification_date_{}.json".format(ds_label))

    # DS
    ds_comp = builder.make_create_datasource(ds_uri, ds_label, description=data_source_info["description"], homepage=data_source_info["homepage"], info_file_path=ds_info_file_path)

    # Import
    importer = builder.make_importer(file_type=FileType.JSON,
                                     class_name=ds_class_name,
                                     ds_uri=ds_uri,
                                     file_paths=[ds_data_file_path],
                                     name=ds_label + " importer")
    scan_result = builder.scan_files(FileType.JSON,
                                     file_paths = [ds_data_file_path],
                                     predicate_prefix='{}:'.format(ct_name),
                                     max_scan_time=20)
    # Resolve step
    if not scan_result.is_resolved:
        chosen_proposal = scan_result.possible_proposal_choices[0]
        scan_result = scan_result.resolve(chosen_proposal)

    importer.set_options_from_scan_result(scan_result)
    importer.add_predecessor(ds_comp)
    predicates = importer.get_option_value("columns")

    # Add URI
    add_uri_comp = builder.make_add_uri(ds_class_name,
                                        '{}:preferred_uri'.format(ct_name),
                                        prefix="",
                                        )
    add_uri_comp.add_predecessor(importer)

    # Add Label
    add_label_comp = builder.make_add_label(ds_class_name,
                                        '{}:preferred_label'.format(ct_name),
                                        new_preferred=True)
    add_label_comp.add_predecessor(importer)

    # Add No Op
    add_no_operation = builder.make_no_operation()

    # Add Label
    if "Synonym" in [predicate["file_column"] for predicate in predicates]:
        add_synonym_comp = builder.make_add_label(ds_class_name,
                                            '{}:synonym'.format(ct_name),
                                            new_preferred=False)
        add_synonym_comp.add_predecessor(add_label_comp)
        add_no_operation.add_predecessor(add_synonym_comp)
    else:
        add_no_operation.add_predecessor(add_label_comp)

    add_no_operation.add_predecessor(add_uri_comp)

    return predicates, add_no_operation, ds_class_name


def build_ds_query(dataset_filter_split, ct_uri):
    if len(dataset_filter_split) == 1:
        query = (FacetEqual("dataset", dataset_filter_split[0])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 2:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 3:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 4:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 5:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3]) |
                 FacetEqual("dataset", dataset_filter_split[4])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 6:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3]) |
                 FacetEqual("dataset", dataset_filter_split[4]) |
                 FacetEqual("dataset", dataset_filter_split[5])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 7:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3]) |
                 FacetEqual("dataset", dataset_filter_split[4]) |
                 FacetEqual("dataset", dataset_filter_split[5]) |
                 FacetEqual("dataset", dataset_filter_split[6])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 8:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3]) |
                 FacetEqual("dataset", dataset_filter_split[4]) |
                 FacetEqual("dataset", dataset_filter_split[5]) |
                 FacetEqual("dataset", dataset_filter_split[6]) |
                 FacetEqual("dataset", dataset_filter_split[7])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 9:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3]) |
                 FacetEqual("dataset", dataset_filter_split[4]) |
                 FacetEqual("dataset", dataset_filter_split[5]) |
                 FacetEqual("dataset", dataset_filter_split[6]) |
                 FacetEqual("dataset", dataset_filter_split[7]) |
                 FacetEqual("dataset", dataset_filter_split[8])) & BelongsToCanonicalType(ct_uri)
    elif len(dataset_filter_split) == 10:
        query = (FacetEqual("dataset", dataset_filter_split[0]) |
                 FacetEqual("dataset", dataset_filter_split[1]) |
                 FacetEqual("dataset", dataset_filter_split[2]) |
                 FacetEqual("dataset", dataset_filter_split[3]) |
                 FacetEqual("dataset", dataset_filter_split[4]) |
                 FacetEqual("dataset", dataset_filter_split[5]) |
                 FacetEqual("dataset", dataset_filter_split[6]) |
                 FacetEqual("dataset", dataset_filter_split[7]) |
                 FacetEqual("dataset", dataset_filter_split[8]) |
                 FacetEqual("dataset", dataset_filter_split[9])) & BelongsToCanonicalType(ct_uri)
    return query


def process_events(events, options_values, log, authenticator, **kwargs):
    federation_server = options_values["federation_server"]
    federation_user = options_values["federation_user"]
    federation_pwd = options_values["federation_pwd"]
    ct_uri = options_values["canonical_type"]
    dataset_filter = options_values["data_source_uri_list"]
    create_pipeline_bool = options_values["create_pipeline_bool"]
    dataset_filter_split = dataset_filter.split(",")

    log("Creating necessary directories for the plugin")
    plugin_storage_dir = os.path.join("plugin", "external")
    plugin_ct_storage_dir = os.path.join(plugin_storage_dir, "federation_download_plugin")
    if not os.path.exists(plugin_ct_storage_dir):
        os.mkdir(plugin_ct_storage_dir)

    if dataset_filter == "all":
        ct_name = ct_uri.split("/")[-1].lower() + "_" + "_".join([datasource_uri.split("/")[-1] for datasource_uri in dataset_filter_split])
        ct_dir = os.path.join(plugin_ct_storage_dir, ct_name)
    else:
        ct_name = ct_uri.split("/")[-1].lower() + "_" + "_".join([datasource_uri.split("/")[-1] for datasource_uri in dataset_filter_split])
        ct_dir = os.path.join(plugin_ct_storage_dir, ct_name)
    if not os.path.exists(ct_dir):
        os.mkdir(ct_dir)
    config_dir = os.path.join(ct_dir, "config")
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    data_dir = os.path.join(ct_dir, "data")
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    log("Starting Federation session to download following data sources from CT {}: [{}]".format(ct_uri, dataset_filter))
    session = start_session(federation_server, federation_user, federation_pwd)  # start the Disqover session for the api
    api = DataQueryAPI(session)  # start the DataQueryAPI from the Disqover API module

    #### ONTOLOGY to config/data_sources.json
    log("Saving Ontology and Data Source information for {}...".format(ct_uri))
    data_source_lookup_dict, data_sources = save_ontology(api, ct_uri, config_dir)

    #### CONFIGURATION
    log("Saving Configuration information for {}...".format(ct_uri))
    property_lookup_dict, ct_uri_list, ct_config, properties, facets = save_configuration(api, ct_uri, config_dir)

    #### Create the Query for the API
    if dataset_filter == "all":
        query = BelongsToCanonicalType(ct_uri)  # create the Query for the API
    else:
        query = build_ds_query(dataset_filter_split, ct_uri)
        if len(dataset_filter_split) > 10:
            return {"message": "Exceeded max number of data source filters for plugin."}

    #### FOR COUNT
    instance_count = api.get_instances(query=query).get_count()
    log("Starting download of {} instances...".format(instance_count.__str__()))
    page_size = 5000
    page_end = (instance_count // page_size) + 1
    offset_list = [i for i in range(0, page_end)]
    for offset in offset_list:
        download_and_save_instances(api, query, offset, page_size, property_lookup_dict, data_source_lookup_dict, data_dir)
        log(f"Download of {(offset * page_size) + page_size}/{instance_count} instances complete - [OK]")
    log("Finished downloading.")
    log("Transferring data to source data...")
    from disqover.disqover_session import _InternalSession
    session = _InternalSession('http://metis', 8000)
    from disqover import SourceData
    from pathlib import Path

    sd = SourceData(session)
    for subdir, dirs, files in os.walk(ct_dir):
        for file in files:
            sd.upload_file(Path(subdir) / Path(file), Path("federation_download_plugin/" + "/".join(subdir.split("/")[3:])) / file)
            log("Uploaded file {} to directory {} - [OK]".format("federation_download_plugin/" + subdir + "/" + file, "/".join(subdir.split("/")[3:]) + "/" + str(file)))
    log("Finished data transfer.")

    if create_pipeline_bool:
        log("Building Pipeline for downloaded data...")
        session = DisqoverSession(authenticator)
        session._authenticator.authenticate(session._http_session, session._logger)
        session._retrieve_application_info()
        api = DataIngestionAPI(session)
        if dataset_filter == "all":
            pipeline_name = "RDS {} Automated Pipeline".format(ct_uri.split("#")[-1])
        else:
            pipeline_name = "RDS {} Automated Pipeline".format(ct_uri.split("#")[-1] + " (" + ", ".join([data_source_lookup_dict[datasource_uri] for datasource_uri in dataset_filter_split]) + ")")

        pipeline = api.pipelines.create(name=pipeline_name, description="This is an automatically built pipeline from the Federation Downloader Plugin")
        builder = PipelineBuilder(session, pipeline.guid)
        ds_dir_list = os.listdir(data_dir)
        count = 0
        merge_list = list()
        predicate_lookup_dict = dict()
        for ds_uri in data_source_lookup_dict.keys():
            if data_source_lookup_dict[ds_uri] in ds_dir_list:
                new_ds_uri = "http://ns.ontoforce.com/disqover.dataset/" + ds_uri.split("/")[-1]
                data_source_info = data_sources[[data_source["uri"] for data_source in data_sources].index(ds_uri)]
                predicates, no_operation_component, ds_class_name = make_datasource_segment(builder, ct_name, new_ds_uri,
                                                                                            data_source_lookup_dict[ds_uri],
                                                                                            data_source_info)
                for predicate in predicates:
                    if predicate["file_column"] not in predicate_lookup_dict.keys():
                        predicate_lookup_dict[predicate["file_column"]] = predicate["predicate"] + ".lit"
                merge_list.insert(0, (ds_class_name, no_operation_component))
                if count >= 1:
                    add_merge_classes = builder.make_merge_classes(merge_list[0][0], merge_list[1][0])
                    add_merge_classes.add_predecessor(merge_list[0][1])
                    add_merge_classes.add_predecessor(merge_list[1][1])
                    merge_list = [(merge_list[1][0], add_merge_classes)]
                count += 1

        properties_config_list = list()
        for property in properties:
            if not property["label"] in ["Synonym", "Label", "Alternative URI", "Datasource"]:
                prop_dict = dict()
                new_prop_uri = "http://ns.ontoforce.com/disqover.ontology/property/{}/{}".format(ct_name,
                                                                                                 property["uri"].split("/")[
                                                                                                     -1].lower())
                prop_dict["uri"] = new_prop_uri
                prop_dict["label"] = property["label"]
                try:
                    if property["label"] in predicate_lookup_dict.keys():
                        if predicate_lookup_dict[property["label"]]:
                            prop_dict["predicates"] = [predicate_lookup_dict[property["label"]]]
                    else:
                        if predicate_lookup_dict[property["label"] + "[*]"]:
                            prop_dict["predicates"] = [predicate_lookup_dict[property["label"] + "[*]"]]
                except Exception as e:
                    prop_dict["predicates"] = None
                    log("Property label {} not found in lookup dictionary\n{}".format(property["label"], e))
                for facet in facets:
                    if facet["label"] == property["label"]:
                        prop_dict["description"] = facet["description"]
                        new_facet_uri = "http://ns.bms.com/disqover.ontology/facet/{}/{}".format(ct_name, property[
                            "uri"].split("/")[-1].lower())
                        prop_dict["facet_uri"] = new_facet_uri
                if prop_dict["predicates"]:
                    properties_config_list.append(prop_dict)

        new_ct_uri = "http://ns.bms.com/disqover.ontology/canoncial_type/" + ct_uri.split("/")[-1].lower()
        add_ct = builder.make_ct_configuration(ct_uri=new_ct_uri,
                                               ct_name=ct_config["label"],
                                               icon=ct_config["icon"],
                                               class_names=[merge_list[0][0]],
                                               description=ct_config["description"],
                                               properties=properties_config_list
                                               )
        add_ct.add_predecessor(add_merge_classes)
        add_indexer = builder.make_indexer()
        add_indexer.add_predecessor(add_ct)
        log("Finished building pipeline {}.".format(pipeline_name))
        return {"message": "Plugin finished, please check the configuration of the CT in the created Pipeline {}".format(pipeline_name)}
    else:
        log("Skipped building pipeline, data is avialable in source_data directory {}".format("federation_download_plugin/" + ct_name))
        return {"message": "Plugin finished, data is avialable in source_data directory {}".format("federation_download_plugin/" + ct_name)}


