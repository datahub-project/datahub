from abc import ABC
import time
from typing import Iterable, Dict, Optional
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.ingestion.api.source import Source, SourceReport
from dataclasses import dataclass
import logging
import warnings
from datahub.metadata.schema_classes import (DatasetPropertiesClass, InstitutionalMemoryClass,
                                             InstitutionalMemoryMetadataClass, AuditStampClass, GlobalTagsClass,
                                             TagAssociationClass)
from tqdm.auto import tqdm
from datahub.ingestion.source.openapi_parser import (get_swag_json, get_tok, get_url_basepath, set_metadata,
                                                     clean_url, request_call, extract_fields, try_guessing,
                                                     compose_url_attr, get_endpoints)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.emitter.mce_builder import make_tag_urn

logger: logging.Logger = logging.getLogger(__name__)


class OpenApiConfig(ConfigModel):
    name: str
    url: str
    swagger_file: str
    ignore_endpoints: Optional[list] = []
    username: str = ""
    password: str = ""
    forced_examples: Optional[dict] = {}
    token: str = None
    get_token: bool = False

    def get_swagger(self) -> Dict:
        if self.get_token:  # token based authentication, to be tested
            if not self.token:
                self.token = get_tok(url=self.url, username=self.username, password=self.password)

            sw_dict = get_swag_json(self.url, token=self.token, swagger_file=self.swagger_file)  # load the swagger file
        else:
            sw_dict = get_swag_json(self.url, username=self.username, password=self.password,
                                    swagger_file=self.swagger_file)
        return sw_dict


# class ParserWarning(UserWarning):
#     def __init__(self, message: str, key: str) -> None:
#         self.message


@dataclass
class ApiWorkUnit(MetadataWorkUnit):
    pass


class APISource(Source, ABC):

    def __init__(self, config: OpenApiConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SourceReport()

    def report_bad_responses(self, status_code: int, key: str):
        if status_code == 400:
            self.report.report_warning(key=key, reason="Unknown error for reaching endpoint")
        elif status_code == 403:
            self.report.report_warning(key=key, reason="Not authorised to get endpoint")
        elif status_code == 404:
            self.report.report_warning(key=key, reason=f"Unable to find an example for {key}"
                                                       f" in. Please add it to the list of forced examples.")
        elif status_code == 500:
            self.report.report_warning(key=key, reason="Server error for reaching endpoint")
        elif status_code == 504:
            self.report.report_warning(key=key, reason="Timeout for reaching endpoint")
        else:
            raise Exception(f"Unable to retrieve {key}, response code {status_code}")

    def get_workunits(self) -> Iterable[ApiWorkUnit]:
        config = self.config

        sw_dict = config.get_swagger()

        url_basepath = get_url_basepath(sw_dict)

        # Getting all the URLs accepting the "GET" method,
        # together with their description and the tags
        with warnings.catch_warnings(record=True) as warn_c:
            url_endpoints = get_endpoints(sw_dict)

            # catch eventual warnings
            for w in warn_c:
                w_spl = w.message.args[0].split(" --- ")
                self.report.report_warning(key=w_spl[1], reason=w_spl[0])

        # here we put a sample from the listing of urls. To be used for later guessing of comosed urls.
        root_dataset_samples = {}

        # looping on all the urls. Each URL will be a dataset, for our definition
        for endpoint_k, endpoint_dets in tqdm(url_endpoints.items(), desc="Checking urls..."):
            if endpoint_k in config.ignore_endpoints:
                continue

            dataset_name = endpoint_k[1:].replace("/", ".")
            if dataset_name[-1] == ".":
                dataset_name = dataset_name[:-1]

            dataset_snapshot = DatasetSnapshot(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{config.name}.{dataset_name},PROD)",
                aspects=[],
            )

            # adding description
            dataset_properties = DatasetPropertiesClass(
                description=endpoint_dets["description"],
                customProperties={},
                # tags=tags_str
            )
            dataset_snapshot.aspects.append(dataset_properties)

            # adding tags
            tags_str = [make_tag_urn(t) for t in endpoint_dets["tags"]]
            tags_tac = [TagAssociationClass(t) for t in tags_str]
            gtc = GlobalTagsClass(tags_tac)
            dataset_snapshot.aspects.append(gtc)

            # the link will appear in the "documentation"
            link_url = clean_url(config.url + url_basepath + endpoint_k)
            link_description = "Link to call for the dataset."
            creation = AuditStampClass(time=int(time.time()),
                                       actor="urn:li:corpuser:etl",
                                       impersonator=None)
            link_metadata = InstitutionalMemoryMetadataClass(url=link_url, description=link_description,
                                                             createStamp=creation)
            inst_memory = InstitutionalMemoryClass([link_metadata])
            dataset_snapshot.aspects.append(inst_memory)

            # adding dataset fields
            if "data" in endpoint_dets.keys():
                # we are lucky! data is defined in the swagger for this endpoint
                schema_metadata = set_metadata(dataset_name, endpoint_dets["data"])
                dataset_snapshot.aspects.append(schema_metadata)

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = ApiWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu
            # elif "parameters" in endpoint_dets.keys():
            #     # half of a luck: we have explicitely declared parameters
            #     enp_ex_pars = ""
            #     for param_def in endpoint_dets["parameters"]:
            #         enp_ex_pars += ""
            elif "{" not in endpoint_k:  # if the API does not explicitely require parameters
                tot_url = clean_url(config.url + url_basepath + endpoint_k)

                if config.token:
                    response = request_call(tot_url, token=config.token)
                else:
                    response = request_call(tot_url, username=config.username, password=config.password)
                if response.status_code == 200:
                    fields2add, root_dataset_samples[dataset_name] = extract_fields(response, dataset_name)
                    if not fields2add:
                        self.report.report_warning(key=endpoint_k, reason="No Fields")
                    schema_metadata = set_metadata(dataset_name, fields2add)
                    dataset_snapshot.aspects.append(schema_metadata)

                    mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                    wu = ApiWorkUnit(id=dataset_name, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu
                else:
                    self.report_bad_responses(response.status_code, key=endpoint_k)
            else:
                if endpoint_k not in config.forced_examples.keys():
                    # start guessing...
                    url_guess = try_guessing(endpoint_k, root_dataset_samples)  # try to guess informations
                    tot_url = clean_url(config.url + url_basepath + url_guess)
                    if config.token:
                        response = request_call(tot_url, token=config.token)
                    else:
                        response = request_call(tot_url, username=config.username, password=config.password)
                    if response.status_code == 200:
                        fields2add, _ = extract_fields(response, dataset_name)
                        if not fields2add:
                            self.report.report_warning(key=endpoint_k, reason="No Fields")
                        schema_metadata = set_metadata(dataset_name, fields2add)
                        dataset_snapshot.aspects.append(schema_metadata)

                        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                        wu = ApiWorkUnit(id=dataset_name, mce=mce)
                        self.report.report_workunit(wu)
                        yield wu
                    else:
                        self.report_bad_responses(response.status_code, key=endpoint_k)
                else:
                    composed_url = compose_url_attr(raw_url=endpoint_k, attr_list=config.forced_examples[endpoint_k])
                    tot_url = clean_url(config.url + url_basepath + composed_url)
                    if config.token:
                        response = request_call(tot_url, token=config.token)
                    else:
                        response = request_call(tot_url, username=config.username, password=config.password)
                    if response.status_code == 200:
                        fields2add, _ = extract_fields(response, dataset_name)
                        if not fields2add:
                            self.report.report_warning(key=endpoint_k, reason="No Fields")
                        schema_metadata = set_metadata(dataset_name, fields2add)
                        dataset_snapshot.aspects.append(schema_metadata)

                        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                        wu = ApiWorkUnit(id=dataset_name, mce=mce)
                        self.report.report_workunit(wu)
                        yield wu
                    else:
                        self.report_bad_responses(response.status_code, key=endpoint_k)

    def get_report(self):
        return self.report

    def close(self):
        pass


class OpenApiSource(APISource):
    def __init__(self, config: OpenApiConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "OpenApi")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OpenApiConfig.parse_obj(config_dict)
        return cls(config, ctx)
