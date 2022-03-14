import logging
import time
import warnings
from abc import ABC
from typing import Dict, Generator, Iterable, Optional, Tuple

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.openapi_parser import (
    clean_url,
    compose_url_attr,
    extract_fields,
    get_endpoints,
    get_swag_json,
    get_tok,
    get_url_basepath,
    request_call,
    set_metadata,
    try_guessing,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    TagAssociationClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class OpenApiConfig(ConfigModel):
    name: str
    url: str
    swagger_file: str
    ignore_endpoints: list = []
    username: str = ""
    password: str = ""
    forced_examples: dict = {}
    token: Optional[str] = None
    get_token: dict = {}

    def get_swagger(self) -> Dict:
        if self.get_token or self.token is not None:
            if self.token is not None:
                ...
            else:
                assert (
                    "url_complement" in self.get_token.keys()
                ), "When 'request_type' is set to 'get', an url_complement is needed for the request."
                if self.get_token["request_type"] == "get":
                    assert (
                        "{username}" in self.get_token["url_complement"]
                    ), "we expect the keyword {username} to be present in the url"
                    assert (
                        "{password}" in self.get_token["url_complement"]
                    ), "we expect the keyword {password} to be present in the url"
                    url4req = self.get_token["url_complement"].replace(
                        "{username}", self.username
                    )
                    url4req = url4req.replace("{password}", self.password)
                elif self.get_token["request_type"] == "post":
                    url4req = self.get_token["url_complement"]
                else:
                    raise KeyError(
                        "This tool accepts only 'get' and 'post' as method for getting tokens"
                    )
                self.token = get_tok(
                    url=self.url,
                    username=self.username,
                    password=self.password,
                    tok_url=url4req,
                    method=self.get_token["request_type"],
                )
            sw_dict = get_swag_json(
                self.url, token=self.token, swagger_file=self.swagger_file
            )  # load the swagger file

        else:  # using basic auth for accessing endpoints
            sw_dict = get_swag_json(
                self.url,
                username=self.username,
                password=self.password,
                swagger_file=self.swagger_file,
            )
        return sw_dict


# class ParserWarning(UserWarning):
#     def __init__(self, message: str, key: str) -> None:
#         self.message


class ApiWorkUnit(MetadataWorkUnit):
    pass


class APISource(Source, ABC):
    def __init__(self, config: OpenApiConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SourceReport()
        self.url_basepath = ""

    def report_bad_responses(self, status_code: int, key: str) -> None:
        if status_code == 400:
            self.report.report_warning(
                key=key, reason="Unknown error for reaching endpoint"
            )
        elif status_code == 403:
            self.report.report_warning(key=key, reason="Not authorised to get endpoint")
        elif status_code == 404:
            self.report.report_warning(
                key=key,
                reason="Unable to find an example for endpoint. Please add it to the list of forced examples.",
            )
        elif status_code == 500:
            self.report.report_warning(
                key=key, reason="Server error for reaching endpoint"
            )
        elif status_code == 504:
            self.report.report_warning(key=key, reason="Timeout for reaching endpoint")
        else:
            raise Exception(
                f"Unable to retrieve endpoint, response code {status_code}, key {key}"
            )

    def init_dataset(
        self, endpoint_k: str, endpoint_dets: dict
    ) -> Tuple[DatasetSnapshot, str]:
        config = self.config

        dataset_name = endpoint_k[1:].replace("/", ".")

        if len(dataset_name) > 0:
            if dataset_name[-1] == ".":
                dataset_name = dataset_name[:-1]
        else:
            dataset_name = "root"

        dataset_snapshot = DatasetSnapshot(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{config.name}.{dataset_name},PROD)",
            aspects=[],
        )

        # adding description
        dataset_properties = DatasetPropertiesClass(
            description=endpoint_dets["description"], customProperties={}
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # adding tags
        tags_str = [make_tag_urn(t) for t in endpoint_dets["tags"]]
        tags_tac = [TagAssociationClass(t) for t in tags_str]
        gtc = GlobalTagsClass(tags_tac)
        dataset_snapshot.aspects.append(gtc)

        # the link will appear in the "documentation"
        link_url = clean_url(config.url + self.url_basepath + endpoint_k)
        link_description = "Link to call for the dataset."
        creation = AuditStampClass(
            time=int(time.time()), actor="urn:li:corpuser:etl", impersonator=None
        )
        link_metadata = InstitutionalMemoryMetadataClass(
            url=link_url, description=link_description, createStamp=creation
        )
        inst_memory = InstitutionalMemoryClass([link_metadata])
        dataset_snapshot.aspects.append(inst_memory)

        return dataset_snapshot, dataset_name

    def build_wu(
        self, dataset_snapshot: DatasetSnapshot, dataset_name: str
    ) -> Generator[ApiWorkUnit, None, None]:
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = ApiWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def get_workunits(self) -> Iterable[ApiWorkUnit]:  # noqa: C901
        config = self.config

        sw_dict = self.config.get_swagger()

        self.url_basepath = get_url_basepath(sw_dict)

        # Getting all the URLs accepting the "GET" method
        with warnings.catch_warnings(record=True) as warn_c:
            url_endpoints = get_endpoints(sw_dict)

            for w in warn_c:
                w_msg = w.message
                w_spl = w_msg.args[0].split(" --- ")  # type: ignore
                self.report.report_warning(key=w_spl[1], reason=w_spl[0])

        # here we put a sample from the "listing endpoint". To be used for later guessing of comosed endpoints.
        root_dataset_samples = {}

        # looping on all the urls
        for endpoint_k, endpoint_dets in url_endpoints.items():
            if endpoint_k in config.ignore_endpoints:
                continue

            dataset_snapshot, dataset_name = self.init_dataset(
                endpoint_k, endpoint_dets
            )

            # adding dataset fields
            if "data" in endpoint_dets.keys():
                # we are lucky! data is defined in the swagger for this endpoint
                schema_metadata = set_metadata(dataset_name, endpoint_dets["data"])
                dataset_snapshot.aspects.append(schema_metadata)
                yield from self.build_wu(dataset_snapshot, dataset_name)
            elif (
                "{" not in endpoint_k
            ):  # if the API does not explicitely require parameters
                tot_url = clean_url(config.url + self.url_basepath + endpoint_k)

                if config.token:
                    response = request_call(tot_url, token=config.token)
                else:
                    response = request_call(
                        tot_url, username=config.username, password=config.password
                    )
                if response.status_code == 200:
                    fields2add, root_dataset_samples[dataset_name] = extract_fields(
                        response, dataset_name
                    )
                    if not fields2add:
                        self.report.report_warning(key=endpoint_k, reason="No Fields")
                    schema_metadata = set_metadata(dataset_name, fields2add)
                    dataset_snapshot.aspects.append(schema_metadata)

                    yield from self.build_wu(dataset_snapshot, dataset_name)
                else:
                    self.report_bad_responses(response.status_code, key=endpoint_k)
            else:
                if endpoint_k not in config.forced_examples.keys():
                    # start guessing...
                    url_guess = try_guessing(endpoint_k, root_dataset_samples)
                    tot_url = clean_url(config.url + self.url_basepath + url_guess)
                    if config.token:
                        response = request_call(tot_url, token=config.token)
                    else:
                        response = request_call(
                            tot_url, username=config.username, password=config.password
                        )
                    if response.status_code == 200:
                        fields2add, _ = extract_fields(response, dataset_name)
                        if not fields2add:
                            self.report.report_warning(
                                key=endpoint_k, reason="No Fields"
                            )
                        schema_metadata = set_metadata(dataset_name, fields2add)
                        dataset_snapshot.aspects.append(schema_metadata)

                        yield from self.build_wu(dataset_snapshot, dataset_name)
                    else:
                        self.report_bad_responses(response.status_code, key=endpoint_k)
                else:
                    composed_url = compose_url_attr(
                        raw_url=endpoint_k, attr_list=config.forced_examples[endpoint_k]
                    )
                    tot_url = clean_url(config.url + self.url_basepath + composed_url)
                    if config.token:
                        response = request_call(tot_url, token=config.token)
                    else:
                        response = request_call(
                            tot_url, username=config.username, password=config.password
                        )
                    if response.status_code == 200:
                        fields2add, _ = extract_fields(response, dataset_name)
                        if not fields2add:
                            self.report.report_warning(
                                key=endpoint_k, reason="No Fields"
                            )
                        schema_metadata = set_metadata(dataset_name, fields2add)
                        dataset_snapshot.aspects.append(schema_metadata)

                        yield from self.build_wu(dataset_snapshot, dataset_name)
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
