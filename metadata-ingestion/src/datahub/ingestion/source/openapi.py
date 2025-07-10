import json
import logging
import time
import warnings
from abc import ABC
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor.json_schema_util import (
    get_schema_metadata,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.openapi_parser import (
    clean_url,
    compose_url_attr,
    extract_fields,
    get_endpoints,
    get_schema_from_response,
    get_swag_json,
    get_tok,
    get_url_basepath,
    request_call,
    set_metadata,
    try_guessing,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OtherSchemaClass,
    SchemaMetadataClass,
    SubTypesClass,
    TagAssociationClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class OpenApiConfig(ConfigModel):
    name: str = Field(description="Name of ingestion.")
    url: str = Field(description="Endpoint URL. e.g. https://example.com")
    swagger_file: str = Field(
        description="Route for access to the swagger file. e.g. openapi.json"
    )
    ignore_endpoints: list = Field(
        default=[], description="List of endpoints to ignore during ingestion."
    )
    username: str = Field(
        default="", description="Username used for basic HTTP authentication."
    )
    password: str = Field(
        default="", description="Password used for basic HTTP authentication."
    )
    proxies: Optional[dict] = Field(
        default=None,
        description="Eg. "
        "`{'http': 'http://10.10.1.10:3128', 'https': 'http://10.10.1.10:1080'}`."
        "If authentication is required, add it to the proxy url directly e.g. "
        "`http://user:pass@10.10.1.10:3128/`.",
    )
    forced_examples: dict = Field(
        default={},
        description="If no example is provided for a route, it is possible to create one using forced_example.",
    )
    token: Optional[str] = Field(
        default=None, description="Token for endpoint authentication."
    )
    bearer_token: Optional[str] = Field(
        default=None, description="Bearer token for endpoint authentication."
    )
    get_token: dict = Field(
        default={}, description="Retrieving a token from the endpoint."
    )
    verify_ssl: bool = Field(
        default=True, description="Enable SSL certificate verification"
    )
    use_schema_extraction: bool = Field(
        default=True,
        description="Whether to use json_schema_util.py to extract fields from response schemas.",
    )

    @validator("bearer_token", always=True)
    def ensure_only_one_token(
        cls, bearer_token: Optional[str], values: Dict
    ) -> Optional[str]:
        if bearer_token is not None and values.get("token") is not None:
            raise ValueError("Unable to use 'token' and 'bearer_token' together.")
        return bearer_token

    def get_swagger(self) -> Dict:
        if self.get_token or self.token or self.bearer_token is not None:
            if self.token:
                pass
            elif self.bearer_token:
                # TRICKY: To avoid passing a bunch of different token types around, we set the
                # token's value to the properly formatted bearer token.
                # TODO: We should just create a requests.Session and set all the auth
                # details there once, and then use that session for all requests.
                self.token = f"Bearer {self.bearer_token}"
            else:
                assert "url_complement" in self.get_token, (
                    "When 'request_type' is set to 'get', an url_complement is needed for the request."
                )
                if self.get_token["request_type"] == "get":
                    assert "{username}" in self.get_token["url_complement"], (
                        "we expect the keyword {username} to be present in the url"
                    )
                    assert "{password}" in self.get_token["url_complement"], (
                        "we expect the keyword {password} to be present in the url"
                    )
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
                    proxies=self.proxies,
                    verify_ssl=self.verify_ssl,
                )
            sw_dict = get_swag_json(
                self.url,
                token=self.token,
                swagger_file=self.swagger_file,
                proxies=self.proxies,
                verify_ssl=self.verify_ssl,
            )  # load the swagger file

        else:  # using basic auth for accessing endpoints
            sw_dict = get_swag_json(
                self.url,
                username=self.username,
                password=self.password,
                swagger_file=self.swagger_file,
                proxies=self.proxies,
                verify_ssl=self.verify_ssl,
            )
        return sw_dict


class ApiWorkUnit(MetadataWorkUnit):
    pass


@platform_name("OpenAPI", id="openapi")
@config_class(OpenApiConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, supported=False, description="")
class APISource(Source, ABC):
    """

    This plugin is meant to gather dataset-like information about OpenApi Endpoints.

    As example, if by calling GET at the endpoint at `https://test_endpoint.com/api/users/` you obtain as result:
    ```JSON
    [{"user": "albert_physics",
      "name": "Albert Einstein",
      "job": "nature declutterer",
      "is_active": true},
      {"user": "phytagoras",
      "name": "Phytagoras of Kroton",
      "job": "Phylosopher on steroids",
      "is_active": true}
    ]
    ```

    in Datahub you will see a dataset called `test_endpoint/users` which contains as fields `user`, `name` and `job`.

    """

    def __init__(self, config: OpenApiConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SourceReport()
        self.url_basepath = ""

    def report_bad_responses(self, status_code: int, type: str) -> None:
        if status_code == 400:
            self.report.report_warning(
                title="Failed to Extract Metadata",
                message="Bad request body when retrieving data from OpenAPI endpoint",
                context=f"Endpoint Type: {type}, Status Code: {status_code}",
            )
        elif status_code == 403:
            self.report.report_warning(
                title="Unauthorized to Extract Metadata",
                message="Received unauthorized response when attempting to retrieve data from OpenAPI endpoint",
                context=f"Endpoint Type: {type}, Status Code: {status_code}",
            )
        elif status_code == 404:
            self.report.report_warning(
                title="Failed to Extract Metadata",
                message="Unable to find an example for endpoint. Please add it to the list of forced examples.",
                context=f"Endpoint Type: {type}, Status Code: {status_code}",
            )
        elif status_code == 500:
            self.report.report_warning(
                title="Failed to Extract Metadata",
                message="Received unknown server error from OpenAPI endpoint",
                context=f"Endpoint Type: {type}, Status Code: {status_code}",
            )
        elif status_code == 504:
            self.report.report_warning(
                title="Failed to Extract Metadata",
                message="Timed out when attempting to retrieve data from OpenAPI endpoint",
                context=f"Endpoint Type: {type}, Status Code: {status_code}",
            )
        else:
            raise Exception(
                f"Unable to retrieve endpoint, response code {status_code}, key {type}"
            )

    def detect_openapi_version(self, sw_dict: Dict) -> str:
        """Detect whether this is OpenAPI v2 (Swagger) or v3."""
        if "swagger" in sw_dict:
            return "v2"
        elif "openapi" in sw_dict:
            return "v3"
        else:
            raise ValueError(
                "Unable to detect OpenAPI version - missing 'swagger' or 'openapi' field"
            )

    def extract_response_schema_from_endpoint(
        self, endpoint_spec: Dict, sw_dict: Dict
    ) -> Optional[Dict]:
        """Extract the response schema from an endpoint specification."""
        try:
            # Get the 200 response
            responses = endpoint_spec.get("responses", {})
            success_response = responses.get("200") or responses.get(200)

            if not success_response:
                return None

            # Extract schema from response
            if "content" in success_response:
                # OpenAPI v3 format
                content = success_response["content"]
                if "application/json" in content:
                    schema = content["application/json"].get("schema")
                    if schema:
                        return get_schema_from_response(schema, sw_dict)
            elif "schema" in success_response:
                # Swagger v2 format
                schema = success_response["schema"]
                return get_schema_from_response(schema, sw_dict)

            return None
        except Exception as e:
            logger.warning(f"Error extracting response schema: {str(e)}")
            return None

    def create_schema_metadata_from_schema(
        self, dataset_name: str, schema: Dict
    ) -> SchemaMetadataClass:
        """Create schema metadata using json_schema_util.py."""
        try:
            return get_schema_metadata(
                platform=self.platform,
                name=dataset_name,
                json_schema=schema,
                raw_schema_string=json.dumps(schema, indent=2),
            )
        except Exception as e:
            logger.warning(
                f"Error creating schema metadata for {dataset_name}: {str(e)}"
            )
            # Fallback to empty schema metadata
            return SchemaMetadataClass(
                schemaName=dataset_name,
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=[],
            )

    def init_dataset(
        self, endpoint_k: str, endpoint_dets: dict
    ) -> Tuple[str, str, List[MetadataWorkUnit]]:
        config = self.config
        workunits = []

        dataset_name = endpoint_k[1:].replace("/", ".")

        if len(dataset_name) > 0:
            if dataset_name[-1] == ".":
                dataset_name = dataset_name[:-1]
        else:
            dataset_name = "root"

        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{config.name}.{dataset_name},PROD)"

        # Create dataset properties aspect
        properties = DatasetPropertiesClass(
            description=endpoint_dets["description"], customProperties={}
        )
        wu = MetadataWorkUnit(
            id=dataset_name,
            mcp=MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=properties),
        )
        workunits.append(wu)

        # Create tags aspect
        tags_str = [make_tag_urn(t) for t in endpoint_dets["tags"]]
        tags_tac = [TagAssociationClass(t) for t in tags_str]
        gtc = GlobalTagsClass(tags_tac)
        wu = MetadataWorkUnit(
            id=f"{dataset_name}-tags",
            mcp=MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=gtc),
        )
        workunits.append(wu)

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
        wu = MetadataWorkUnit(
            id=f"{dataset_name}-docs",
            mcp=MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=inst_memory
            ),
        )
        workunits.append(wu)

        # Create subtype aspect
        sub_types = SubTypesClass(typeNames=[DatasetSubTypes.API_ENDPOINT])
        wu = MetadataWorkUnit(
            id=f"{dataset_name}-subtype",
            mcp=MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=sub_types),
        )
        workunits.append(wu)

        return dataset_name, dataset_urn, workunits

    def _extract_schema_from_openapi_spec(
        self, endpoint_k: str, dataset_name: str, sw_dict: Dict
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema from OpenAPI specification if enabled."""
        if not self.config.use_schema_extraction:
            return None

        path_spec = sw_dict["paths"].get(endpoint_k, {})
        get_spec = path_spec.get("get", {})

        if get_spec:
            response_schema = self.extract_response_schema_from_endpoint(
                get_spec, sw_dict
            )
            if response_schema:
                schema_metadata = self.create_schema_metadata_from_schema(
                    dataset_name, response_schema
                )
                logger.info(f"Extracted schema from OpenAPI spec for {dataset_name}")
                return schema_metadata
        return None

    def _extract_schema_from_endpoint_data(
        self, endpoint_dets: Dict, dataset_name: str
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema from endpoint data if available."""
        if "data" in endpoint_dets:
            return set_metadata(dataset_name, endpoint_dets["data"])
        return None

    def _make_api_request(self, url: str) -> Optional[requests.Response]:
        """Make API request with appropriate authentication."""
        if self.config.token:
            return request_call(
                url,
                token=self.config.token,
                proxies=self.config.proxies,
                verify_ssl=self.config.verify_ssl,
            )
        else:
            return request_call(
                url,
                username=self.config.username,
                password=self.config.password,
                proxies=self.config.proxies,
                verify_ssl=self.config.verify_ssl,
            )

    def _extract_schema_from_simple_endpoint(
        self, endpoint_k: str, dataset_name: str, root_dataset_samples: Dict
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema from simple endpoint (no parameters)."""
        tot_url = clean_url(self.config.url + self.url_basepath + endpoint_k)
        response = self._make_api_request(tot_url)

        if response and response.status_code == 200:
            fields2add, root_dataset_samples[dataset_name] = extract_fields(
                response, dataset_name
            )
            if not fields2add:
                self.report.info(
                    message="No fields found from endpoint response.",
                    context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                )
            return set_metadata(dataset_name, fields2add)
        elif response:
            self.report_bad_responses(response.status_code, type=endpoint_k)
        return None

    def _extract_schema_from_parameterized_endpoint(
        self, endpoint_k: str, dataset_name: str, root_dataset_samples: Dict
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema from parameterized endpoint."""
        if endpoint_k not in self.config.forced_examples:
            # Try guessing
            url_guess = try_guessing(endpoint_k, root_dataset_samples)
            tot_url = clean_url(self.config.url + self.url_basepath + url_guess)
            response = self._make_api_request(tot_url)

            if response and response.status_code == 200:
                fields2add, _ = extract_fields(response, dataset_name)
                if not fields2add:
                    self.report.info(
                        message="No fields found from endpoint response.",
                        context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                    )
                return set_metadata(dataset_name, fields2add)
            elif response:
                self.report_bad_responses(response.status_code, type=endpoint_k)
        else:
            # Use forced examples
            composed_url = compose_url_attr(
                raw_url=endpoint_k, attr_list=self.config.forced_examples[endpoint_k]
            )
            tot_url = clean_url(self.config.url + self.url_basepath + composed_url)
            response = self._make_api_request(tot_url)

            if response and response.status_code == 200:
                fields2add, _ = extract_fields(response, dataset_name)
                if not fields2add:
                    self.report.info(
                        message="No fields found from endpoint response.",
                        context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                    )
                return set_metadata(dataset_name, fields2add)
            elif response:
                self.report_bad_responses(response.status_code, type=endpoint_k)
        return None

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        config = self.config
        sw_dict = self.config.get_swagger()
        self.url_basepath = get_url_basepath(sw_dict)

        # Getting all the URLs accepting the "GET" method
        with warnings.catch_warnings(record=True) as warn_c:
            url_endpoints = get_endpoints(sw_dict)
            for w in warn_c:
                w_msg = w.message
                w_spl = w_msg.args[0].split(" --- ")  # type: ignore
                self.report.report_warning(message=w_spl[1], context=w_spl[0])

        # Sample from "listing endpoint" for guessing composed endpoints
        root_dataset_samples: Dict[str, Any] = {}

        # Process all endpoints
        for endpoint_k, endpoint_dets in url_endpoints.items():
            if endpoint_k in config.ignore_endpoints:
                continue

            # Initialize dataset and get common aspects
            dataset_name, dataset_urn, workunits = self.init_dataset(
                endpoint_k, endpoint_dets
            )
            for wu in workunits:
                yield wu

            # Try to extract schema metadata
            schema_metadata = None

            # First try OpenAPI spec extraction
            schema_metadata = self._extract_schema_from_openapi_spec(
                endpoint_k, dataset_name, sw_dict
            )

            # If not found, try endpoint data
            if not schema_metadata:
                schema_metadata = self._extract_schema_from_endpoint_data(
                    endpoint_dets, dataset_name
                )

            # If still not found, try API calls
            if not schema_metadata:
                if endpoint_dets["method"] != "get":
                    self.report.report_warning(
                        title="Failed to Extract Endpoint Metadata",
                        message=f"No example provided for {endpoint_dets['method']}",
                        context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                    )
                    continue

                # Try simple endpoint first
                if "{" not in endpoint_k:
                    schema_metadata = self._extract_schema_from_simple_endpoint(
                        endpoint_k, dataset_name, root_dataset_samples
                    )
                else:
                    # Try parameterized endpoint
                    schema_metadata = self._extract_schema_from_parameterized_endpoint(
                        endpoint_k, dataset_name, root_dataset_samples
                    )

            # Yield the schema metadata work unit
            if schema_metadata:
                wu = MetadataWorkUnit(
                    id=f"{dataset_name}-schema",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=schema_metadata
                    ),
                )
                yield wu

    def get_report(self):
        return self.report


class OpenApiSource(APISource):
    def __init__(self, config: OpenApiConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "OpenApi")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OpenApiConfig.parse_obj(config_dict)
        return cls(config, ctx)
