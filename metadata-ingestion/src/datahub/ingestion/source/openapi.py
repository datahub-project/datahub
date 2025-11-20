import json
import logging
import warnings
from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from pydantic import model_validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn
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
    SCHEMA_EXTRACTABLE_METHODS,
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
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    SubTypesClass,
    TagAssociationClass,
)

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class SchemaExtractionStats:
    """Statistics tracking for schema extraction from different sources."""

    from_openapi_spec: int = 0
    from_api_calls: int = 0
    from_endpoint_data: int = 0
    no_schema_found: int = 0

    def total(self) -> int:
        """Calculate total number of endpoints processed."""
        return (
            self.from_openapi_spec
            + self.from_api_calls
            + self.from_endpoint_data
            + self.no_schema_found
        )


class OpenApiConfig(ConfigModel):
    """
    Configuration for OpenAPI source ingestion.

    This class defines all the configuration parameters needed to ingest OpenAPI specifications
    and extract dataset metadata from API endpoints.

    Schema Extraction Behavior:
    - OpenAPI spec extraction always occurs (parsing the specification file)
    - Example data extraction always occurs (from examples in the spec)
    - Live API calls only occur if enable_api_calls_for_schema_extraction=True
    - API calls are only made for GET methods with valid credentials
    """

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
    enable_api_calls_for_schema_extraction: bool = Field(
        default=True,  # TODO This should be false by default, but set to true for backwards compatibility
        description="If True, will make live GET API calls to extract schemas when OpenAPI spec extraction fails. "
        "Requires credentials (username/password, token, or bearer_token). "
        "Only applicable for GET methods.",
    )
    schema_resolution_max_depth: int = Field(
        default=10,
        description="Maximum recursion depth for resolving schema references. "
        "Prevents infinite recursion from deeply nested or circular references. "
        "Default is 10 levels.",
    )

    @model_validator(mode="after")
    def ensure_only_one_token(self) -> "OpenApiConfig":
        if self.bearer_token is not None and self.token is not None:
            raise ValueError("Unable to use 'token' and 'bearer_token' together.")
        return self

    def get_swagger(self) -> Dict:
        """
        Fetch and parse the OpenAPI specification from the configured endpoint.

        This method handles different authentication methods and retrieves the OpenAPI spec
        from the configured URL and swagger file path.

        Returns:
            Dictionary containing the parsed OpenAPI specification

        Raises:
            KeyError: If invalid token retrieval method is specified
            AssertionError: If required URL complement parameters are missing
        """
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
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Extracts schemas from OpenAPI specifications for GET, POST, PUT, and PATCH methods",
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Extracts endpoint descriptions and summaries from OpenAPI specifications",
)
@capability(
    SourceCapability.TAGS,
    "Extracts tags from OpenAPI specifications",
)
@capability(
    SourceCapability.OWNERSHIP,
    "Does not currently support extracting ownership",
    supported=False,
)
@capability(
    SourceCapability.DOMAINS,
    "Does not currently support domain assignment",
    supported=False,
)
class APISource(Source, ABC):
    """

    This plugin is meant to gather dataset-like information about OpenApi Endpoints.

    The plugin focuses on extracting schemas from OpenAPI specifications for GET, POST, PUT, and PATCH
    methods with 200 response codes. It prioritizes schema extraction from the OpenAPI spec over
    making actual API calls.

    API calls are only made for GET methods when credentials are provided (username/password, token,
    bearer_token, or get_token configuration). This ensures safe and authenticated access to endpoints.

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
        self.schema_extraction_stats = SchemaExtractionStats()

    def report_bad_responses(self, status_code: int, type: str) -> None:
        """
        Report bad HTTP responses with appropriate warning messages.

        This method categorizes different HTTP error codes and reports them with
        meaningful messages to help users understand what went wrong.

        Args:
            status_code: HTTP status code from the API response
            type: Endpoint type or identifier for context

        Raises:
            Exception: For unhandled status codes
        """
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

    def extract_response_schema_from_endpoint(
        self, endpoint_spec: Dict, sw_dict: Dict
    ) -> Optional[Dict]:
        """
        Extract the response schema from an endpoint specification.

        This method looks for schema definitions in the 200 response of an endpoint.
        It handles both OpenAPI v2 (Swagger) and v3 formats.

        Args:
            endpoint_spec: The endpoint specification containing responses
            sw_dict: The complete OpenAPI specification dictionary

        Returns:
            Extracted schema dictionary if found, None otherwise

        Note:
            Only looks for 200 response codes and application/json content types
        """
        try:
            # Get all responses
            responses = endpoint_spec.get("responses", {})

            # Focus on 200 response code
            success_response = responses.get("200") or responses.get(200)

            if not success_response:
                return None

            # Extract schema from response
            if "content" in success_response:
                # OpenAPI v3 format
                content = success_response["content"]
                # Try application/json first, then fallback to others
                for content_type in [
                    "application/json",
                    "application/xml",
                    "text/json",
                ]:
                    if content_type in content:
                        schema = content[content_type].get("schema")
                        if schema:
                            return get_schema_from_response(
                                schema,
                                sw_dict,
                                max_depth=self.config.schema_resolution_max_depth,
                            )
            elif "schema" in success_response:
                # Swagger v2 format
                schema = success_response["schema"]
                return get_schema_from_response(
                    schema, sw_dict, max_depth=self.config.schema_resolution_max_depth
                )

            return None
        except (KeyError, TypeError, AttributeError) as e:
            logger.warning(f"Error extracting response schema: {str(e)}", exc_info=True)
            return None

    def extract_request_schema_from_endpoint(
        self, endpoint_spec: Dict, sw_dict: Dict
    ) -> Optional[Dict]:
        """Extract the request schema from an endpoint specification."""
        try:
            # Check for request body (OpenAPI v3)
            if "requestBody" in endpoint_spec:
                request_body = endpoint_spec["requestBody"]
                if "content" in request_body:
                    content = request_body["content"]
                    for content_type in [
                        "application/json",
                        "application/xml",
                        "text/json",
                    ]:
                        if content_type in content:
                            schema = content[content_type].get("schema")
                            if schema:
                                return get_schema_from_response(
                                    schema,
                                    sw_dict,
                                    max_depth=self.config.schema_resolution_max_depth,
                                )

            # Check for parameters (both v2 and v3)
            parameters = endpoint_spec.get("parameters", [])
            if parameters:
                # Create a schema from parameters
                param_schema: Dict[str, Any] = {"type": "object", "properties": {}}
                for param in parameters:
                    if isinstance(param, dict):
                        param_name = param.get("name", "")
                        param_schema_obj = param.get("schema", {})
                        if param_name and param_schema_obj:
                            param_schema["properties"][param_name] = param_schema_obj

                if param_schema["properties"]:
                    return param_schema

            return None
        except Exception as e:
            logger.warning(f"Error extracting request schema: {str(e)}")
            return None

    def extract_schema_from_all_methods(
        self, endpoint_k: str, sw_dict: Dict
    ) -> Optional[Dict]:
        """Extract schema from GET, POST, PUT, PATCH methods for an endpoint."""
        path_spec = sw_dict["paths"].get(endpoint_k, {})

        # Focus on the HTTP methods that can provide useful schemas
        methods = SCHEMA_EXTRACTABLE_METHODS

        for method in methods:
            method_spec = path_spec.get(method, {})
            if method_spec:
                # Try response schema first
                response_schema = self.extract_response_schema_from_endpoint(
                    method_spec, sw_dict
                )
                if response_schema:
                    return response_schema

                # If no response schema, try request schema for POST/PUT/PATCH
                if method in ["post", "put", "patch"]:
                    request_schema = self.extract_request_schema_from_endpoint(
                        method_spec, sw_dict
                    )
                    if request_schema:
                        return request_schema

        return None

    def create_schema_metadata_from_schema(
        self, dataset_name: str, schema: Dict
    ) -> Optional[SchemaMetadataClass]:
        """
        This method converts a JSON schema into DataHub's SchemaMetadataClass format.
        It uses the json_schema_util module to handle the conversion and field extraction.

        Args:
            dataset_name: Name of the dataset/endpoint
            schema: JSON schema dictionary to convert

        Returns:
            SchemaMetadataClass instance with extracted field information

        Note:
            Falls back to empty schema metadata if conversion fails
        """
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
            # Return None instead of empty schema - no schema aspect is better than empty one
            return None

    def init_dataset(
        self, endpoint_k: str, endpoint_dets: dict
    ) -> Tuple[str, str, List[MetadataWorkUnit]]:
        """
        Initialize a dataset with basic metadata aspects.

        This method creates the foundational metadata for an API endpoint dataset,
        including properties, tags, documentation links, and subtypes.

        Args:
            endpoint_k: The endpoint path/key
            endpoint_dets: Endpoint details containing description and tags

        Returns:
            Tuple containing:
            - dataset_name: Normalized dataset name
            - dataset_urn: Unique resource identifier for the dataset
            - workunits: List of metadata work units for the dataset

        Note:
            Creates dataset properties, tags, documentation links, and subtype aspects
        """
        config = self.config
        workunits = []

        # Create dataset name with braces (for display name)
        dataset_name_display = endpoint_k[1:].replace("/", ".")
        if len(dataset_name_display) > 0:
            if dataset_name_display[-1] == ".":
                dataset_name_display = dataset_name_display[:-1]
        else:
            dataset_name_display = "root"

        # Create dataset name without braces (for URN)
        dataset_name_urn = dataset_name_display.replace("{", "").replace("}", "")

        # Clean config name for URN (replace spaces with underscores)
        config_name_urn = config.name.replace(" ", "_")

        dataset_urn = make_dataset_urn(
            platform=self.platform,
            name=f"{config_name_urn}.{dataset_name_urn}",
            env="PROD",
        )

        # Create dataset properties aspect with display name (keeping braces)
        properties = DatasetPropertiesClass(
            name=dataset_name_display,
            description=endpoint_dets["description"],
            customProperties={},
        )
        wu = MetadataWorkUnit(
            id=dataset_name_urn,
            mcp=MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=properties),
        )
        workunits.append(wu)

        # Create tags aspect
        tags_str = [make_tag_urn(t) for t in endpoint_dets["tags"]]
        tags_tac = [TagAssociationClass(t) for t in tags_str]
        gtc = GlobalTagsClass(tags_tac)
        wu = MetadataWorkUnit(
            id=f"{dataset_name_urn}-tags",
            mcp=MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=gtc),
        )
        workunits.append(wu)

        # Create subtype aspect
        sub_types = SubTypesClass(typeNames=[DatasetSubTypes.API_ENDPOINT])
        wu = MetadataWorkUnit(
            id=f"{dataset_name_urn}-subtype",
            mcp=MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=sub_types),
        )
        workunits.append(wu)

        # Create browse paths v2 aspect
        # Parse endpoint path and create browse path segments
        # Include both path segments and parameter placeholders (with braces removed)
        # Example: /endpoint/{variable1}/{variable2}/ -> ["config_name", "endpoint", "variable1", "variable2"]
        # Example: /path/{variable1}/endpoint -> ["config_name", "path", "variable1", "endpoint"]
        browse_path_entries = []
        # Add config name as the top level of the browse path
        browse_path_entries.append(BrowsePathEntryClass(id=config_name_urn))

        if endpoint_k:
            # Remove leading and trailing slashes
            path = endpoint_k.strip("/")
            if path:
                # Split by / to get segments
                segments = path.split("/")
                for segment in segments:
                    if segment:
                        # Remove braces from parameter names (e.g., {value1} -> value1)
                        clean_segment = segment.replace("{", "").replace("}", "")
                        if clean_segment:
                            browse_path_entries.append(
                                BrowsePathEntryClass(id=clean_segment)
                            )

        # Only create browse path if we have entries (we always have at least config_name_urn)
        if browse_path_entries:
            browse_paths_v2 = BrowsePathsV2Class(path=browse_path_entries)
            wu = MetadataWorkUnit(
                id=f"{dataset_name_urn}-browse-paths",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=browse_paths_v2
                ),
            )
            workunits.append(wu)

        return dataset_name_urn, dataset_urn, workunits

    def _extract_schema_from_openapi_spec(
        self, endpoint_k: str, dataset_name: str, sw_dict: Dict
    ) -> Optional[SchemaMetadataClass]:
        """
        Extract schema from OpenAPI specification.

        This is the primary method for schema extraction, attempting to extract
        schemas directly from the OpenAPI specification without making API calls.

        Args:
            endpoint_k: The endpoint path/key
            dataset_name: Name of the dataset/endpoint
            sw_dict: The complete OpenAPI specification dictionary

        Returns:
            SchemaMetadataClass if schema found and extracted, None otherwise

        Note:
            Tracks statistics for reporting
        """

        # Try to extract schema from all methods for this endpoint
        schema = self.extract_schema_from_all_methods(endpoint_k, sw_dict)

        if schema:
            schema_metadata = self.create_schema_metadata_from_schema(
                dataset_name, schema
            )
            logger.info(
                f"Successfully extracted schema from OpenAPI spec for {dataset_name}"
            )
            self.report.info(
                message="Schema extracted from OpenAPI specification",
                context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
            )
            self.schema_extraction_stats.from_openapi_spec += 1
            return schema_metadata
        else:
            logger.debug(f"No schema found in OpenAPI spec for {dataset_name}")

        return None

    def _extract_schema_from_endpoint_data(
        self, endpoint_dets: Dict, dataset_name: str
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema from endpoint data if available."""
        if "data" in endpoint_dets:
            # Extract fields from the example data using flatten2list
            from datahub.ingestion.source.openapi_parser import flatten2list

            fields = flatten2list(endpoint_dets["data"])
            if fields:
                self.schema_extraction_stats.from_endpoint_data += 1
                return set_metadata(
                    dataset_name, fields, original_data=endpoint_dets["data"]
                )
        return None

    def _has_credentials(self) -> bool:
        """Check if any form of authentication credentials are provided."""
        return (
            bool(self.config.username and self.config.password)
            or bool(self.config.token)
            or bool(self.config.bearer_token)
            or bool(self.config.get_token)
        )

    def _should_make_api_call(self, endpoint_k: str, endpoint_dets: Dict) -> bool:
        """
        Determine if we should make an API call based on configuration and endpoint details.

        This method implements the logic for when API calls are allowed:
        - API calls must be explicitly enabled
        - Method must be GET (for safety)
        - Credentials must be provided
        - Endpoint must not be in ignore list

        Args:
            endpoint_k: The endpoint path/key
            endpoint_dets: Endpoint details including method information

        Returns:
            True if API call should be made, False otherwise

        Note:
            This ensures API calls are only made when safe and necessary
        """
        # Don't make API calls if not explicitly enabled
        if not self.config.enable_api_calls_for_schema_extraction:
            return False

        # Only make API calls for GET methods
        method = endpoint_dets.get("method", "").lower()
        if method != "get":
            return False

        # Only make API calls if credentials are provided
        if not self._has_credentials():
            return False

        # Don't make API calls if endpoint is in ignore list
        if endpoint_k in self.config.ignore_endpoints:
            return False

        # Only make API calls if we have forced examples or no schema was found
        # This should be rare with improved schema extraction
        return True

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
        """Extract schema from simple endpoint (no parameters) - only if necessary."""
        # Only make API calls if absolutely necessary
        if not self._should_make_api_call(endpoint_k, {"method": "get"}):
            return None

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
            else:
                self.schema_extraction_stats.from_api_calls += 1
            return set_metadata(dataset_name, fields2add)
        elif response:
            self.report_bad_responses(response.status_code, type=endpoint_k)
        return None

    def _extract_schema_from_parameterized_endpoint(
        self, endpoint_k: str, dataset_name: str, root_dataset_samples: Dict
    ) -> Optional[SchemaMetadataClass]:
        """
        Extract schema from parameterized endpoint - only if necessary.

        This method handles endpoints with path parameters by either using forced examples
        or guessing parameter values based on previously collected samples.

        Args:
            endpoint_k: The endpoint path/key (may contain {parameter} placeholders)
            dataset_name: Name of the dataset/endpoint
            root_dataset_samples: Dictionary containing sample data for parameter guessing

        Returns:
            SchemaMetadataClass if schema extracted successfully, None otherwise

        Note:
            Uses forced_examples configuration or tries to guess parameter values
            Tracks statistics for reporting
        """
        # Only make API calls if absolutely necessary
        if not self._should_make_api_call(endpoint_k, {"method": "get"}):
            return None

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
                else:
                    self.schema_extraction_stats.from_api_calls += 1
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
                else:
                    self.schema_extraction_stats.from_api_calls += 1
                return set_metadata(dataset_name, fields2add)
            elif response:
                self.report_bad_responses(response.status_code, type=endpoint_k)
        return None

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main processing method that generates metadata work units for all endpoints.

        This is the core method that orchestrates the entire ingestion process:
        1. Fetches and parses the OpenAPI specification
        2. Extracts all endpoints with their metadata
        3. For each endpoint, attempts schema extraction in priority order:
           - OpenAPI specification (primary)
           - Example data from spec (secondary)
           - API calls (fallback, only for GET with credentials)
        4. Generates metadata work units for DataHub

        Yields:
            MetadataWorkUnit instances for each endpoint and its aspects

        Note:
            Implements the prioritized schema extraction strategy
            Tracks statistics for final reporting
        """
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

            # Try to extract schema metadata - always attempt spec extraction first
            schema_metadata = None

            # Always try OpenAPI spec extraction first
            schema_metadata = self._extract_schema_from_openapi_spec(
                endpoint_k, dataset_name, sw_dict
            )

            # If not found, always try endpoint data from spec
            if not schema_metadata:
                schema_metadata = self._extract_schema_from_endpoint_data(
                    endpoint_dets, dataset_name
                )

            # Only make API calls as a last resort and only if explicitly enabled
            if not schema_metadata and self._should_make_api_call(
                endpoint_k, endpoint_dets
            ):
                method = endpoint_dets.get("method", "").lower()
                if method != "get":
                    self.report.report_warning(
                        title="Failed to Extract Endpoint Metadata",
                        message=f"No schema found in OpenAPI spec for {endpoint_dets.get('method', 'unknown')} method (API calls only made for GET methods with credentials)",
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
            else:
                # Log when no schema could be extracted
                self.schema_extraction_stats.no_schema_found += 1

                # Check if we could have made an API call but didn't due to missing credentials
                method = endpoint_dets.get("method", "").lower()
                if (
                    method == "get"
                    and self.config.enable_api_calls_for_schema_extraction
                    and not self._has_credentials()
                ):
                    self.report.report_warning(
                        title="No Schema Extracted - Missing Credentials",
                        message="Could not extract schema from OpenAPI spec and no API call made due to missing credentials (GET methods only)",
                        context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                    )
                else:
                    self.report.report_warning(
                        title="No Schema Extracted",
                        message="Could not extract schema from OpenAPI spec (GET/POST/PUT/PATCH with 200 responses) or API calls for endpoint",
                        context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                    )

    def get_report(self):
        return self.report

    def close(self) -> None:
        """
        Close the source and log the final schema extraction summary.

        This method is called once at the end of ingestion, ensuring the summary
        is only displayed once in the logs.
        """
        # Log schema extraction statistics summary at the end of ingestion
        total_endpoints = self.schema_extraction_stats.total()

        if total_endpoints > 0:
            openapi_percentage = (
                self.schema_extraction_stats.from_openapi_spec / total_endpoints
            ) * 100
            api_calls_percentage = (
                self.schema_extraction_stats.from_api_calls / total_endpoints
            ) * 100

            self.report.info(
                message=f"Schema extraction summary: {self.schema_extraction_stats.from_openapi_spec} from OpenAPI spec ({openapi_percentage:.1f}%), "
                f"{self.schema_extraction_stats.from_api_calls} from API calls ({api_calls_percentage:.1f}%), "
                f"{self.schema_extraction_stats.from_endpoint_data} from endpoint data, "
                f"{self.schema_extraction_stats.no_schema_found} no schema found"
            )

        # Call parent close to ensure proper cleanup
        super().close()


class OpenApiSource(APISource):
    """
    OpenAPI source implementation for DataHub ingestion.

    This class provides the concrete implementation of the OpenAPI source,
    configured specifically for OpenAPI specifications.
    """

    def __init__(self, config: OpenApiConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "OpenApi")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OpenApiConfig.model_validate(config_dict)
        return cls(config, ctx)
