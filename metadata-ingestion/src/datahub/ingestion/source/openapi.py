import json
import logging
import threading
from abc import ABC
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Literal, Optional, Tuple

import requests
from pydantic import SecretStr, field_validator, model_validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel, TransparentSecretStr
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
from datahub.ingestion.extractor.json_schema_util import get_schema_metadata
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.openapi_parser import (
    SCHEMA_EXTRACTABLE_METHODS,
    clean_url,
    compose_url_attr,
    extract_fields,
    flatten2list,
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

_RESPONSE_CONTENT_TYPES = ("application/json", "application/xml", "text/json")

# openapi_parser's schema-walking functions (get_endpoints, merge_allof_schemas,
# resolve_schema_references, ...) are free functions with no SourceReport access,
# so malformed/unusual-input degradation there (a skipped path item, an unresolved
# $ref, a lossy patternProperties collapse) only ever reaches the Python logger --
# never the ingestion report an operator actually looks at. Bridging its logger
# output into the report (below) surfaces that without threading a report/callback
# parameter through every one of those functions.
_PARSER_LOGGER_NAME = "datahub.ingestion.source.openapi_parser"


class _CollectingLogHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: List[str] = []
        # Multiple APISource instances can run get_workunits_internal
        # concurrently (e.g. parallel recipes in one process), each attaching
        # its own handler to the same shared openapi_parser logger. Without
        # this, every handler would see every thread's records, mis-
        # attributing another run's parsing warnings to this one's report.
        self._owner_thread = threading.get_ident()

    def emit(self, record: logging.LogRecord) -> None:
        if threading.get_ident() == self._owner_thread:
            self.messages.append(record.getMessage())


@contextmanager
def _capture_parser_warnings() -> Iterator[List[str]]:
    handler = _CollectingLogHandler()
    parser_logger = logging.getLogger(_PARSER_LOGGER_NAME)
    # logger.warning(...) short-circuits on isEnabledFor(WARNING), resolved
    # via the ancestor chain, before any handler (including this one) ever
    # sees the record. Force this logger's own level down for the duration
    # of the capture so an ambient config that raised some parent logger's
    # threshold above WARNING can't silently make this bridge report nothing
    # while real degradation is still happening.
    # Only force it down when needed (effective level suppresses WARNING) --
    # never override a MORE permissive level a user explicitly set (e.g. -v/
    # --debug), which would otherwise hide their DEBUG/INFO output for the
    # duration of the capture.
    previous_level = parser_logger.level
    lowered_level = parser_logger.getEffectiveLevel() > logging.WARNING
    if lowered_level:
        parser_logger.setLevel(logging.WARNING)
    parser_logger.addHandler(handler)
    try:
        yield handler.messages
    finally:
        parser_logger.removeHandler(handler)
        if lowered_level:
            parser_logger.setLevel(previous_level)


_BAD_RESPONSE_MESSAGES: Dict[int, Tuple[str, str]] = {
    400: (
        "Failed to Extract Metadata",
        "Bad request body when retrieving data from OpenAPI endpoint",
    ),
    401: (
        "Unauthorized to Extract Metadata",
        "Authentication failed when retrieving data from OpenAPI endpoint; check credentials or token",
    ),
    403: (
        "Unauthorized to Extract Metadata",
        "Received unauthorized response when attempting to retrieve data from OpenAPI endpoint",
    ),
    404: (
        "Failed to Extract Metadata",
        "Unable to find an example for endpoint. Please add it to the list of forced examples.",
    ),
    429: (
        "Failed to Extract Metadata",
        "Rate limited when retrieving data from OpenAPI endpoint; retry later or reduce request volume",
    ),
    500: (
        "Failed to Extract Metadata",
        "Received unknown server error from OpenAPI endpoint",
    ),
    504: (
        "Failed to Extract Metadata",
        "Timed out when attempting to retrieve data from OpenAPI endpoint",
    ),
}


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


class OpenApiGetTokenConfig(ConfigModel):
    request_type: Literal["get", "post"] = Field(
        description="HTTP method used to retrieve an auth token."
    )
    url_complement: str = Field(
        description="Path appended to the base URL for the token request. "
        "For request_type=get, must contain {username} and {password} placeholders."
    )

    @model_validator(mode="after")
    def validate_get_placeholders(self) -> "OpenApiGetTokenConfig":
        if self.request_type == "get":
            if "{username}" not in self.url_complement:
                raise ValueError(
                    "When request_type is 'get', url_complement must contain {username}"
                )
            if "{password}" not in self.url_complement:
                raise ValueError(
                    "When request_type is 'get', url_complement must contain {password}"
                )
        return self


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
    ignore_endpoints: List[str] = Field(
        default_factory=list,
        description="List of endpoints to ignore during ingestion.",
    )
    username: str = Field(
        default="", description="Username used for basic HTTP authentication."
    )
    password: TransparentSecretStr = Field(
        default=SecretStr(""),
        description="Password used for basic HTTP authentication.",
    )
    proxies: Optional[Dict[str, str]] = Field(
        default=None,
        description="Eg. "
        "`{'http': 'http://10.10.1.10:3128', 'https': 'http://10.10.1.10:1080'}`."
        "If authentication is required, add it to the proxy url directly e.g. "
        "`http://user:pass@10.10.1.10:3128/`.",
    )
    forced_examples: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Path-parameter examples keyed by endpoint path. Values may be "
        "scalars (str/int/float; bool is accepted via int) and are stringified for "
        "URL composition.",
    )
    token: Optional[TransparentSecretStr] = Field(
        default=None, description="Token for endpoint authentication."
    )
    bearer_token: Optional[TransparentSecretStr] = Field(
        default=None, description="Bearer token for endpoint authentication."
    )
    get_token: Optional[OpenApiGetTokenConfig] = Field(
        default=None, description="Retrieving a token from the endpoint."
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
        ge=1,
        le=100,
        description="Maximum recursion depth for resolving schema references. "
        "Prevents infinite recursion from deeply nested or circular references. "
        "Default is 10 levels; capped at 100 to avoid RecursionError.",
    )

    @field_validator("get_token", mode="before")
    @classmethod
    def empty_get_token_to_none(cls, value: Any) -> Any:
        # Recipes historically used get_token: {} to mean "unset".
        if value == {} or value is None:
            return None
        return value

    @field_validator("forced_examples", mode="before")
    @classmethod
    def stringify_forced_examples(cls, value: Any) -> Any:
        # Docs/recipes use numeric path params (e.g. /pet/{petId}: [1]).
        # Only coerce documented scalars — leave null/objects untouched so List[str]
        # validation rejects them instead of silently emitting "None" in URLs.
        if not isinstance(value, dict):
            return value
        coerced: Dict[str, Any] = {}
        for endpoint, examples in value.items():
            if not isinstance(examples, list):
                coerced[endpoint] = examples
                continue
            coerced[endpoint] = [
                # bool is a subclass of int, so it is covered without listing bool.
                str(item) if isinstance(item, (str, int, float)) else item
                for item in examples
            ]
        return coerced

    @model_validator(mode="after")
    def ensure_only_one_token(self) -> "OpenApiConfig":
        configured = [
            name
            for name, value in (
                ("token", self.token),
                ("bearer_token", self.bearer_token),
                ("get_token", self.get_token),
            )
            if value is not None
        ]
        if len(configured) > 1:
            raise ValueError(
                "Unable to use "
                + ", ".join(repr(name) for name in configured)
                + " together; configure only one of 'token', 'bearer_token', or 'get_token'."
            )
        return self

    def get_swagger(self) -> Dict:
        """
        Fetch and parse the OpenAPI specification from the configured endpoint.

        This method handles different authentication methods and retrieves the OpenAPI spec
        from the configured URL and swagger file path.

        Returns:
            Dictionary containing the parsed OpenAPI specification

        Raises:
            ValueError: If token retrieval fails or the token response has an
                unexpected shape (get_token's request_type/url_complement are
                validated at config construction time by OpenApiGetTokenConfig).
        """
        # Truthiness (not `is not None`) for all three: an empty SecretStr("") is
        # falsy, and treating it as "configured" here would fall through to the
        # `else` below and hit `assert self.get_token is not None` with neither
        # get_token nor a real token/bearer_token actually set.
        if self.get_token or self.token or self.bearer_token:
            if self.token:
                pass
            elif self.bearer_token:
                # TRICKY: To avoid passing a bunch of different token types around, we set the
                # token's value to the properly formatted bearer token.
                # TODO: We should just create a requests.Session and set all the auth
                # details there once, and then use that session for all requests.
                self.token = SecretStr(f"Bearer {self.bearer_token.get_secret_value()}")
            else:
                assert self.get_token is not None
                if self.get_token.request_type == "get":
                    url4req = self.get_token.url_complement.replace(
                        "{username}", self.username
                    )
                    url4req = url4req.replace(
                        "{password}", self.password.get_secret_value()
                    )
                else:
                    url4req = self.get_token.url_complement
                self.token = SecretStr(
                    get_tok(
                        url=self.url,
                        username=self.username,
                        password=self.password.get_secret_value(),
                        tok_url=url4req,
                        method=self.get_token.request_type,
                        proxies=self.proxies,
                        verify_ssl=self.verify_ssl,
                    )
                )
            sw_dict = get_swag_json(
                self.url,
                token=self.token.get_secret_value(),
                swagger_file=self.swagger_file,
                proxies=self.proxies,
                verify_ssl=self.verify_ssl,
            )  # load the swagger file

        else:  # using basic auth for accessing endpoints
            sw_dict = get_swag_json(
                self.url,
                username=self.username,
                password=self.password.get_secret_value(),
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
@capability(SourceCapability.TAGS, "Extracts tags from OpenAPI specifications")
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
    Source that extracts API endpoint metadata from OpenAPI v2/v3 specifications.

    Implementation notes:
    - Uses openapi_parser module for spec parsing and schema extraction
    - Supports multi-step schema extraction: spec → examples → live API calls (GET only)
    - Represents endpoints as datasets with API_ENDPOINT subtype
    - Optional authenticated API calls controlled by enable_api_calls_for_schema_extraction
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
        """
        context = f"Endpoint Type: {type}, Status Code: {status_code}"
        title, message = _BAD_RESPONSE_MESSAGES.get(
            status_code,
            (
                "Failed to Extract Metadata",
                "Unexpected HTTP status when retrieving data from OpenAPI endpoint",
            ),
        )
        self.report.warning(title=title, message=message, context=context, log=False)

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
                for content_type in _RESPONSE_CONTENT_TYPES:
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
            # self.report.warning already logs this (WARN level, plus the full
            # traceback at DEBUG via exc=e) -- a separate logger.warning call
            # here would just duplicate it.
            self.report.warning(
                title="Failed to Extract Response Schema",
                message="Error extracting response schema from OpenAPI endpoint",
                context=str(e),
                exc=e,
            )
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
                    for content_type in _RESPONSE_CONTENT_TYPES:
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
        except (KeyError, TypeError, AttributeError) as e:
            self.report.warning(
                title="Failed to Extract Request Schema",
                message="Error extracting request schema from OpenAPI endpoint",
                context=str(e),
                exc=e,
            )
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
            SchemaMetadataClass instance with extracted field information, or None
            if conversion fails (no schema aspect is emitted).
        """
        try:
            # Do not swallow jsonref/jsonschema failures — empty fields would otherwise
            # look like a successful extract and inflate from_openapi_spec stats.
            return get_schema_metadata(
                platform=self.platform,
                name=dataset_name,
                json_schema=schema,
                raw_schema_string=json.dumps(schema, indent=2),
                swallow_exceptions=False,
            )
        except Exception as e:
            # A warning, not a failure: the caller (_process_endpoint) still tries
            # example-data and live-API fallback after this returns None, and a
            # successful fallback would otherwise leave a sticky run failure behind
            # even though the endpoint's schema was ultimately extracted.
            self.report.warning(
                title="Failed to Create Schema Metadata",
                message="Error creating schema metadata from OpenAPI schema",
                context=f"Dataset: {dataset_name}",
                exc=e,
            )
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
            if not schema_metadata:
                return None
            if not schema_metadata.fields:
                # A schema that resolves without error but yields zero fields (e.g. a
                # map-only response with no explicit "type": "object") is not a
                # successful extraction — counting it as one hides the gap from users.
                self.report.warning(
                    title="Schema Extracted With No Fields",
                    message="OpenAPI spec schema resolved but produced no extractable fields",
                    context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                    log=False,
                )
                return None
            # self.report.info already logs this at INFO level -- a separate
            # logger.info call here would just duplicate it.
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
            example_data = endpoint_dets["data"]
            # The OpenAPI "example" value is free-form JSON, so it may legitimately
            # be a list (e.g. an array-typed response: "example": [{"id": 1}, ...])
            # or a bare scalar rather than an object. flatten2list only understands
            # a dict of fields and calls .items() unconditionally -- passing it
            # anything else raises AttributeError/TypeError, which aborts this
            # endpoint's entire processing (see get_workunits_internal's per-endpoint
            # except) instead of falling through to the next extraction strategy.
            if isinstance(example_data, list):
                example_data = (
                    example_data[0]
                    if example_data and isinstance(example_data[0], dict)
                    else None
                )
            if not isinstance(example_data, dict):
                return None

            fields = flatten2list(example_data)
            if fields:
                self.schema_extraction_stats.from_endpoint_data += 1
                return set_metadata(dataset_name, fields, original_data=example_data)
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
        """Return True when a live GET is allowed for schema extraction fallback.

        Requires enable_api_calls_for_schema_extraction, GET method, credentials,
        and an endpoint not listed in ignore_endpoints. Callers also gate on
        failed OpenAPI/example extraction before invoking this.
        """
        if not self.config.enable_api_calls_for_schema_extraction:
            return False

        method = endpoint_dets.get("method", "").lower()
        if method != "get":
            return False

        if not self._has_credentials():
            return False

        if endpoint_k in self.config.ignore_endpoints:
            return False

        return True

    def _make_api_request(self, url: str) -> Optional[requests.Response]:
        """Make API request with appropriate authentication."""
        try:
            if self.config.token:
                return request_call(
                    url,
                    token=self.config.token.get_secret_value(),
                    proxies=self.config.proxies,
                    verify_ssl=self.config.verify_ssl,
                )
            return request_call(
                url,
                username=self.config.username,
                password=self.config.password.get_secret_value(),
                proxies=self.config.proxies,
                verify_ssl=self.config.verify_ssl,
            )
        except requests.exceptions.RequestException as e:
            self.report.warning(
                title="Failed to Call OpenAPI Endpoint",
                message="HTTP request to OpenAPI endpoint failed",
                context=url,
                exc=e,
            )
            return None

    def _schema_from_api_response(
        self,
        endpoint_k: str,
        dataset_name: str,
        tot_url: str,
        root_dataset_samples: Optional[Dict] = None,
    ) -> Optional[SchemaMetadataClass]:
        response = self._make_api_request(tot_url)
        if response and response.status_code == 200:
            fields2add, sample = extract_fields(response, dataset_name)
            if root_dataset_samples is not None:
                root_dataset_samples[dataset_name] = sample
            if not fields2add:
                # Matches the sibling extractors (_extract_schema_from_openapi_spec,
                # _extract_schema_from_endpoint_data): a zero-field result is not a
                # successful extraction. Returning a schema anyway would emit an
                # empty-fields aspect and leave this endpoint counted in neither
                # from_api_calls nor no_schema_found, undercounting the run's stats.
                self.report.info(
                    message="No fields found from endpoint response.",
                    context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                )
                return None
            self.schema_extraction_stats.from_api_calls += 1
            return set_metadata(dataset_name, fields2add)
        if response:
            self.report_bad_responses(response.status_code, type=endpoint_k)
        return None

    def _extract_schema_from_simple_endpoint(
        self, endpoint_k: str, dataset_name: str, root_dataset_samples: Dict
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema from simple endpoint (no parameters) - only if necessary."""
        # Caller already gated on _should_make_api_call + GET.
        tot_url = clean_url(self.config.url + self.url_basepath + endpoint_k)
        return self._schema_from_api_response(
            endpoint_k, dataset_name, tot_url, root_dataset_samples
        )

    def _extract_schema_from_parameterized_endpoint(
        self, endpoint_k: str, dataset_name: str, root_dataset_samples: Dict
    ) -> Optional[SchemaMetadataClass]:
        """
        Extract schema from parameterized endpoint - only if necessary.

        Handles path parameters via forced_examples or guessing from prior samples.
        """
        # Caller already gated on _should_make_api_call + GET.
        if endpoint_k not in self.config.forced_examples:
            url_guess = try_guessing(endpoint_k, root_dataset_samples)
            tot_url = clean_url(self.config.url + self.url_basepath + url_guess)
            return self._schema_from_api_response(endpoint_k, dataset_name, tot_url)

        composed_url = compose_url_attr(
            raw_url=endpoint_k, attr_list=self.config.forced_examples[endpoint_k]
        )
        tot_url = clean_url(self.config.url + self.url_basepath + composed_url)
        return self._schema_from_api_response(endpoint_k, dataset_name, tot_url)

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
        with _capture_parser_warnings() as parser_warnings:
            # try/finally (not two explicit call sites) so parser warnings
            # captured before an early return AND warnings captured before a
            # consumer stops draining this generator early (a GeneratorExit
            # at the `yield from` below skips everything after it) both still
            # reach the report exactly once.
            try:
                try:
                    sw_dict = self.config.get_swagger()
                    self.url_basepath = get_url_basepath(sw_dict)
                    url_endpoints = get_endpoints(sw_dict)
                except Exception as e:
                    # get_url_basepath/get_endpoints are included here (not
                    # just get_swagger) because a spec that fetches/parses
                    # fine but is missing an expected key (e.g. "paths")
                    # would otherwise raise an unhandled KeyError and crash
                    # the whole run instead of surfacing a report failure.
                    #
                    # Attaching the real exception is safe: get_tok (the only
                    # place a get_token password is ever substituted into a
                    # URL) already sanitizes every exception it raises down
                    # to response.status_code / exception type, with no
                    # url4req/password in the message -- see
                    # test_get_tok_error_does_not_leak_credentials_in_message
                    # and test_get_tok_connection_error_does_not_leak_credentials_in_message.
                    # Hiding it behind a generic RuntimeError would just
                    # deprive operators of the real cause (401, parse error,
                    # missing "paths", TLS failure).
                    self.report.failure(
                        title="Failed to Fetch OpenAPI Spec",
                        message="Unable to retrieve, parse, or interpret the OpenAPI specification",
                        context=f"{config.url} / {config.swagger_file}",
                        exc=e,
                    )
                    return

                # Sample from "listing endpoint" for guessing composed endpoints
                root_dataset_samples: Dict[str, Any] = {}

                # Process all endpoints. Materialize each endpoint's workunits
                # inside the try so consumer-side failures at yield are not
                # mis-attributed here.
                for endpoint_k, endpoint_dets in url_endpoints.items():
                    if endpoint_k in config.ignore_endpoints:
                        continue

                    try:
                        workunits = list(
                            self._process_endpoint(
                                endpoint_k, endpoint_dets, sw_dict, root_dataset_samples
                            )
                        )
                    except Exception as e:
                        self.report.failure(
                            title="Failed to Process Endpoint",
                            message="Unexpected error while processing OpenAPI endpoint",
                            context=endpoint_k,
                            exc=e,
                        )
                        continue

                    yield from workunits
            finally:
                self._report_parser_warnings(parser_warnings)

    def _report_parser_warnings(self, parser_warnings: List[str]) -> None:
        # Each message is already logged once by the parser's own logger.warning
        # call (log=False here avoids duplicating that), and is typically
        # per-entity-specific (includes the malformed value/endpoint), so dedup
        # only collapses genuinely repeated messages.
        #
        # Title is deliberately neutral ("Warning", not "Malformed"): these
        # come from a mix of spec-structural issues (a skipped path item, an
        # unresolved $ref) and live-API response issues (a non-JSON response
        # during the API-call fallback) -- calling every one of them a
        # malformed *spec* entry would misdescribe the latter, and purely
        # informational parser messages are logged at INFO so they never
        # reach this WARNING-level capture in the first place.
        for message in dict.fromkeys(parser_warnings):
            self.report.warning(
                title="OpenAPI Parsing Warning",
                message=message,
                context=f"{self.config.url} / {self.config.swagger_file}",
                log=False,
            )

    def _process_endpoint(
        self,
        endpoint_k: str,
        endpoint_dets: Dict[str, Any],
        sw_dict: Dict[str, Any],
        root_dataset_samples: Dict[str, Any],
    ) -> Iterable[MetadataWorkUnit]:
        # Initialize dataset and get common aspects
        dataset_name, dataset_urn, workunits = self.init_dataset(
            endpoint_k, endpoint_dets
        )
        for wu in workunits:
            yield wu

        # Always try OpenAPI spec extraction first
        schema_metadata = self._extract_schema_from_openapi_spec(
            endpoint_k, dataset_name, sw_dict
        )

        # If not found, always try endpoint data from spec
        if not schema_metadata:
            schema_metadata = self._extract_schema_from_endpoint_data(
                endpoint_dets, dataset_name
            )

        # Only make API calls as a last resort and only if explicitly enabled.
        # _should_make_api_call already requires method == "get", so no separate
        # non-GET guard is needed here.
        if not schema_metadata and self._should_make_api_call(
            endpoint_k, endpoint_dets
        ):
            if "{" not in endpoint_k:
                schema_metadata = self._extract_schema_from_simple_endpoint(
                    endpoint_k, dataset_name, root_dataset_samples
                )
            else:
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
                self.report.warning(
                    title="No Schema Extracted - Missing Credentials",
                    message="Could not extract schema from OpenAPI spec and no API call made due to missing credentials (GET methods only)",
                    context=f"Endpoint Type: {endpoint_k}, Name: {dataset_name}",
                )
            else:
                self.report.warning(
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
