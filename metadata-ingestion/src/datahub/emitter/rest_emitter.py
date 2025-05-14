from __future__ import annotations

import functools
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import auto
from json.decoder import JSONDecodeError
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

import pydantic
import requests
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError, RequestException
from typing_extensions import deprecated

from datahub._version import nice_version_name
from datahub.cli import config_utils
from datahub.cli.cli_utils import ensure_has_system_metadata, fixup_gms_url, get_or_else
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.configuration.common import (
    ConfigEnum,
    ConfigModel,
    ConfigurationError,
    OperationalError,
    TraceTimeoutError,
    TraceValidationError,
)
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.request_helper import OpenApiRequest, make_curl_command
from datahub.emitter.response_helper import (
    TraceData,
    extract_trace_data,
    extract_trace_data_from_mcps,
)
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.graph.config import (
    DATAHUB_COMPONENT_ENV,
    ClientMode,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation
from datahub.utilities.server_config_util import RestServiceConfig, ServiceFeature

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SEC = 30  # 30 seconds should be plenty to connect
_TIMEOUT_LOWER_BOUND_SEC = 1  # if below this, we log a warning
_DEFAULT_RETRY_STATUS_CODES = [  # Additional status codes to retry on
    429,
    500,
    502,
    503,
    504,
]
_DEFAULT_RETRY_METHODS = ["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
_DEFAULT_RETRY_MAX_TIMES = int(
    os.getenv("DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES", "4")
)

_DATAHUB_EMITTER_TRACE = get_boolean_env_variable("DATAHUB_EMITTER_TRACE", False)

_DEFAULT_CLIENT_MODE: ClientMode = ClientMode.SDK

TRACE_PENDING_STATUS = "PENDING"
TRACE_INITIAL_BACKOFF = 1.0  # Start with 1 second
TRACE_MAX_BACKOFF = 300.0  # Cap at 5 minutes
TRACE_BACKOFF_FACTOR = 2.0  # Double the wait time each attempt

# The limit is 16mb. We will use a max of 15mb to have some space
# for overhead like request headers.
# This applies to pretty much all calls to GMS.
INGEST_MAX_PAYLOAD_BYTES = 15 * 1024 * 1024

# This limit is somewhat arbitrary. All GMS endpoints will timeout
# and return a 500 if processing takes too long. To avoid sending
# too much to the backend and hitting a timeout, we try to limit
# the number of MCPs we send in a batch.
BATCH_INGEST_MAX_PAYLOAD_LENGTH = int(
    os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_LENGTH", 200)
)


class EmitMode(ConfigEnum):
    # Fully synchronous processing that updates both primary storage (SQL) and search storage (Elasticsearch) before returning.
    # Provides the strongest consistency guarantee but with the highest cost. Best for critical operations where immediate
    # searchability and consistent reads are required.
    SYNC_WAIT = auto()
    # Synchronously updates the primary storage (SQL) but asynchronously updates search storage (Elasticsearch). Provides
    # a balance between consistency and performance. Suitable for updates that need to be immediately reflected in direct
    # entity retrievals but where search index consistency can be slightly delayed.
    SYNC_PRIMARY = auto()
    # Queues the metadata change for asynchronous processing and returns immediately. The client continues execution without
    # waiting for the change to be fully processed. Best for high-throughput scenarios where eventual consistency is acceptable.
    ASYNC = auto()
    # Queues the metadata change asynchronously but blocks until confirmation that the write has been fully persisted.
    # More efficient than fully synchronous operations due to backend parallelization and batching while still providing
    # strong consistency guarantees. Useful when you need confirmation of successful persistence without sacrificing performance.
    ASYNC_WAIT = auto()


_DEFAULT_EMIT_MODE = pydantic.parse_obj_as(
    EmitMode,
    os.getenv("DATAHUB_EMIT_MODE", EmitMode.SYNC_PRIMARY),
)


class RestSinkEndpoint(ConfigEnum):
    RESTLI = auto()
    OPENAPI = auto()


DEFAULT_REST_EMITTER_ENDPOINT = pydantic.parse_obj_as(
    RestSinkEndpoint,
    os.getenv("DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT", RestSinkEndpoint.RESTLI),
)


class RequestsSessionConfig(ConfigModel):
    timeout: Union[float, Tuple[float, float], None] = _DEFAULT_TIMEOUT_SEC

    retry_status_codes: List[int] = _DEFAULT_RETRY_STATUS_CODES
    retry_methods: List[str] = _DEFAULT_RETRY_METHODS
    retry_max_times: int = _DEFAULT_RETRY_MAX_TIMES

    extra_headers: Dict[str, str] = {}

    ca_certificate_path: Optional[str] = None
    client_certificate_path: Optional[str] = None
    disable_ssl_verification: bool = False
    client_mode: Optional[ClientMode] = _DEFAULT_CLIENT_MODE
    datahub_component: Optional[str] = None

    def build_session(self) -> requests.Session:
        session = requests.Session()

        user_agent = self._get_user_agent_string(session)

        base_headers = {
            "User-Agent": user_agent,
            "X-DataHub-Client-Mode": self.client_mode.name
            if self.client_mode
            else _DEFAULT_CLIENT_MODE.name,
            "X-DataHub-Py-Cli-Version": nice_version_name(),
        }

        headers = {**base_headers, **self.extra_headers}
        session.headers.update(headers)

        if self.client_certificate_path:
            session.cert = self.client_certificate_path

        if self.ca_certificate_path:
            session.verify = self.ca_certificate_path

        if self.disable_ssl_verification:
            session.verify = False

        try:
            # Set raise_on_status to False to propagate errors:
            # https://stackoverflow.com/questions/70189330/determine-status-code-from-python-retry-exception
            # Must call `raise_for_status` after making a request, which we do
            retry_strategy = Retry(
                total=self.retry_max_times,
                status_forcelist=self.retry_status_codes,
                backoff_factor=2,
                allowed_methods=self.retry_methods,
                raise_on_status=False,
            )
        except TypeError:
            # Prior to urllib3 1.26, the Retry class used `method_whitelist` instead of `allowed_methods`.
            retry_strategy = Retry(
                total=self.retry_max_times,
                status_forcelist=self.retry_status_codes,
                backoff_factor=2,
                method_whitelist=self.retry_methods,
                raise_on_status=False,
            )

        adapter = HTTPAdapter(
            pool_connections=100, pool_maxsize=100, max_retries=retry_strategy
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if self.timeout is not None:
            # Shim session.request to apply default timeout values.
            # Via https://stackoverflow.com/a/59317604.
            session.request = functools.partial(  # type: ignore
                session.request,
                timeout=self.timeout,
            )

        return session

    @classmethod
    def get_client_mode_from_session(
        cls, session: requests.Session
    ) -> Optional[ClientMode]:
        """
        Extract the ClientMode enum from a requests Session by checking the headers.

        Args:
            session: The requests.Session object to check

        Returns:
            The corresponding ClientMode enum value if found, None otherwise
        """
        # Check if the session has the X-DataHub-Client-Mode header
        mode_str = session.headers.get("X-DataHub-Client-Mode")

        if not mode_str:
            return None

        # Try to convert the string value to enum
        try:
            # First ensure we're working with a str value
            if isinstance(mode_str, bytes):
                mode_str = mode_str.decode("utf-8")

            # Then find the matching enum value
            for mode in ClientMode:
                if mode.name == mode_str:
                    return mode

            # If we got here, no matching enum was found
            return None
        except Exception:
            # Handle any other errors
            return None

    def _get_user_agent_string(self, session: requests.Session) -> str:
        """Generate appropriate user agent string based on client mode"""
        version = nice_version_name()
        client_mode = self.client_mode if self.client_mode else _DEFAULT_CLIENT_MODE

        if "User-Agent" in session.headers:
            user_agent = session.headers["User-Agent"]
            if isinstance(user_agent, bytes):
                requests_user_agent = " " + user_agent.decode("utf-8")
            else:
                requests_user_agent = " " + user_agent
        else:
            requests_user_agent = ""

        # 1.0 refers to the user agent string version
        return f"DataHub-Client/1.0 ({client_mode.name.lower()}; {self.datahub_component if self.datahub_component else DATAHUB_COMPONENT_ENV}; {version}){requests_user_agent}"


@dataclass
class _Chunk:
    items: List[str]
    total_bytes: int = 0

    def add_item(self, item: str) -> bool:
        item_bytes = len(item.encode())
        if not self.items:  # Always add at least one item even if over byte limit
            self.items.append(item)
            self.total_bytes += item_bytes
            return True
        self.items.append(item)
        self.total_bytes += item_bytes
        return True

    @staticmethod
    def join(chunk: "_Chunk") -> str:
        return "[" + ",".join(chunk.items) + "]"


class DataHubRestEmitter(Closeable, Emitter):
    _gms_server: str
    _token: Optional[str]
    _session: requests.Session
    _openapi_ingestion: Optional[bool]
    _server_config: RestServiceConfig

    def __init__(
        self,
        gms_server: str,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        connect_timeout_sec: Optional[float] = None,
        read_timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_methods: Optional[List[str]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        ca_certificate_path: Optional[str] = None,
        client_certificate_path: Optional[str] = None,
        disable_ssl_verification: bool = False,
        openapi_ingestion: Optional[bool] = None,
        client_mode: Optional[ClientMode] = None,
        datahub_component: Optional[str] = None,
    ):
        if not gms_server:
            raise ConfigurationError("gms server is required")
        if gms_server == "__from_env__" and token is None:
            # HACK: similar to what we do with system auth, we transparently
            # inject the config in here. Ideally this should be done in the
            # config loader or by the caller, but it gets the job done for now.
            gms_server, token = config_utils.require_config_from_env()

        self._gms_server = fixup_gms_url(gms_server)
        self._token = token
        self._session = requests.Session()
        self._openapi_ingestion = (
            openapi_ingestion  # Re-evaluated after test connection
        )

        headers = {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        else:
            # HACK: When no token is provided but system auth env variables are set, we use them.
            # Ideally this should simply get passed in as config, instead of being sneakily injected
            # in as part of this constructor.
            # It works because everything goes through here. The DatahubGraph inherits from the
            # rest emitter, and the rest sink uses the rest emitter under the hood.
            system_auth = config_utils.get_system_auth()
            if system_auth is not None:
                headers["Authorization"] = system_auth

        timeout: float | tuple[float, float]
        if connect_timeout_sec is not None or read_timeout_sec is not None:
            timeout = (
                connect_timeout_sec or timeout_sec or _DEFAULT_TIMEOUT_SEC,
                read_timeout_sec or timeout_sec or _DEFAULT_TIMEOUT_SEC,
            )
            if (
                timeout[0] < _TIMEOUT_LOWER_BOUND_SEC
                or timeout[1] < _TIMEOUT_LOWER_BOUND_SEC
            ):
                logger.warning(
                    f"Setting timeout values lower than {_TIMEOUT_LOWER_BOUND_SEC} second is not recommended. Your configuration is (connect_timeout, read_timeout) = {timeout} seconds"
                )
        else:
            timeout = get_or_else(timeout_sec, _DEFAULT_TIMEOUT_SEC)
            if timeout < _TIMEOUT_LOWER_BOUND_SEC:
                logger.warning(
                    f"Setting timeout values lower than {_TIMEOUT_LOWER_BOUND_SEC} second is not recommended. Your configuration is timeout = {timeout} seconds"
                )

        self._session_config = RequestsSessionConfig(
            timeout=timeout,
            retry_status_codes=get_or_else(
                retry_status_codes, _DEFAULT_RETRY_STATUS_CODES
            ),
            retry_methods=get_or_else(retry_methods, _DEFAULT_RETRY_METHODS),
            retry_max_times=get_or_else(retry_max_times, _DEFAULT_RETRY_MAX_TIMES),
            extra_headers={**headers, **(extra_headers or {})},
            ca_certificate_path=ca_certificate_path,
            client_certificate_path=client_certificate_path,
            disable_ssl_verification=disable_ssl_verification,
            client_mode=client_mode,
            datahub_component=datahub_component,
        )

        self._session = self._session_config.build_session()

    @property
    def server_config(self) -> RestServiceConfig:
        return self.fetch_server_config()

    # TODO: This should move to DataHubGraph once it no longer inherits from DataHubRestEmitter
    def fetch_server_config(self) -> RestServiceConfig:
        """
        Fetch configuration from the server if not already loaded.

        Returns:
            The configuration dictionary

        Raises:
            ConfigurationError: If there's an error fetching or validating the configuration
        """
        if not hasattr(self, "_server_config") or not self._server_config:
            if self._session is None or self._gms_server is None:
                raise ConfigurationError(
                    "Session and URL are required to load configuration"
                )

            url = f"{self._gms_server}/config"
            response = self._session.get(url)

            if response.status_code == 200:
                raw_config = response.json()

                # Validate that we're connected to the correct service
                if not raw_config.get("noCode") == "true":
                    raise ConfigurationError(
                        "You seem to have connected to the frontend service instead of the GMS endpoint. "
                        "The rest emitter should connect to DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms). "
                        "For Acryl users, the endpoint should be https://<name>.acryl.io/gms"
                    )

                self._server_config = RestServiceConfig(raw_config=raw_config)
                self._post_fetch_server_config()

            else:
                logger.debug(
                    f"Unable to connect to {url} with status_code: {response.status_code}. Response: {response.text}"
                )

                if response.status_code == 401:
                    message = f"Unable to connect to {url} - got an authentication error: {response.text}."
                else:
                    message = f"Unable to connect to {url} with status_code: {response.status_code}."

                message += "\nPlease check your configuration and make sure you are talking to the DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms)."
                raise ConfigurationError(message)

        return self._server_config

    def _post_fetch_server_config(self) -> None:
        # Determine OpenAPI mode
        if self._openapi_ingestion is None:
            # No constructor parameter
            if (
                not os.getenv("DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT")
                and self._session_config.client_mode == ClientMode.SDK
                and self._server_config.supports_feature(ServiceFeature.OPEN_API_SDK)
            ):
                # Enable if SDK client and no environment variable specified
                self._openapi_ingestion = True
            else:
                # The system env is specifying the value
                self._openapi_ingestion = (
                    DEFAULT_REST_EMITTER_ENDPOINT == RestSinkEndpoint.OPENAPI
                )

        logger.debug(
            f"Using {'OpenAPI' if self._openapi_ingestion else 'Restli'} for ingestion."
        )
        logger.debug(
            f"{EmitMode.ASYNC_WAIT} {'IS' if self._should_trace(emit_mode=EmitMode.ASYNC_WAIT, warn=False) else 'IS NOT'} supported."
        )

    def test_connection(self) -> None:
        self.fetch_server_config()

    def get_server_config(self) -> dict:
        return self.server_config.raw_config

    def to_graph(self) -> "DataHubGraph":
        from datahub.ingestion.graph.client import DataHubGraph

        return DataHubGraph.from_emitter(self)

    def _to_openapi_request(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        emit_mode: EmitMode,
    ) -> Optional[OpenApiRequest]:
        """
        Convert a MetadataChangeProposal to an OpenAPI request format.

        Args:
            mcp: The metadata change proposal
            emit_mode: Client emit mode

        Returns:
            An OpenApiRequest object or None if the MCP doesn't have required fields
        """
        return OpenApiRequest.from_mcp(
            mcp=mcp,
            gms_server=self._gms_server,
            async_flag=emit_mode in (EmitMode.ASYNC, EmitMode.ASYNC_WAIT),
            search_sync_flag=emit_mode == EmitMode.SYNC_WAIT,
        )

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ],
        callback: Optional[Callable[[Exception, str], None]] = None,
        emit_mode: EmitMode = _DEFAULT_EMIT_MODE,
    ) -> None:
        try:
            if isinstance(item, UsageAggregation):
                self.emit_usage(item)
            elif isinstance(
                item, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            ):
                self.emit_mcp(item, emit_mode=emit_mode)
            else:
                self.emit_mce(item)
        except Exception as e:
            if callback:
                callback(e, str(e))
            raise
        else:
            if callback:
                callback(None, "success")  # type: ignore

    def emit_mce(self, mce: MetadataChangeEvent) -> None:
        url = f"{self._gms_server}/entities?action=ingest"

        raw_mce_obj = mce.proposedSnapshot.to_obj()
        mce_obj = pre_json_transform(raw_mce_obj)
        snapshot_fqn = (
            f"com.linkedin.metadata.snapshot.{mce.proposedSnapshot.RECORD_SCHEMA.name}"
        )
        ensure_has_system_metadata(mce)
        # To make lint happy
        assert mce.systemMetadata is not None
        system_metadata_obj = mce.systemMetadata.to_obj()
        snapshot = {
            "entity": {"value": {snapshot_fqn: mce_obj}},
            "systemMetadata": system_metadata_obj,
        }
        payload = json.dumps(snapshot)

        self._emit_generic(url, payload)

    @overload
    @deprecated("Use emit_mode instead of async_flag")
    def emit_mcp(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        *,
        async_flag: Optional[bool] = None,
    ) -> None: ...

    @overload
    def emit_mcp(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        *,
        emit_mode: EmitMode = _DEFAULT_EMIT_MODE,
        wait_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> None: ...

    def emit_mcp(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        async_flag: Optional[bool] = None,
        emit_mode: EmitMode = _DEFAULT_EMIT_MODE,
        wait_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> None:
        if async_flag is True:
            emit_mode = EmitMode.ASYNC

        ensure_has_system_metadata(mcp)

        trace_data = None

        if self._openapi_ingestion:
            request = self._to_openapi_request(mcp, emit_mode)
            if request:
                response = self._emit_generic(
                    request.url, payload=request.payload, method=request.method
                )

                if self._should_trace(emit_mode):
                    trace_data = extract_trace_data(response) if response else None

        else:
            url = f"{self._gms_server}/aspects?action=ingestProposal"

            mcp_obj = pre_json_transform(mcp.to_obj())
            payload_dict = {
                "proposal": mcp_obj,
                "async": "true"
                if emit_mode in (EmitMode.ASYNC, EmitMode.ASYNC_WAIT)
                else "false",
            }

            payload = json.dumps(payload_dict)

            response = self._emit_generic(url, payload)

            if self._should_trace(emit_mode):
                trace_data = (
                    extract_trace_data_from_mcps(response, [mcp]) if response else None
                )

        if trace_data:
            self._await_status(
                [trace_data],
                wait_timeout,
            )

    def emit_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        emit_mode: EmitMode = _DEFAULT_EMIT_MODE,
        wait_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> int:
        if _DATAHUB_EMITTER_TRACE:
            logger.debug(f"Attempting to emit MCP batch of size {len(mcps)}")

        for mcp in mcps:
            ensure_has_system_metadata(mcp)

        if self._openapi_ingestion:
            return self._emit_openapi_mcps(mcps, emit_mode, wait_timeout)
        else:
            return self._emit_restli_mcps(mcps, emit_mode)

    def _emit_openapi_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        emit_mode: EmitMode,
        wait_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> int:
        """
        1. Grouping MCPs by their HTTP method and entity URL and HTTP method
        2. Breaking down large batches into smaller chunks based on both:
         * Total byte size (INGEST_MAX_PAYLOAD_BYTES)
         * Maximum number of items (BATCH_INGEST_MAX_PAYLOAD_LENGTH)

        The Chunk class encapsulates both the items and their byte size tracking
        Serializing the items only once with json.dumps(request.payload) and reusing that
        The chunking logic handles edge cases (always accepting at least one item per chunk)
        The joining logic is efficient with a simple string concatenation

        :param mcps: metadata change proposals to transmit
        :param emit_mode: the mode to emit the MCPs
        :param wait_timeout: timeout for blocking queue
        :return: number of requests
        """
        # Group by entity URL and HTTP method
        batches: Dict[Tuple[str, str], List[_Chunk]] = defaultdict(
            lambda: [_Chunk(items=[])]
        )  # Initialize with one empty Chunk

        for mcp in mcps:
            request = self._to_openapi_request(mcp, emit_mode)
            if request:
                # Create a composite key with both method and URL
                key = (request.method, request.url)
                current_chunk = batches[key][-1]  # Get the last chunk

                # Only serialize once - we're serializing a single payload item
                serialized_item = json.dumps(request.payload[0])
                item_bytes = len(serialized_item.encode())

                # If adding this item would exceed max_bytes, create a new chunk
                # Unless the chunk is empty (always add at least one item)
                if current_chunk.items and (
                    current_chunk.total_bytes + item_bytes > INGEST_MAX_PAYLOAD_BYTES
                    or len(current_chunk.items) >= BATCH_INGEST_MAX_PAYLOAD_LENGTH
                ):
                    new_chunk = _Chunk(items=[])
                    batches[key].append(new_chunk)
                    current_chunk = new_chunk

                current_chunk.add_item(serialized_item)

        responses = []
        for (method, url), chunks in batches.items():
            for chunk in chunks:
                response = self._emit_generic(
                    url, payload=_Chunk.join(chunk), method=method
                )
                responses.append(response)

        if self._should_trace(emit_mode):
            trace_data = []
            for response in responses:
                data = extract_trace_data(response) if response else None
                if data is not None:
                    trace_data.append(data)

            if trace_data:
                self._await_status(trace_data, wait_timeout)

        return len(responses)

    def _emit_restli_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        emit_mode: EmitMode,
    ) -> int:
        url = f"{self._gms_server}/aspects?action=ingestProposalBatch"

        mcp_objs = [pre_json_transform(mcp.to_obj()) for mcp in mcps]

        # As a safety mechanism, we need to make sure we don't exceed the max payload size for GMS.
        # If we will exceed the limit, we need to break it up into chunks.
        mcp_obj_chunks: List[List[str]] = []
        current_chunk_size = INGEST_MAX_PAYLOAD_BYTES
        for mcp_obj in mcp_objs:
            mcp_obj_size = len(json.dumps(mcp_obj))
            if _DATAHUB_EMITTER_TRACE:
                logger.debug(
                    f"Iterating through object with size {mcp_obj_size} (type: {mcp_obj.get('aspectName')}"
                )

            if (
                mcp_obj_size + current_chunk_size > INGEST_MAX_PAYLOAD_BYTES
                or len(mcp_obj_chunks[-1]) >= BATCH_INGEST_MAX_PAYLOAD_LENGTH
            ):
                if _DATAHUB_EMITTER_TRACE:
                    logger.debug("Decided to create new chunk")
                mcp_obj_chunks.append([])
                current_chunk_size = 0
            mcp_obj_chunks[-1].append(mcp_obj)
            current_chunk_size += mcp_obj_size
        if len(mcp_obj_chunks) > 0:
            logger.debug(
                f"Decided to send {len(mcps)} MCP batch in {len(mcp_obj_chunks)} chunks"
            )

        for mcp_obj_chunk in mcp_obj_chunks:
            # TODO: We're calling json.dumps on each MCP object twice, once to estimate
            # the size when chunking, and again for the actual request.
            payload_dict: dict = {
                "proposals": mcp_obj_chunk,
                "async": "true"
                if emit_mode in (EmitMode.ASYNC, EmitMode.ASYNC_WAIT)
                else "false",
            }

            payload = json.dumps(payload_dict)
            self._emit_generic(url, payload)

        return len(mcp_obj_chunks)

    @deprecated("Use emit with a datasetUsageStatistics aspect instead")
    def emit_usage(self, usageStats: UsageAggregation) -> None:
        url = f"{self._gms_server}/usageStats?action=batchIngest"

        raw_usage_obj = usageStats.to_obj()
        usage_obj = pre_json_transform(raw_usage_obj)

        snapshot = {"buckets": [usage_obj]}
        payload = json.dumps(snapshot)
        self._emit_generic(url, payload)

    def _emit_generic(
        self, url: str, payload: Union[str, Any], method: str = "POST"
    ) -> requests.Response:
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        curl_command = make_curl_command(self._session, method, url, payload)
        payload_size = len(payload)
        if payload_size > INGEST_MAX_PAYLOAD_BYTES:
            # since we know total payload size here, we could simply avoid sending such payload at all and report a warning, with current approach we are going to cause whole ingestion to fail
            logger.warning(
                f"Apparent payload size exceeded {INGEST_MAX_PAYLOAD_BYTES}, might fail with an exception due to the size"
            )
        logger.debug(
            "Attempting to emit aspect (size: %s) to DataHub GMS; using curl equivalent to:\n%s",
            payload_size,
            curl_command,
        )
        try:
            method_func = getattr(self._session, method.lower())
            response = method_func(url, data=payload) if payload else method_func(url)
            response.raise_for_status()
            return response
        except HTTPError as e:
            try:
                info: Dict = response.json()

                if info.get("stackTrace"):
                    logger.debug(
                        "Full stack trace from DataHub:\n%s", info.get("stackTrace")
                    )
                    info.pop("stackTrace", None)

                hint = ""
                if "unrecognized field found but not allowed" in (
                    info.get("message") or ""
                ):
                    hint = ", likely because the server version is too old relative to the client"

                raise OperationalError(
                    f"Unable to emit metadata to DataHub GMS{hint}: {info.get('message')}",
                    info,
                ) from e
            except JSONDecodeError:
                # If we can't parse the JSON, just raise the original error.
                raise OperationalError(
                    "Unable to emit metadata to DataHub GMS", {"message": str(e)}
                ) from e
        except RequestException as e:
            raise OperationalError(
                "Unable to emit metadata to DataHub GMS", {"message": str(e)}
            ) from e

    def _await_status(
        self,
        trace_data: List[TraceData],
        wait_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> None:
        """Verify the status of asynchronous write operations.
        Args:
            trace_data: List of trace data to verify
            trace_timeout: Maximum time to wait for verification.
        Raises:
            TraceTimeoutError: If verification fails or times out
            TraceValidationError: Expected write was not completed successfully
        """
        if wait_timeout is None:
            raise ValueError("wait_timeout cannot be None")

        try:
            if not trace_data:
                logger.debug("No trace data to verify")
                return

            start_time = datetime.now()

            for trace in trace_data:
                current_backoff = TRACE_INITIAL_BACKOFF

                while trace.data:
                    if datetime.now() - start_time > wait_timeout:
                        raise TraceTimeoutError(
                            f"Timeout waiting for async write completion after {wait_timeout.total_seconds()} seconds"
                        )

                    base_url = f"{self._gms_server}/openapi/v1/trace/write"
                    url = f"{base_url}/{trace.trace_id}?onlyIncludeErrors=false&detailed=true"

                    response = self._emit_generic(url, payload=trace.data)
                    json_data = response.json()

                    for urn, aspects in json_data.items():
                        for aspect_name, aspect_status in aspects.items():
                            if not aspect_status["success"]:
                                error_msg = (
                                    f"Unable to validate async write to DataHub GMS: "
                                    f"Persistence failure for URN '{urn}' aspect '{aspect_name}'. "
                                    f"Status: {aspect_status}"
                                )
                                raise TraceValidationError(error_msg, aspect_status)

                            primary_storage = aspect_status["primaryStorage"][
                                "writeStatus"
                            ]
                            search_storage = aspect_status["searchStorage"][
                                "writeStatus"
                            ]

                            # Remove resolved statuses
                            if (
                                primary_storage != TRACE_PENDING_STATUS
                                and search_storage != TRACE_PENDING_STATUS
                            ):
                                trace.data[urn].remove(aspect_name)

                        # Remove urns with all statuses resolved
                        if not trace.data[urn]:
                            trace.data.pop(urn)

                    # Adjust backoff based on response
                    if trace.data:
                        # If we still have pending items, increase backoff
                        current_backoff = min(
                            current_backoff * TRACE_BACKOFF_FACTOR, TRACE_MAX_BACKOFF
                        )
                        logger.debug(
                            f"Waiting {current_backoff} seconds before next check"
                        )
                        time.sleep(current_backoff)

        except Exception as e:
            logger.error(f"Error during status verification: {str(e)}")
            raise

    def _should_trace(self, emit_mode: EmitMode, warn: bool = True) -> bool:
        if emit_mode == EmitMode.ASYNC_WAIT:
            if not bool(self._openapi_ingestion):
                if warn:
                    logger.warning(
                        f"{emit_mode} requested but is only available when using OpenAPI."
                    )
                return False
            elif getattr(
                self, "server_config", None
            ) is None or not self.server_config.supports_feature(
                ServiceFeature.API_TRACING
            ):
                if warn:
                    logger.warning(
                        f"{emit_mode} requested but is only available with a newer GMS version."
                    )
                return False
            else:
                return True
        else:
            return False

    def __repr__(self) -> str:
        token_str = (
            f" with token: {self._token[:4]}**********{self._token[-4:]}"
            if self._token
            else ""
        )
        return f"{self.__class__.__name__}: configured to talk to {self._gms_server}{token_str}"

    def flush(self) -> None:
        # No-op, but present to keep the interface consistent with the Kafka emitter.
        pass

    def close(self) -> None:
        self._session.close()


"""This class exists as a pass-through for backwards compatibility"""
DatahubRestEmitter = DataHubRestEmitter
