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
)

import pydantic
import requests
from deprecated import deprecated
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError, RequestException

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
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.request_helper import make_curl_command
from datahub.emitter.response_helper import (
    TraceData,
    extract_trace_data,
    extract_trace_data_from_mcps,
)
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.api.closeable import Closeable
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation

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


class RestTraceMode(ConfigEnum):
    ENABLED = auto()
    DISABLED = auto()


class RestSinkEndpoint(ConfigEnum):
    RESTLI = auto()
    OPENAPI = auto()


DEFAULT_REST_SINK_ENDPOINT = pydantic.parse_obj_as(
    RestSinkEndpoint,
    os.getenv("DATAHUB_REST_SINK_DEFAULT_ENDPOINT", RestSinkEndpoint.RESTLI),
)


# Supported with v1.0
DEFAULT_REST_TRACE_MODE = pydantic.parse_obj_as(
    RestTraceMode,
    os.getenv("DATAHUB_REST_TRACE_MODE", RestTraceMode.DISABLED),
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

    def build_session(self) -> requests.Session:
        session = requests.Session()

        if self.extra_headers:
            session.headers.update(self.extra_headers)

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
    _openapi_ingestion: bool
    _default_trace_mode: bool

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
        openapi_ingestion: bool = False,
        default_trace_mode: bool = False,
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
        self.server_config: Dict[str, Any] = {}
        self._openapi_ingestion = openapi_ingestion
        self._default_trace_mode = default_trace_mode
        self._session = requests.Session()

        logger.debug(
            f"Using {'OpenAPI' if self._openapi_ingestion else 'Restli'} for ingestion."
        )

        if self._default_trace_mode:
            logger.debug("Using API Tracing for ingestion.")

        headers = {
            "X-RestLi-Protocol-Version": "2.0.0",
            "X-DataHub-Py-Cli-Version": nice_version_name(),
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
        )

        self._session = self._session_config.build_session()

    def test_connection(self) -> None:
        url = f"{self._gms_server}/config"
        response = self._session.get(url)
        if response.status_code == 200:
            config: dict = response.json()
            if config.get("noCode") == "true":
                self.server_config = config
                return

            else:
                raise ConfigurationError(
                    "You seem to have connected to the frontend service instead of the GMS endpoint. "
                    "The rest emitter should connect to DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms). "
                    "For Acryl users, the endpoint should be https://<name>.acryl.io/gms"
                )
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

    def get_server_config(self) -> dict:
        self.test_connection()
        return self.server_config

    def to_graph(self) -> "DataHubGraph":
        from datahub.ingestion.graph.client import DataHubGraph

        return DataHubGraph.from_emitter(self)

    def _to_openapi_request(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        async_flag: Optional[bool] = None,
        async_default: bool = False,
    ) -> Optional[Tuple[str, List[Dict[str, Any]]]]:
        if mcp.aspect and mcp.aspectName:
            resolved_async_flag = (
                async_flag if async_flag is not None else async_default
            )
            url = f"{self._gms_server}/openapi/v3/entity/{mcp.entityType}?async={'true' if resolved_async_flag else 'false'}"

            if isinstance(mcp, MetadataChangeProposalWrapper):
                aspect_value = pre_json_transform(
                    mcp.to_obj(simplified_structure=True)
                )["aspect"]["json"]
            else:
                obj = mcp.aspect.to_obj()
                if obj.get("value") and obj.get("contentType") == JSON_CONTENT_TYPE:
                    obj = json.loads(obj["value"])
                aspect_value = pre_json_transform(obj)
            return (
                url,
                [
                    {
                        "urn": mcp.entityUrn,
                        mcp.aspectName: {
                            "value": aspect_value,
                            "systemMetadata": mcp.systemMetadata.to_obj()
                            if mcp.systemMetadata
                            else None,
                        },
                    }
                ],
            )
        return None

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ],
        callback: Optional[Callable[[Exception, str], None]] = None,
        async_flag: Optional[bool] = None,
    ) -> None:
        try:
            if isinstance(item, UsageAggregation):
                self.emit_usage(item)
            elif isinstance(
                item, (MetadataChangeProposal, MetadataChangeProposalWrapper)
            ):
                self.emit_mcp(item, async_flag=async_flag)
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

    def emit_mcp(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        async_flag: Optional[bool] = None,
        trace_flag: Optional[bool] = None,
        trace_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> None:
        ensure_has_system_metadata(mcp)

        trace_data = None

        if self._openapi_ingestion:
            request = self._to_openapi_request(mcp, async_flag, async_default=False)
            if request:
                response = self._emit_generic(request[0], payload=request[1])

                if self._should_trace(async_flag, trace_flag):
                    trace_data = extract_trace_data(response) if response else None

        else:
            url = f"{self._gms_server}/aspects?action=ingestProposal"

            mcp_obj = pre_json_transform(mcp.to_obj())
            payload_dict = {"proposal": mcp_obj}

            if async_flag is not None:
                payload_dict["async"] = "true" if async_flag else "false"

            payload = json.dumps(payload_dict)

            response = self._emit_generic(url, payload)

            if self._should_trace(async_flag, trace_flag):
                trace_data = (
                    extract_trace_data_from_mcps(response, [mcp]) if response else None
                )

        if trace_data:
            self._await_status(
                [trace_data],
                trace_timeout,
            )

    def emit_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        async_flag: Optional[bool] = None,
        trace_flag: Optional[bool] = None,
        trace_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> int:
        if _DATAHUB_EMITTER_TRACE:
            logger.debug(f"Attempting to emit MCP batch of size {len(mcps)}")

        for mcp in mcps:
            ensure_has_system_metadata(mcp)

        if self._openapi_ingestion:
            return self._emit_openapi_mcps(mcps, async_flag, trace_flag, trace_timeout)
        else:
            return self._emit_restli_mcps(mcps, async_flag)

    def _emit_openapi_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        async_flag: Optional[bool] = None,
        trace_flag: Optional[bool] = None,
        trace_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> int:
        """
        1. Grouping MCPs by their entity URL
        2. Breaking down large batches into smaller chunks based on both:
         * Total byte size (INGEST_MAX_PAYLOAD_BYTES)
         * Maximum number of items (BATCH_INGEST_MAX_PAYLOAD_LENGTH)

        The Chunk class encapsulates both the items and their byte size tracking
        Serializing the items only once with json.dumps(request[1]) and reusing that
        The chunking logic handles edge cases (always accepting at least one item per chunk)
        The joining logic is efficient with a simple string concatenation

        :param mcps: metadata change proposals to transmit
        :param async_flag: the mode
        :return: number of requests
        """
        # group by entity url
        batches: Dict[str, List[_Chunk]] = defaultdict(
            lambda: [_Chunk(items=[])]
        )  # Initialize with one empty Chunk

        for mcp in mcps:
            request = self._to_openapi_request(mcp, async_flag, async_default=True)
            if request:
                current_chunk = batches[request[0]][-1]  # Get the last chunk
                # Only serialize once
                serialized_item = json.dumps(request[1][0])
                item_bytes = len(serialized_item.encode())

                # If adding this item would exceed max_bytes, create a new chunk
                # Unless the chunk is empty (always add at least one item)
                if current_chunk.items and (
                    current_chunk.total_bytes + item_bytes > INGEST_MAX_PAYLOAD_BYTES
                    or len(current_chunk.items) >= BATCH_INGEST_MAX_PAYLOAD_LENGTH
                ):
                    new_chunk = _Chunk(items=[])
                    batches[request[0]].append(new_chunk)
                    current_chunk = new_chunk

                current_chunk.add_item(serialized_item)

        responses = []
        for url, chunks in batches.items():
            for chunk in chunks:
                response = self._emit_generic(url, payload=_Chunk.join(chunk))
                responses.append(response)

        if self._should_trace(async_flag, trace_flag, async_default=True):
            trace_data = []
            for response in responses:
                data = extract_trace_data(response) if response else None
                if data is not None:
                    trace_data.append(data)

            if trace_data:
                self._await_status(trace_data, trace_timeout)

        return len(responses)

    def _emit_restli_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        async_flag: Optional[bool] = None,
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
            payload_dict: dict = {"proposals": mcp_obj_chunk}
            if async_flag is not None:
                payload_dict["async"] = "true" if async_flag else "false"

            payload = json.dumps(payload_dict)
            self._emit_generic(url, payload)

        return len(mcp_obj_chunks)

    @deprecated
    def emit_usage(self, usageStats: UsageAggregation) -> None:
        url = f"{self._gms_server}/usageStats?action=batchIngest"

        raw_usage_obj = usageStats.to_obj()
        usage_obj = pre_json_transform(raw_usage_obj)

        snapshot = {"buckets": [usage_obj]}
        payload = json.dumps(snapshot)
        self._emit_generic(url, payload)

    def _emit_generic(self, url: str, payload: Union[str, Any]) -> requests.Response:
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        curl_command = make_curl_command(self._session, "POST", url, payload)
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
            response = self._session.post(url, data=payload)
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
        trace_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> None:
        """Verify the status of asynchronous write operations.
        Args:
            trace_data: List of trace data to verify
            trace_timeout: Maximum time to wait for verification.
        Raises:
            TraceTimeoutError: If verification fails or times out
            TraceValidationError: Expected write was not completed successfully
        """
        if trace_timeout is None:
            raise ValueError("trace_timeout cannot be None")

        try:
            if not trace_data:
                logger.debug("No trace data to verify")
                return

            start_time = datetime.now()

            for trace in trace_data:
                current_backoff = TRACE_INITIAL_BACKOFF

                while trace.data:
                    if datetime.now() - start_time > trace_timeout:
                        raise TraceTimeoutError(
                            f"Timeout waiting for async write completion after {trace_timeout.total_seconds()} seconds"
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

    def _should_trace(
        self,
        async_flag: Optional[bool] = None,
        trace_flag: Optional[bool] = None,
        async_default: bool = False,
    ) -> bool:
        resolved_trace_flag = (
            trace_flag if trace_flag is not None else self._default_trace_mode
        )
        resolved_async_flag = async_flag if async_flag is not None else async_default
        return resolved_trace_flag and resolved_async_flag

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
