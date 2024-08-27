import functools
import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import requests
from deprecated import deprecated
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError, RequestException

from datahub.cli import config_utils
from datahub.cli.cli_utils import ensure_has_system_metadata, fixup_gms_url
from datahub.configuration.common import ConfigurationError, OperationalError
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.request_helper import make_curl_command
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

_DEFAULT_CONNECT_TIMEOUT_SEC = 30  # 30 seconds should be plenty to connect
_DEFAULT_READ_TIMEOUT_SEC = (
    30  # Any ingest call taking longer than 30 seconds should be abandoned
)
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


class DataHubRestEmitter(Closeable, Emitter):
    _gms_server: str
    _token: Optional[str]
    _session: requests.Session
    _connect_timeout_sec: float = _DEFAULT_CONNECT_TIMEOUT_SEC
    _read_timeout_sec: float = _DEFAULT_READ_TIMEOUT_SEC
    _retry_status_codes: List[int] = _DEFAULT_RETRY_STATUS_CODES
    _retry_methods: List[str] = _DEFAULT_RETRY_METHODS
    _retry_max_times: int = _DEFAULT_RETRY_MAX_TIMES

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
    ):
        if not gms_server:
            raise ConfigurationError("gms server is required")
        self._gms_server = fixup_gms_url(gms_server)
        self._token = token
        self.server_config: Dict[str, Any] = {}

        self._session = requests.Session()

        self._session.headers.update(
            {
                "X-RestLi-Protocol-Version": "2.0.0",
                "Content-Type": "application/json",
            }
        )
        if token:
            self._session.headers.update({"Authorization": f"Bearer {token}"})
        else:
            # HACK: When no token is provided but system auth env variables are set, we use them.
            # Ideally this should simply get passed in as config, instead of being sneakily injected
            # in as part of this constructor.
            # It works because everything goes through here. The DatahubGraph inherits from the
            # rest emitter, and the rest sink uses the rest emitter under the hood.
            system_auth = config_utils.get_system_auth()
            if system_auth is not None:
                self._session.headers.update({"Authorization": system_auth})

        if extra_headers:
            self._session.headers.update(extra_headers)

        if client_certificate_path:
            self._session.cert = client_certificate_path

        if ca_certificate_path:
            self._session.verify = ca_certificate_path

        if disable_ssl_verification:
            self._session.verify = False

        self._connect_timeout_sec = (
            connect_timeout_sec or timeout_sec or _DEFAULT_CONNECT_TIMEOUT_SEC
        )
        self._read_timeout_sec = (
            read_timeout_sec or timeout_sec or _DEFAULT_READ_TIMEOUT_SEC
        )

        if self._connect_timeout_sec < 1 or self._read_timeout_sec < 1:
            logger.warning(
                f"Setting timeout values lower than 1 second is not recommended. Your configuration is connect_timeout:{self._connect_timeout_sec}s, read_timeout:{self._read_timeout_sec}s"
            )

        if retry_status_codes is not None:  # Only if missing. Empty list is allowed
            self._retry_status_codes = retry_status_codes

        if retry_methods is not None:
            self._retry_methods = retry_methods

        if retry_max_times:
            self._retry_max_times = retry_max_times

        try:
            # Set raise_on_status to False to propagate errors:
            # https://stackoverflow.com/questions/70189330/determine-status-code-from-python-retry-exception
            # Must call `raise_for_status` after making a request, which we do
            retry_strategy = Retry(
                total=self._retry_max_times,
                status_forcelist=self._retry_status_codes,
                backoff_factor=2,
                allowed_methods=self._retry_methods,
                raise_on_status=False,
            )
        except TypeError:
            # Prior to urllib3 1.26, the Retry class used `method_whitelist` instead of `allowed_methods`.
            retry_strategy = Retry(
                total=self._retry_max_times,
                status_forcelist=self._retry_status_codes,
                backoff_factor=2,
                method_whitelist=self._retry_methods,
                raise_on_status=False,
            )

        adapter = HTTPAdapter(
            pool_connections=100, pool_maxsize=100, max_retries=retry_strategy
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        # Shim session.request to apply default timeout values.
        # Via https://stackoverflow.com/a/59317604.
        self._session.request = functools.partial(  # type: ignore
            self._session.request,
            timeout=(self._connect_timeout_sec, self._read_timeout_sec),
        )

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
    ) -> None:
        url = f"{self._gms_server}/aspects?action=ingestProposal"
        ensure_has_system_metadata(mcp)

        mcp_obj = pre_json_transform(mcp.to_obj())
        payload_dict = {"proposal": mcp_obj}

        if async_flag is not None:
            payload_dict["async"] = "true" if async_flag else "false"

        payload = json.dumps(payload_dict)

        self._emit_generic(url, payload)

    def emit_mcps(
        self,
        mcps: List[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        async_flag: Optional[bool] = None,
    ) -> None:
        url = f"{self._gms_server}/aspects?action=ingestProposalBatch"
        for mcp in mcps:
            ensure_has_system_metadata(mcp)

        mcp_objs = [pre_json_transform(mcp.to_obj()) for mcp in mcps]
        payload_dict: dict = {"proposals": mcp_objs}

        if async_flag is not None:
            payload_dict["async"] = "true" if async_flag else "false"

        payload = json.dumps(payload_dict)

        self._emit_generic(url, payload)

    @deprecated
    def emit_usage(self, usageStats: UsageAggregation) -> None:
        url = f"{self._gms_server}/usageStats?action=batchIngest"

        raw_usage_obj = usageStats.to_obj()
        usage_obj = pre_json_transform(raw_usage_obj)

        snapshot = {"buckets": [usage_obj]}
        payload = json.dumps(snapshot)
        self._emit_generic(url, payload)

    def _emit_generic(self, url: str, payload: str) -> None:
        curl_command = make_curl_command(self._session, "POST", url, payload)
        logger.debug(
            "Attempting to emit to DataHub GMS; using curl equivalent to:\n%s",
            curl_command,
        )
        try:
            response = self._session.post(url, data=payload)
            response.raise_for_status()
        except HTTPError as e:
            try:
                info: Dict = response.json()
                logger.debug(
                    "Full stack trace from DataHub:\n%s", info.get("stackTrace")
                )
                info.pop("stackTrace", None)
                raise OperationalError(
                    f"Unable to emit metadata to DataHub GMS: {info.get('message')}",
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
