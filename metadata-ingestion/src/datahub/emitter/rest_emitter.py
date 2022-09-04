import datetime
import functools
import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError, RequestException

from datahub.cli.cli_utils import get_system_auth
from datahub.configuration.common import ConfigurationError, OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.request_helper import _make_curl_command
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation

logger = logging.getLogger(__name__)


class DataHubRestEmitter:
    DEFAULT_CONNECT_TIMEOUT_SEC = 30  # 30 seconds should be plenty to connect
    DEFAULT_READ_TIMEOUT_SEC = (
        30  # Any ingest call taking longer than 30 seconds should be abandoned
    )
    DEFAULT_RETRY_STATUS_CODES = [  # Additional status codes to retry on
        429,
        502,
        503,
        504,
    ]
    DEFAULT_RETRY_METHODS = ["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
    DEFAULT_RETRY_MAX_TIMES = int(
        os.getenv("DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES", "3")
    )

    _gms_server: str
    _token: Optional[str]
    _session: requests.Session
    _connect_timeout_sec: float = DEFAULT_CONNECT_TIMEOUT_SEC
    _read_timeout_sec: float = DEFAULT_READ_TIMEOUT_SEC
    _retry_status_codes: List[int] = DEFAULT_RETRY_STATUS_CODES
    _retry_methods: List[str] = DEFAULT_RETRY_METHODS
    _retry_max_times: int = DEFAULT_RETRY_MAX_TIMES

    def __init__(
        self,
        gms_server: str,
        token: Optional[str] = None,
        connect_timeout_sec: Optional[float] = None,
        read_timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_methods: Optional[List[str]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        ca_certificate_path: Optional[str] = None,
        server_telemetry_id: Optional[str] = None,
        disable_ssl_verification: bool = False,
    ):
        self._gms_server = gms_server
        self._token = token
        self.server_config: Dict[str, Any] = {}
        self.server_telemetry_id: str = ""

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
            system_auth = get_system_auth()
            if system_auth is not None:
                self._session.headers.update({"Authorization": system_auth})

        if extra_headers:
            self._session.headers.update(extra_headers)

        if ca_certificate_path:
            self._session.verify = ca_certificate_path

        if disable_ssl_verification:
            self._session.verify = False

        if connect_timeout_sec:
            self._connect_timeout_sec = connect_timeout_sec

        if read_timeout_sec:
            self._read_timeout_sec = read_timeout_sec

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
            retry_strategy = Retry(
                total=self._retry_max_times,
                status_forcelist=self._retry_status_codes,
                backoff_factor=2,
                allowed_methods=self._retry_methods,
            )
        except TypeError:
            # Prior to urllib3 1.26, the Retry class used `method_whitelist` instead of `allowed_methods`.
            retry_strategy = Retry(
                total=self._retry_max_times,
                status_forcelist=self._retry_status_codes,
                backoff_factor=2,
                method_whitelist=self._retry_methods,
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

    def test_connection(self) -> dict:
        response = self._session.get(f"{self._gms_server}/config")
        if response.status_code == 200:
            config: dict = response.json()
            if config.get("noCode") == "true":
                self.server_config = config
                return config

            else:
                # Looks like we either connected to an old GMS or to some other service. Let's see if we can determine which before raising an error
                # A common misconfiguration is connecting to datahub-frontend so we special-case this check
                if (
                    config.get("config", {}).get("application") == "datahub-frontend"
                    or config.get("config", {}).get("shouldShowDatasetLineage")
                    is not None
                ):
                    message = "You seem to have connected to the frontend instead of the GMS endpoint. The rest emitter should connect to DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms)"
                else:
                    message = "You have either connected to a pre-v0.8.0 DataHub GMS instance, or to a different server altogether! Please check your configuration and make sure you are talking to the DataHub GMS endpoint."
                raise ConfigurationError(message)
        else:
            auth_message = "Maybe you need to set up authentication? "
            message = f"Unable to connect to {self._gms_server}/config with status_code: {response.status_code}. {auth_message if response.status_code == 401 else ''}Please check your configuration and make sure you are talking to the DataHub GMS (usually <datahub-gms-host>:8080) or Frontend GMS API (usually <frontend>:9002/api/gms)."
            raise ConfigurationError(message)

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ],
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        start_time = datetime.datetime.now()
        if isinstance(item, UsageAggregation):
            self.emit_usage(item)
        elif isinstance(item, (MetadataChangeProposal, MetadataChangeProposalWrapper)):
            self.emit_mcp(item)
        else:
            self.emit_mce(item)
        return start_time, datetime.datetime.now()

    def emit_mce(self, mce: MetadataChangeEvent) -> None:
        url = f"{self._gms_server}/entities?action=ingest"

        raw_mce_obj = mce.proposedSnapshot.to_obj()
        mce_obj = pre_json_transform(raw_mce_obj)
        snapshot_fqn = (
            f"com.linkedin.metadata.snapshot.{mce.proposedSnapshot.RECORD_SCHEMA.name}"
        )
        system_metadata_obj = {}
        if mce.systemMetadata is not None:
            system_metadata_obj = {
                "lastObserved": mce.systemMetadata.lastObserved,
                "runId": mce.systemMetadata.runId,
            }
        snapshot = {
            "entity": {"value": {snapshot_fqn: mce_obj}},
            "systemMetadata": system_metadata_obj,
        }
        payload = json.dumps(snapshot)

        self._emit_generic(url, payload)

    def emit_mcp(
        self, mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper]
    ) -> None:
        url = f"{self._gms_server}/aspects?action=ingestProposal"

        mcp_obj = pre_json_transform(mcp.to_obj())
        payload = json.dumps({"proposal": mcp_obj})

        self._emit_generic(url, payload)

    def emit_usage(self, usageStats: UsageAggregation) -> None:
        url = f"{self._gms_server}/usageStats?action=batchIngest"

        raw_usage_obj = usageStats.to_obj()
        usage_obj = pre_json_transform(raw_usage_obj)

        snapshot = {
            "buckets": [
                usage_obj,
            ]
        }
        payload = json.dumps(snapshot)
        self._emit_generic(url, payload)

    def _emit_generic(self, url: str, payload: str) -> None:
        curl_command = _make_curl_command(self._session, "POST", url, payload)
        logger.debug(
            "Attempting to emit to DataHub GMS; using curl equivalent to:\n%s",
            curl_command,
        )
        try:
            response = self._session.post(url, data=payload)
            response.raise_for_status()
        except HTTPError as e:
            try:
                info = response.json()
                raise OperationalError(
                    "Unable to emit metadata to DataHub GMS", info
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
        return (
            f"DataHubRestEmitter: configured to talk to {self._gms_server}{token_str}"
        )


class DatahubRestEmitter(DataHubRestEmitter):
    """This class exists as a pass-through for backwards compatibility"""

    pass
