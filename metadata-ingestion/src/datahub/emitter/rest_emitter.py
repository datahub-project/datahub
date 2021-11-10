import itertools
import json
import logging
import shlex
from json.decoder import JSONDecodeError
from typing import Dict, List, Optional, Union

import requests
from requests.exceptions import HTTPError, RequestException

from datahub import __package_name__
from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation

logger = logging.getLogger(__name__)


def _make_curl_command(
    session: requests.Session, method: str, url: str, payload: str
) -> str:
    fragments: List[str] = [
        "curl",
        *itertools.chain(
            *[
                ("-X", method),
                *[("-H", f"{k}: {v}") for (k, v) in session.headers.items()],
                ("--data", payload),
            ]
        ),
        url,
    ]
    return " ".join(shlex.quote(fragment) for fragment in fragments)


class DatahubRestEmitter:
    DEFAULT_CONNECT_TIMEOUT_SEC = 30  # 30 seconds should be plenty to connect
    DEFAULT_READ_TIMEOUT_SEC = (
        30  # Any ingest call taking longer than 30 seconds should be abandoned
    )

    _gms_server: str
    _token: Optional[str]
    _session: requests.Session
    _connect_timeout_sec: float = DEFAULT_CONNECT_TIMEOUT_SEC
    _read_timeout_sec: float = DEFAULT_READ_TIMEOUT_SEC

    def __init__(
        self,
        gms_server: str,
        token: Optional[str] = None,
        connect_timeout_sec: Optional[float] = None,
        read_timeout_sec: Optional[float] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        if ":9002" in gms_server:
            logger.warning(
                "the rest emitter should connect to GMS (usually port 8080) instead of frontend"
            )
        self._gms_server = gms_server
        self._token = token

        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-RestLi-Protocol-Version": "2.0.0",
                "Content-Type": "application/json",
            }
        )
        if token:
            self._session.headers.update({"Authorization": f"Bearer {token}"})

        if extra_headers:
            self._session.headers.update(extra_headers)

        if connect_timeout_sec:
            self._connect_timeout_sec = connect_timeout_sec

        if read_timeout_sec:
            self._read_timeout_sec = read_timeout_sec

        if self._connect_timeout_sec < 1 or self._read_timeout_sec < 1:
            logger.warning(
                f"Setting timeout values lower than 1 second is not recommended. Your configuration is connect_timeout:{self._connect_timeout_sec}s, read_timeout:{self._read_timeout_sec}s"
            )

    def test_connection(self) -> None:
        response = self._session.get(f"{self._gms_server}/config")
        response.raise_for_status()
        config: dict = response.json()
        if config.get("noCode") != "true":
            raise ValueError(
                f"This version of {__package_name__} requires GMS v0.8.0 or higher"
            )

    def emit(
        self,
        item: Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ],
    ) -> None:
        if isinstance(item, UsageAggregation):
            return self.emit_usage(item)
        elif isinstance(item, (MetadataChangeProposal, MetadataChangeProposalWrapper)):
            return self.emit_mcp(item)
        else:
            return self.emit_mce(item)

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
            response = self._session.post(
                url,
                data=payload,
                timeout=(self._connect_timeout_sec, self._read_timeout_sec),
            )

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
