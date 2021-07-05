import itertools
import json
import logging
import shlex
from collections import OrderedDict
from json.decoder import JSONDecodeError
from typing import Any, List, Optional, Union

import requests
from requests.exceptions import HTTPError, RequestException

from datahub.configuration.common import OperationalError
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation

logger = logging.getLogger(__name__)


def _rest_li_ify(obj: Any) -> Any:
    if isinstance(obj, (dict, OrderedDict)):
        if len(obj.keys()) == 1:
            key = list(obj.keys())[0]
            value = obj[key]
            if key.find("com.linkedin.pegasus2avro.") >= 0:
                new_key = key.replace("com.linkedin.pegasus2avro.", "com.linkedin.")
                return {new_key: _rest_li_ify(value)}

        if "fieldDiscriminator" in obj:
            # Field discriminators are used for unions between primitive types.
            field = obj["fieldDiscriminator"]
            return {field: _rest_li_ify(obj[field])}

        new_obj: Any = {}
        for key, value in obj.items():
            if value is not None:
                new_obj[key] = _rest_li_ify(value)
        return new_obj
    elif isinstance(obj, list):
        new_obj = [_rest_li_ify(item) for item in obj]
        return new_obj
    return obj


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
    _gms_server: str
    _token: Optional[str]
    _session: requests.Session

    def __init__(self, gms_server: str, token: Optional[str] = None):
        if ":9002" in gms_server:
            logger.warn(
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

    def emit(self, item: Union[MetadataChangeEvent, UsageAggregation]) -> None:
        if isinstance(item, UsageAggregation):
            return self.emit_usage(item)
        return self.emit_mce(item)

    def emit_mce(self, mce: MetadataChangeEvent) -> None:
        url = f"{self._gms_server}/entities?action=ingest"

        raw_mce_obj = mce.proposedSnapshot.to_obj()
        mce_obj = _rest_li_ify(raw_mce_obj)
        snapshot_fqn = (
            f"com.linkedin.metadata.snapshot.{mce.proposedSnapshot.RECORD_SCHEMA.name}"
        )
        snapshot = {"entity": {"value": {snapshot_fqn: mce_obj}}}
        payload = json.dumps(snapshot)

        self._emit_generic(url, payload)

    def emit_usage(self, usageStats: UsageAggregation) -> None:
        url = f"{self._gms_server}/usageStats?action=batchIngest"

        raw_usage_obj = usageStats.to_obj()
        usage_obj = _rest_li_ify(raw_usage_obj)

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
