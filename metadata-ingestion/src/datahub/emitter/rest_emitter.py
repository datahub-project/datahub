import itertools
import json
import logging
import shlex
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Type

import requests
from requests.exceptions import HTTPError, RequestException

from datahub.configuration.common import OperationalError
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (  # MLFeatureSnapshotClass,
    ChartSnapshotClass,
    CorpGroupSnapshotClass,
    CorpUserSnapshotClass,
    DashboardSnapshotClass,
    DataFlowSnapshotClass,
    DataJobSnapshotClass,
    DataProcessSnapshotClass,
    DatasetSnapshotClass,
    MLModelSnapshotClass,
    TagSnapshotClass,
)

logger = logging.getLogger(__name__)

resource_locator: Dict[Type[object], str] = {
    ChartSnapshotClass: "charts",
    DashboardSnapshotClass: "dashboards",
    CorpUserSnapshotClass: "corpUsers",
    CorpGroupSnapshotClass: "corpGroups",
    DatasetSnapshotClass: "datasets",
    DataProcessSnapshotClass: "dataProcesses",
    DataFlowSnapshotClass: "dataFlows",
    DataJobSnapshotClass: "dataJobs",
    MLModelSnapshotClass: "mlModels",
    TagSnapshotClass: "tags",
}


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

    def _get_ingest_endpoint(self, mce: MetadataChangeEvent) -> str:
        snapshot_type = type(mce.proposedSnapshot)
        snapshot_resource = resource_locator.get(snapshot_type, None)
        if not snapshot_resource:
            raise ValueError(
                f"Failed to locate a snapshot resource for type {snapshot_type}"
            )

        return f"{self._gms_server}/{snapshot_resource}?action=ingest"

    def emit_mce(self, mce: MetadataChangeEvent) -> None:
        url = self._get_ingest_endpoint(mce)

        raw_mce_obj = mce.proposedSnapshot.to_obj()
        mce_obj = _rest_li_ify(raw_mce_obj)
        snapshot = {"snapshot": mce_obj}
        payload = json.dumps(snapshot)

        curl_command = _make_curl_command(self._session, "POST", url, payload)
        logger.debug(
            "Attempting to emit to DataHub GMS; using curl equivalent to:\n%s",
            curl_command,
        )
        try:
            response = self._session.post(url, data=payload)

            response.raise_for_status()
        except HTTPError as e:
            info = response.json()
            raise OperationalError(
                "Unable to emit metadata to DataHub GMS", info
            ) from e
        except RequestException as e:
            raise OperationalError(
                "Unable to emit metadata to DataHub GMS", {"message": str(e)}
            ) from e
