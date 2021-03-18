from collections import OrderedDict
from typing import Any, Dict, Type

import requests
from requests.exceptions import HTTPError, RequestException

from datahub.configuration.common import OperationalError
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (  # MLFeatureSnapshotClass,
    ChartSnapshotClass,
    CorpGroupSnapshotClass,
    CorpUserSnapshotClass,
    DashboardSnapshotClass,
    DataProcessSnapshotClass,
    DatasetSnapshotClass,
    MLModelSnapshotClass,
    TagSnapshotClass,
)

resource_locator: Dict[Type[object], str] = {
    ChartSnapshotClass: "charts",
    DashboardSnapshotClass: "dashboards",
    CorpUserSnapshotClass: "corpUsers",
    CorpGroupSnapshotClass: "corpGroups",
    DatasetSnapshotClass: "datasets",
    DataProcessSnapshotClass: "dataProcesses",
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

        new_obj: Any = {}
        for key, value in obj.items():
            if value is not None:
                new_obj[key] = _rest_li_ify(value)
        return new_obj
    elif isinstance(obj, list):
        new_obj = [_rest_li_ify(item) for item in obj]
        return new_obj
    return obj


class DatahubRestEmitter:
    _gms_server: str

    def __init__(self, gms_server: str):
        self._gms_server = gms_server

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
        headers = {"X-RestLi-Protocol-Version": "2.0.0"}

        raw_mce_obj = mce.proposedSnapshot.to_obj()
        mce_obj = _rest_li_ify(raw_mce_obj)
        snapshot = {"snapshot": mce_obj}

        try:
            response = requests.post(url, headers=headers, json=snapshot)

            # import curlify
            # print(curlify.to_curl(response.request))
            # breakpoint()

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
