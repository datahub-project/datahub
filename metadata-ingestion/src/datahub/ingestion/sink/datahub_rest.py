import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, Type

import requests
from pydantic import BaseModel
from requests.exceptions import HTTPError

from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata import (  # MLFeatureSnapshotClass,
    ChartSnapshotClass,
    CorpGroupSnapshotClass,
    CorpUserSnapshotClass,
    DashboardSnapshotClass,
    DataProcessSnapshotClass,
    DatasetSnapshotClass,
    MLModelSnapshotClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

logger = logging.getLogger(__name__)

resource_locator: Dict[Type[object], str] = {
    ChartSnapshotClass: "charts",
    DashboardSnapshotClass: "dashboards",
    CorpUserSnapshotClass: "corpUsers",
    CorpGroupSnapshotClass: "corpGroups",
    DatasetSnapshotClass: "datasets",
    DataProcessSnapshotClass: "dataProcesses",
    MLModelSnapshotClass: "mlModels",
}


def _rest_li_ify(obj):
    if isinstance(obj, (dict, OrderedDict)):
        if len(obj.keys()) == 1:
            key = list(obj.keys())[0]
            value = obj[key]
            if key.find("com.linkedin.pegasus2avro.") >= 0:
                new_key = key.replace("com.linkedin.pegasus2avro.", "com.linkedin.")
                return {new_key: _rest_li_ify(value)}
            elif key == "string" or key == "array":
                return value

        new_obj = {}
        for key, value in obj.items():
            if value is not None:
                new_obj[key] = _rest_li_ify(value)
        return new_obj
    elif isinstance(obj, list):
        new_obj = [_rest_li_ify(item) for item in obj]
        return new_obj
    return obj


class DatahubRestSinkConfig(BaseModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"


@dataclass
class DatahubRestSink(Sink):
    config: DatahubRestSinkConfig
    report: SinkReport = field(default_factory=SinkReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = DatahubRestSinkConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_ingest_endpoint(self, mce: MetadataChangeEvent):
        snapshot_type = type(mce.proposedSnapshot)
        snapshot_resource = resource_locator.get(snapshot_type, None)
        if not snapshot_resource:
            raise ValueError(
                f"Failed to locate a snapshot resource for type {snapshot_type}"
            )

        return f"{self.config.server}/{snapshot_resource}?action=ingest"

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[MetadataChangeEvent],
        write_callback: WriteCallback,
    ):
        headers = {"X-RestLi-Protocol-Version": "2.0.0"}

        mce = record_envelope.record
        url = self.get_ingest_endpoint(mce)

        raw_mce_obj = mce.proposedSnapshot.to_obj()

        mce_obj = _rest_li_ify(raw_mce_obj)
        snapshot = {"snapshot": mce_obj}
        try:
            response = requests.post(url, headers=headers, json=snapshot)
            # with open('data.json', 'w') as outfile:
            #     json.dump(serialized_snapshot, outfile)
            response.raise_for_status()
            self.report.report_record_written(record_envelope)
            write_callback.on_success(record_envelope, {})
        except HTTPError as e:
            info = response.json()
            self.report.report_failure({"e": e, "info": info})
            write_callback.on_failure(record_envelope, e, info)
        except Exception as e:
            self.report.report_failure({"e": e})
            write_callback.on_failure(record_envelope, e, {})

    def get_report(self) -> SinkReport:
        return self.report

    def close(self):
        pass
