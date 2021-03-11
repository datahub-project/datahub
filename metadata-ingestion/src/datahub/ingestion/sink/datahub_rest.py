import logging
from dataclasses import dataclass

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
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
    TagSnapshotClass: "tags",
}


def _rest_li_ify(obj):
    if isinstance(obj, (dict, OrderedDict)):
        if len(obj.keys()) == 1:
            key = list(obj.keys())[0]
            value = obj[key]
            breakpoint()
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

class DatahubRestSinkConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"


@dataclass
class DatahubRestSink(Sink):
    config: DatahubRestSinkConfig
    emitter: DatahubRestEmitter
    report: SinkReport

    def __init__(self, ctx: PipelineContext, config: DatahubRestSinkConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SinkReport()
        self.emitter = DatahubRestEmitter(self.config.server)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = DatahubRestSinkConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[MetadataChangeEvent],
        write_callback: WriteCallback,
    ):
        mce = record_envelope.record

        mce_obj = _rest_li_ify(raw_mce_obj)
        snapshot = {"snapshot": mce_obj}
        breakpoint()
        try:
            self.emitter.emit_mce(mce)
            self.report.report_record_written(record_envelope)
            write_callback.on_success(record_envelope, {})
        except OperationalError as e:
            self.report.report_failure({"error": e.message, "info": e.info})
            write_callback.on_failure(record_envelope, e, e.info)
        except Exception as e:
            self.report.report_failure({"e": e})
            write_callback.on_failure(record_envelope, e, {})

    def get_report(self) -> SinkReport:
        return self.report

    def close(self):
        pass
