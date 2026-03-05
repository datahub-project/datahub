import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.flink.client import (
    FlinkCheckpointConfig,
    FlinkJobDetail,
    FlinkPlanNode,
)
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.entities import FlinkEntityBuilder
from datahub.ingestion.source.flink.lineage import (
    ClassifiedNode,
    LineageResult,
    NodeRole,
)


def _config(**overrides: object) -> FlinkSourceConfig:
    defaults: dict = {"connection": {"rest_api_url": "http://localhost:8081"}}
    defaults.update(overrides)
    return FlinkSourceConfig.model_validate(defaults)


def _job_detail(**overrides: object) -> FlinkJobDetail:
    defaults: dict = {
        "jid": "abc123",
        "name": "fraud_detection",
        "state": "RUNNING",
        "start_time": 1707676800000,
        "end_time": -1,
        "duration": 3600000,
        "job_type": "STREAMING",
        "max_parallelism": 128,
        "plan_nodes": [
            FlinkPlanNode(
                id="1",
                description="Source: KafkaSource-transactions",
                operator="KafkaSource",
                parallelism=4,
            ),
            FlinkPlanNode(
                id="2",
                description="Sink: KafkaSink-alerts",
                operator="KafkaSink",
                parallelism=2,
            ),
        ],
    }
    defaults.update(overrides)
    return FlinkJobDetail(**defaults)


def _lineage() -> LineageResult:
    return LineageResult(
        sources=[
            ClassifiedNode(
                node_id="1",
                description="Source: KafkaSource-transactions",
                role=NodeRole.SOURCE,
                platform="kafka",
                dataset_name="transactions",
            )
        ],
        sinks=[
            ClassifiedNode(
                node_id="2",
                description="Sink: KafkaSink-alerts",
                role=NodeRole.SINK,
                platform="kafka",
                dataset_name="alerts",
            )
        ],
    )


class TestBuildDataflow:
    def test_includes_checkpoint_custom_properties(self) -> None:
        builder = FlinkEntityBuilder(_config())
        checkpoint = FlinkCheckpointConfig(
            mode="exactly_once",
            interval=60000,
            timeout=120000,
            state_backend="rocksdb",
            checkpoint_storage="filesystem",
        )
        dataflow = builder.build_dataflow(_job_detail(), checkpoint, "1.20.0")
        assert dataflow.custom_properties["state_backend"] == "rocksdb"
        assert dataflow.custom_properties["checkpoint_interval_ms"] == "60000"
        assert dataflow.custom_properties["checkpoint_mode"] == "exactly_once"

    def test_omits_checkpoint_when_absent(self) -> None:
        builder = FlinkEntityBuilder(_config())
        dataflow = builder.build_dataflow(_job_detail(), None, "1.20.0")
        assert "state_backend" not in dataflow.custom_properties
        assert "checkpoint_interval_ms" not in dataflow.custom_properties


class TestBuildDatajob:
    def test_populates_inlets_outlets_from_lineage(self) -> None:
        builder = FlinkEntityBuilder(_config())
        job = _job_detail()
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, _lineage())
        assert datajob.inlets is not None
        assert len(datajob.inlets) == 1
        assert "transactions" in str(datajob.inlets[0])
        assert datajob.outlets is not None
        assert len(datajob.outlets) == 1
        assert "alerts" in str(datajob.outlets[0])

    def test_platform_instance_map_resolves_urns(self) -> None:
        """platform_instance_map remaps lineage dataset platform instances."""
        config = _config(platform_instance_map={"kafka": "prod-kafka"})
        builder = FlinkEntityBuilder(config)
        job = _job_detail()
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, _lineage())
        assert datajob.inlets is not None
        assert "prod-kafka" in str(datajob.inlets[0])

    def test_vertex_granularity_routes_lineage_per_node(self) -> None:
        """In vertex mode, source node gets inlets and sink node gets outlets."""
        builder = FlinkEntityBuilder(_config())
        job = _job_detail()
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajobs = builder.build_datajobs_per_vertex(dataflow, job, _lineage())
        assert len(datajobs) == 2
        # Source node (id="1") should have inlets
        source_job = [j for j in datajobs if "1" in str(j.urn)][0]
        assert source_job.inlets is not None
        assert "transactions" in str(source_job.inlets[0])
        # Sink node (id="2") should have outlets
        sink_job = [j for j in datajobs if "2" in str(j.urn)][0]
        assert sink_job.outlets is not None
        assert "alerts" in str(sink_job.outlets[0])


class TestBuildDpiWorkunits:
    @staticmethod
    def _mcps(wus: list) -> list:
        return [
            wu for wu in wus if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]

    def test_running_job_emits_start_but_no_end(self) -> None:
        builder = FlinkEntityBuilder(_config())
        job = _job_detail(state="RUNNING")
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, LineageResult())
        wus = self._mcps(
            list(builder.build_dpi_workunits(job, datajob, LineageResult()))
        )
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataProcessInstanceProperties" in aspect_names
        assert "dataProcessInstanceRunEvent" in aspect_names
        run_events = [
            wu.metadata.aspect
            for wu in wus
            if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 1  # only start, no end

    def test_finished_job_emits_start_and_end(self) -> None:
        builder = FlinkEntityBuilder(_config())
        job = _job_detail(state="FINISHED", end_time=1707680400000)
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, LineageResult())
        wus = self._mcps(
            list(builder.build_dpi_workunits(job, datajob, LineageResult()))
        )
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataProcessInstanceProperties" in aspect_names
        assert "dataProcessInstanceRunEvent" in aspect_names
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 2  # start + end

    @pytest.mark.parametrize(
        "state,expected_result",
        [
            ("FAILED", "FAILURE"),
            ("CANCELED", "SKIPPED"),
        ],
    )
    def test_terminal_state_emits_correct_run_result(
        self, state: str, expected_result: str
    ) -> None:
        """FAILED→FAILURE, CANCELED→SKIPPED in the end run event."""
        builder = FlinkEntityBuilder(_config())
        job = _job_detail(state=state, end_time=1707680400000)
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, LineageResult())
        wus = self._mcps(
            list(builder.build_dpi_workunits(job, datajob, LineageResult()))
        )
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 2  # start + end
        end_event = run_events[-1].metadata.aspect
        assert expected_result in str(end_event)
