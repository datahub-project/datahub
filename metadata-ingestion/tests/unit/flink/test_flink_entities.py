import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.flink.client import (
    FlinkCheckpointConfig,
    FlinkJobDetail,
    FlinkPlanNode,
)
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.entities import (
    FlinkEntityBuilder,
    compute_dataset_urns,
    materialize_dataset_workunits,
)
from datahub.ingestion.source.flink.lineage import (
    CatalogTableReference,
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


class TestMaterializeDatasetWorkunits:
    def test_emits_workunits_per_urn(self) -> None:
        urns: list = [
            "urn:li:dataset:(urn:li:dataPlatform:kafka,transactions,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:kafka,alerts,PROD)",
        ]
        wus = materialize_dataset_workunits(urns)
        assert len(wus) > 0
        entity_urns = {
            wu.metadata.entityUrn
            for wu in wus
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        }
        assert any("transactions" in str(u) for u in entity_urns)
        assert any("alerts" in str(u) for u in entity_urns)

    def test_empty_urns_returns_empty(self) -> None:
        assert materialize_dataset_workunits([]) == []


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
        config = _config(platform_instance_map={"kafka": "prod-kafka"})
        builder = FlinkEntityBuilder(config)
        job = _job_detail()
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, _lineage())
        assert datajob.inlets is not None
        assert "prod-kafka" in str(datajob.inlets[0])

    def test_catalog_platform_map_takes_priority_over_platform_instance_map(
        self,
    ) -> None:
        """catalog_platform_map.platform_instance is primary;
        platform_instance_map is fallback."""
        config = _config(
            platform_instance_map={"kafka": "fallback-kafka"},
            catalog_platform_map={
                "my_kafka_catalog": {"platform_instance": "primary-kafka"},
            },
        )
        lineage = LineageResult(
            sources=[
                ClassifiedNode(
                    node_id="1",
                    description="TableSourceScan",
                    role=NodeRole.SOURCE,
                    platform="kafka",
                    dataset_name="orders",
                    catalog_ref=CatalogTableReference(
                        catalog="my_kafka_catalog",
                        database="default_database",
                        table="orders",
                    ),
                )
            ],
        )
        builder = FlinkEntityBuilder(config)
        job = _job_detail()
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, lineage)
        assert datajob.inlets is not None
        # catalog_platform_map takes priority
        assert "primary-kafka" in str(datajob.inlets[0])
        assert "fallback-kafka" not in str(datajob.inlets[0])

    def test_vertex_granularity_routes_lineage_per_node(self) -> None:
        builder = FlinkEntityBuilder(_config())
        job = _job_detail()
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajobs = builder.build_datajobs_per_vertex(dataflow, job, _lineage())
        assert len(datajobs) == 2
        source_job = [j for j in datajobs if "1" in str(j.urn)][0]
        assert source_job.inlets is not None
        assert "transactions" in str(source_job.inlets[0])
        sink_job = [j for j in datajobs if "2" in str(j.urn)][0]
        assert sink_job.outlets is not None
        assert "alerts" in str(sink_job.outlets[0])


class TestBuildDpiWorkunits:
    @staticmethod
    def _build_dpi_mcps(**job_overrides: object) -> list:
        """Build DPI workunits for a job and return only MCP-wrapped workunits."""
        builder = FlinkEntityBuilder(_config())
        job = _job_detail(**job_overrides)
        dataflow = builder.build_dataflow(job, None, "1.20.0")
        datajob = builder.build_datajob(dataflow, job, LineageResult())
        return [
            wu
            for wu in builder.build_dpi_workunits(job, datajob, inlets=[], outlets=[])
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]

    def test_running_job_emits_start_but_no_end(self) -> None:
        wus = self._build_dpi_mcps(state="RUNNING")
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataProcessInstanceProperties" in aspect_names
        assert "dataProcessInstanceRunEvent" in aspect_names
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 1

    def test_finished_job_emits_start_and_end(self) -> None:
        wus = self._build_dpi_mcps(state="FINISHED", end_time=1707680400000)
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataProcessInstanceProperties" in aspect_names
        assert "dataProcessInstanceRunEvent" in aspect_names
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 2

    @pytest.mark.parametrize(
        "job_type,expected_type",
        [
            ("BATCH", "BATCH_SCHEDULED"),
            ("STREAMING", "STREAMING"),
            (None, None),
        ],
    )
    def test_dpi_process_type_matches_job_type(
        self, job_type: str, expected_type: str
    ) -> None:
        """BATCH -> BATCH_SCHEDULED, STREAMING -> STREAMING, None -> None (unknown on Flink < 1.20)."""
        wus = self._build_dpi_mcps(job_type=job_type, state="RUNNING")
        props = [
            wu
            for wu in wus
            if wu.metadata.aspectName == "dataProcessInstanceProperties"
        ]
        assert len(props) == 1
        assert props[0].metadata.aspect.type == expected_type

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
        """FAILED -> FAILURE, CANCELED -> SKIPPED in the end run event."""
        wus = self._build_dpi_mcps(state=state, end_time=1707680400000)
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 2
        end_event = run_events[-1].metadata.aspect
        assert expected_result in str(end_event)

    def test_terminal_job_missing_end_time_skips_end_event(self) -> None:
        """Guard against non-deterministic end events: if a terminal job
        lacks a valid end_time (rare API inconsistency), skip the end event
        rather than fabricating a timestamp with time.time()."""
        wus = self._build_dpi_mcps(state="FINISHED", end_time=-1)
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 1
        assert run_events[0].metadata.aspect.status == "STARTED"

    def test_zero_start_time_skips_start_event(self) -> None:
        """start_time=0 means the Flink API didn't provide a valid timestamp.
        Skip the start event rather than emitting epoch-zero."""
        wus = self._build_dpi_mcps(state="RUNNING", start_time=0)
        run_events = [
            wu for wu in wus if wu.metadata.aspectName == "dataProcessInstanceRunEvent"
        ]
        assert len(run_events) == 0


class TestComputeDatasetUrns:
    def test_skips_none_platform(self) -> None:
        nodes = [
            ClassifiedNode(
                node_id="1",
                description="unresolved source",
                role=NodeRole.SOURCE,
                platform=None,
                dataset_name="some.table",
            )
        ]
        urns = compute_dataset_urns(nodes, _config())
        assert urns == []

    def test_skips_none_dataset_name(self) -> None:
        nodes = [
            ClassifiedNode(
                node_id="1",
                description="unresolved source",
                role=NodeRole.SOURCE,
                platform="kafka",
                dataset_name=None,
            )
        ]
        urns = compute_dataset_urns(nodes, _config())
        assert urns == []
