from datahub.ingestion.source.flink.flink_client import (
    FlinkCheckpointConfig,
    FlinkJobDetail,
    FlinkPlanNode,
)
from datahub.ingestion.source.flink.flink_config import FlinkSourceConfig
from datahub.ingestion.source.flink.flink_entities import (
    FlinkEntityBuilder,
    _compute_dataset_urns,
)
from datahub.ingestion.source.flink.flink_lineage import (
    ClassifiedNode,
    LineageResult,
    NodeRole,
)


def _make_config(**overrides) -> FlinkSourceConfig:
    defaults = {"rest_endpoint": "http://localhost:8081"}
    defaults.update(overrides)
    return FlinkSourceConfig.model_validate(defaults)


def _make_job_detail(**overrides) -> FlinkJobDetail:
    defaults = {
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
        ],
    }
    defaults.update(overrides)
    return FlinkJobDetail(**defaults)


def _make_lineage_result() -> LineageResult:
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


class TestComputeDatasetUrns:
    def test_with_platform_instance(self):
        lineage = _make_lineage_result()
        inlets, outlets = _compute_dataset_urns(lineage, "kafka-prod", "PROD")
        assert len(inlets) == 1
        assert "kafka" in inlets[0]
        assert "transactions" in inlets[0]
        assert "kafka-prod" in inlets[0]
        assert len(outlets) == 1
        assert "alerts" in outlets[0]

    def test_without_platform_instance(self):
        lineage = _make_lineage_result()
        inlets, outlets = _compute_dataset_urns(lineage, None, "PROD")
        assert len(inlets) == 1
        assert "kafka" in inlets[0]
        assert "transactions" in inlets[0]

    def test_empty_lineage(self):
        lineage = LineageResult()
        inlets, outlets = _compute_dataset_urns(lineage, None, "PROD")
        assert inlets == []
        assert outlets == []


class TestBuildDataflow:
    def test_basic_properties(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail()
        checkpoint = FlinkCheckpointConfig(
            mode="exactly_once",
            interval=60000,
            timeout=120000,
            externalized_checkpoint_config=None,
            state_backend="rocksdb",
            checkpoint_storage="filesystem",
        )

        dataflow = builder.build_dataflow(job_detail, checkpoint, "1.20.0")

        assert dataflow.name == "fraud_detection"
        assert dataflow.display_name == "fraud_detection"
        assert "1.20.0" in str(dataflow.custom_properties.get("flink_version", ""))
        assert dataflow.custom_properties["flink_job_id"] == "abc123"
        assert dataflow.custom_properties["state_backend"] == "rocksdb"
        assert dataflow.external_url == "http://localhost:8081/#/jobs/abc123"

    def test_without_checkpoint(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail()

        dataflow = builder.build_dataflow(job_detail, None, "1.20.0")

        assert "state_backend" not in dataflow.custom_properties
        assert "checkpoint_interval_ms" not in dataflow.custom_properties

    def test_with_platform_instance(self):
        config = _make_config(platform_instance="my-flink-cluster")
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail()

        dataflow = builder.build_dataflow(job_detail, None, "1.20.0")

        assert "my-flink-cluster" in str(dataflow.urn)


class TestBuildDatajob:
    def test_with_lineage(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail()
        dataflow = builder.build_dataflow(job_detail, None, "1.20.0")
        lineage = _make_lineage_result()

        datajob = builder.build_datajob(dataflow, job_detail, lineage)

        assert datajob.name == "fraud_detection"
        assert len(datajob.inlets) == 1
        assert len(datajob.outlets) == 1
        assert "transactions" in str(datajob.inlets[0])
        assert "alerts" in str(datajob.outlets[0])

    def test_without_lineage(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail()
        dataflow = builder.build_dataflow(job_detail, None, "1.20.0")
        empty_lineage = LineageResult()

        datajob = builder.build_datajob(dataflow, job_detail, empty_lineage)

        assert datajob.name == "fraud_detection"
        assert len(datajob.inlets) == 0
        assert len(datajob.outlets) == 0


class TestBuildDpiWorkunits:
    def _make_datajob(self, config=None, job_detail=None, lineage=None):
        config = config or _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = job_detail or _make_job_detail()
        dataflow = builder.build_dataflow(job_detail, None, "1.20.0")
        lineage = lineage or LineageResult()
        return builder.build_datajob(dataflow, job_detail, lineage)

    def test_running_job_emits_start_no_end(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail(state="RUNNING")
        lineage = _make_lineage_result()
        datajob = self._make_datajob(config, job_detail, lineage)

        workunits = list(
            builder.build_dpi_workunits(
                "fraud_detection",
                job_detail,
                datajob,
                lineage,
            )
        )

        # Should emit: generate_mcp + start_event_mcp (no end event for RUNNING)
        assert len(workunits) >= 2

    def test_finished_job_emits_end_event(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail(
            state="FINISHED",
            end_time=1707680400000,
        )
        lineage = _make_lineage_result()
        datajob = self._make_datajob(config, job_detail, lineage)

        workunits = list(
            builder.build_dpi_workunits(
                "fraud_detection",
                job_detail,
                datajob,
                lineage,
            )
        )

        # Should emit: generate_mcp + start_event_mcp + end_event_mcp
        assert len(workunits) >= 3

    def test_failed_job_emits_failure(self):
        config = _make_config()
        builder = FlinkEntityBuilder(config)
        job_detail = _make_job_detail(
            state="FAILED",
            end_time=1707680400000,
        )
        datajob = self._make_datajob(config, job_detail)

        workunits = list(
            builder.build_dpi_workunits(
                "fraud_detection",
                job_detail,
                datajob,
                LineageResult(),
            )
        )

        assert len(workunits) >= 3
