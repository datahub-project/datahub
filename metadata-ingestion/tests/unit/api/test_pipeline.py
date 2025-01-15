import pathlib
from typing import Iterable, List, cast
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from typing_extensions import Self

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.api.committable import CommitPolicy, Committable
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.run.pipeline import Pipeline, PipelineContext
from datahub.ingestion.sink.datahub_rest import DatahubRestSink
from datahub.metadata.com.linkedin.pegasus2avro.mxe import SystemMetadata
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    StatusClass,
)
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.sink_helpers import RecordingSinkReport

FROZEN_TIME = "2020-04-14 07:00:00"

# TODO: It seems like one of these tests writes to ~/.datahubenv or otherwise sets
# some global config, which impacts other tests.
pytestmark = pytest.mark.random_order(disabled=True)


class TestPipeline:
    @patch("confluent_kafka.Consumer", autospec=True)
    @patch(
        "datahub.ingestion.source.kafka.kafka.KafkaSource.get_workunits", autospec=True
    )
    @patch("datahub.ingestion.sink.console.ConsoleSink.close", autospec=True)
    @freeze_time(FROZEN_TIME)
    def test_configure(self, mock_sink, mock_source, mock_consumer):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "fake-dns-name:9092"}},
                },
                "sink": {"type": "console"},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        pipeline.pretty_print_summary()
        mock_source.assert_called_once()
        mock_sink.assert_called_once()

    @freeze_time(FROZEN_TIME)
    @patch(
        "datahub.emitter.rest_emitter.DatahubRestEmitter.test_connection",
        return_value={"noCode": True},
    )
    @patch(
        "datahub.ingestion.graph.client.DataHubGraph.get_config",
        return_value={"noCode": True},
    )
    @patch(
        "datahub.cli.config_utils.load_client_config",
        return_value=DatahubClientConfig(server="http://fake-gms-server:8080"),
    )
    def test_configure_without_sink(
        self, mock_emitter, mock_graph, mock_load_client_config
    ):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_file.json"},
                },
            }
        )
        # assert that the default sink is a DatahubRestSink
        assert isinstance(pipeline.sink, DatahubRestSink)
        assert pipeline.sink.config.server == "http://fake-gms-server:8080"
        assert pipeline.sink.config.token is None

    @freeze_time(FROZEN_TIME)
    @patch(
        "datahub.emitter.rest_emitter.DatahubRestEmitter.test_connection",
        return_value={"noCode": True},
    )
    @patch(
        "datahub.ingestion.graph.client.DataHubGraph.get_config",
        return_value={"noCode": True},
    )
    @patch(
        "datahub.cli.config_utils.load_client_config",
        return_value=DatahubClientConfig(server="http://fake-internal-server:8080"),
    )
    @patch(
        "datahub.cli.config_utils.get_system_auth",
        return_value="Basic user:pass",
    )
    def test_configure_without_sink_use_system_auth(
        self, mock_emitter, mock_graph, mock_load_client_config, mock_get_system_auth
    ):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_file.json"},
                },
            }
        )
        # assert that the default sink is a DatahubRestSink
        assert isinstance(pipeline.sink, DatahubRestSink)
        assert pipeline.sink.config.server == "http://fake-internal-server:8080"
        assert pipeline.sink.config.token is None
        assert (
            pipeline.sink.emitter._session.headers["Authorization"] == "Basic user:pass"
        )

    @freeze_time(FROZEN_TIME)
    @patch(
        "datahub.emitter.rest_emitter.DatahubRestEmitter.test_connection",
        return_value={"noCode": True},
    )
    @patch(
        "datahub.ingestion.graph.client.DataHubGraph.get_config",
        return_value={"noCode": True},
    )
    def test_configure_with_rest_sink_initializes_graph(
        self, mock_source, mock_test_connection
    ):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_events.json"},
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": "http://somehost.someplace.some:8080",
                        "token": "foo",
                    },
                },
            },
            # We don't want to make actual API calls during this test.
            report_to=None,
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "datahub-rest"
        assert pipeline.config.sink.config == {
            "server": "http://somehost.someplace.some:8080",
            "token": "foo",
        }
        assert pipeline.ctx.graph is not None, "DataHubGraph should be initialized"
        assert pipeline.ctx.graph.config.server == pipeline.config.sink.config["server"]
        assert pipeline.ctx.graph.config.token == pipeline.config.sink.config["token"]

    @freeze_time(FROZEN_TIME)
    @patch(
        "datahub.emitter.rest_emitter.DatahubRestEmitter.test_connection",
        return_value={"noCode": True},
    )
    @patch(
        "datahub.ingestion.graph.client.DataHubGraph.get_config",
        return_value={"noCode": True},
    )
    def test_configure_with_rest_sink_with_additional_props_initializes_graph(
        self, mock_source, mock_test_connection
    ):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "file",
                    "config": {"path": "test_events.json"},
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": "http://somehost.someplace.some:8080",
                        "token": "foo",
                        "mode": "sync",
                    },
                },
            }
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "datahub-rest"
        assert pipeline.config.sink.config == {
            "server": "http://somehost.someplace.some:8080",
            "token": "foo",
            "mode": "sync",
        }
        assert pipeline.ctx.graph is not None, "DataHubGraph should be initialized"
        assert pipeline.ctx.graph.config.server == pipeline.config.sink.config["server"]
        assert pipeline.ctx.graph.config.token == pipeline.config.sink.config["token"]

    @freeze_time(FROZEN_TIME)
    @patch(
        "datahub.ingestion.source.kafka.kafka.KafkaSource.get_workunits", autospec=True
    )
    def test_configure_with_file_sink_does_not_init_graph(self, mock_source, tmp_path):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "localhost:9092"}},
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(tmp_path / "test.json"),
                    },
                },
            }
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "file"
        assert pipeline.config.sink.config == {"filename": str(tmp_path / "test.json")}
        assert pipeline.ctx.graph is None, "DataHubGraph should not be initialized"

    @freeze_time(FROZEN_TIME)
    def test_run_including_fake_transformation(self):
        pipeline = Pipeline.create(
            {
                "source": {"type": "tests.unit.api.test_pipeline.FakeSource"},
                "transformers": [
                    {"type": "tests.unit.api.test_pipeline.AddStatusRemovedTransformer"}
                ],
                "sink": {"type": "tests.test_helpers.sink_helpers.RecordingSink"},
                "run_id": "pipeline_test",
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        expected_mce = get_initial_mce()

        dataset_snapshot = cast(DatasetSnapshotClass, expected_mce.proposedSnapshot)
        dataset_snapshot.aspects.append(get_status_removed_aspect())

        sink_report: RecordingSinkReport = cast(
            RecordingSinkReport, pipeline.sink.get_report()
        )

        assert len(sink_report.received_records) == 1
        assert expected_mce == sink_report.received_records[0].record

    @freeze_time(FROZEN_TIME)
    def test_run_including_registered_transformation(self):
        # This is not testing functionality, but just the transformer registration system.

        pipeline = Pipeline.create(
            {
                "source": {"type": "tests.unit.api.test_pipeline.FakeSource"},
                "transformers": [
                    {
                        "type": "simple_add_dataset_ownership",
                        "config": {
                            "owner_urns": ["urn:li:corpuser:foo"],
                            "ownership_type": "urn:li:ownershipType:__system__technical_owner",
                        },
                    }
                ],
                "sink": {"type": "tests.test_helpers.sink_helpers.RecordingSink"},
            }
        )
        assert pipeline

    @pytest.mark.parametrize(
        "source,strict_warnings,exit_code",
        [
            pytest.param(
                "FakeSource",
                False,
                0,
            ),
            pytest.param(
                "FakeSourceWithWarnings",
                False,
                0,
            ),
            pytest.param(
                "FakeSourceWithWarnings",
                True,
                1,
            ),
        ],
    )
    @freeze_time(FROZEN_TIME)
    def test_pipeline_return_code(self, tmp_path, source, strict_warnings, exit_code):
        config_file: pathlib.Path = tmp_path / "test.yml"

        config_file.write_text(
            f"""
---
run_id: pipeline_test
source:
    type: tests.unit.api.test_pipeline.{source}
    config: {{}}
sink:
    type: console
"""
        )

        res = run_datahub_cmd(
            [
                "ingest",
                "-c",
                f"{config_file}",
                *(("--strict-warnings",) if strict_warnings else ()),
            ],
            tmp_path=tmp_path,
            check_result=False,
        )
        assert res.exit_code == exit_code, res.stdout

    @pytest.mark.parametrize(
        "commit_policy,source,should_commit",
        [
            pytest.param(
                CommitPolicy.ALWAYS,
                "FakeSource",
                True,
                id="ALWAYS-no-warnings-no-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS,
                "FakeSource",
                True,
                id="ON_NO_ERRORS-no-warnings-no-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS,
                "FakeSource",
                True,
                id="ON_NO_ERRORS_AND_NO_WARNINGS-no-warnings-no-errors",
            ),
            pytest.param(
                CommitPolicy.ALWAYS,
                "FakeSourceWithWarnings",
                True,
                id="ALWAYS-with-warnings",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS,
                "FakeSourceWithWarnings",
                True,
                id="ON_NO_ERRORS-with-warnings",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS,
                "FakeSourceWithWarnings",
                False,
                id="ON_NO_ERRORS_AND_NO_WARNINGS-with-warnings",
            ),
            pytest.param(
                CommitPolicy.ALWAYS,
                "FakeSourceWithFailures",
                True,
                id="ALWAYS-with-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS,
                "FakeSourceWithFailures",
                False,
                id="ON_NO_ERRORS-with-errors",
            ),
            pytest.param(
                CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS,
                "FakeSourceWithFailures",
                False,
                id="ON_NO_ERRORS_AND_NO_WARNINGS-with-errors",
            ),
        ],
    )
    @freeze_time(FROZEN_TIME)
    def test_pipeline_process_commits(self, commit_policy, source, should_commit):
        pipeline = Pipeline.create(
            {
                "source": {"type": f"tests.unit.api.test_pipeline.{source}"},
                "sink": {"type": "console"},
                "run_id": "pipeline_test",
            }
        )

        class FakeCommittable(Committable):
            def __init__(self, commit_policy: CommitPolicy):
                self.name = "test_checkpointer"
                self.commit_policy = commit_policy

            def commit(self) -> None:
                pass

        fake_committable: Committable = FakeCommittable(commit_policy)

        with patch.object(
            FakeCommittable, "commit", wraps=fake_committable.commit
        ) as mock_commit:
            pipeline.ctx.register_checkpointer(fake_committable)

            pipeline.run()
            # check that we called the commit method once only if should_commit is True
            if should_commit:
                mock_commit.assert_called_once()
            else:
                mock_commit.assert_not_called()


class AddStatusRemovedTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for record_envelope in record_envelopes:
            if isinstance(record_envelope.record, MetadataChangeEventClass):
                assert isinstance(
                    record_envelope.record.proposedSnapshot, DatasetSnapshotClass
                )
                record_envelope.record.proposedSnapshot.aspects.append(
                    get_status_removed_aspect()
                )
            yield record_envelope


class FakeSource(Source):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report = SourceReport()
        self.work_units: List[MetadataWorkUnit] = [
            MetadataWorkUnit(id="workunit-1", mce=get_initial_mce())
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        assert not config_dict
        return cls(ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


class FakeSourceWithWarnings(FakeSource):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report.report_warning("test_warning", "warning_text")

    def get_report(self) -> SourceReport:
        return self.source_report


class FakeSourceWithFailures(FakeSource):
    def __init__(self, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_report.report_failure("test_failure", "failure_text")

    def get_report(self) -> SourceReport:
        return self.source_report


def get_initial_mce() -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:test_platform,test,PROD)",
            aspects=[
                DatasetPropertiesClass(
                    description="test.description",
                )
            ],
        ),
        systemMetadata=SystemMetadata(
            lastObserved=1586847600000, runId="pipeline_test"
        ),
    )


def get_status_removed_aspect() -> StatusClass:
    return StatusClass(removed=False)
