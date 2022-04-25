from typing import Iterable, List, cast
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.api.committable import CommitPolicy, Committable
from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline import Pipeline, PipelineContext
from datahub.metadata.com.linkedin.pegasus2avro.mxe import SystemMetadata
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    StatusClass,
)
from tests.test_helpers.sink_helpers import RecordingSinkReport

FROZEN_TIME = "2020-04-14 07:00:00"


class TestPipeline(object):
    @patch("datahub.ingestion.source.kafka.KafkaSource.get_workunits", autospec=True)
    @patch("datahub.ingestion.sink.console.ConsoleSink.close", autospec=True)
    @freeze_time(FROZEN_TIME)
    def test_configure(self, mock_sink, mock_source):
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "localhost:9092"}},
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
    @patch("datahub.emitter.rest_emitter.DatahubRestEmitter.test_connection")
    @patch("datahub.ingestion.source.kafka.KafkaSource.get_workunits", autospec=True)
    def test_configure_without_sink(self, mock_source, mock_test_connection):

        mock_test_connection.return_value = {"noCode": True}
        pipeline = Pipeline.create(
            {
                "source": {
                    "type": "kafka",
                    "config": {"connection": {"bootstrap": "localhost:9092"}},
                },
            }
        )
        # assert that the default sink config is for a DatahubRestSink
        assert isinstance(pipeline.config.sink, DynamicTypedConfig)
        assert pipeline.config.sink.type == "datahub-rest"
        assert pipeline.config.sink.config == {
            "server": "http://localhost:8080",
            "token": "",
        }

    @freeze_time(FROZEN_TIME)
    def test_run_including_fake_transformation(self):
        pipeline = Pipeline.create(
            {
                "source": {"type": "tests.unit.test_pipeline.FakeSource"},
                "transformers": [
                    {"type": "tests.unit.test_pipeline.AddStatusRemovedTransformer"}
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
                "source": {"type": "tests.unit.test_pipeline.FakeSource"},
                "transformers": [
                    {
                        "type": "simple_add_dataset_ownership",
                        "config": {"owner_urns": ["urn:li:corpuser:foo"]},
                    }
                ],
                "sink": {"type": "tests.test_helpers.sink_helpers.RecordingSink"},
            }
        )
        assert pipeline

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
                "source": {"type": f"tests.unit.test_pipeline.{source}"},
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
            pipeline.ctx.register_reporter(fake_committable)

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
    def __init__(self):
        self.source_report = SourceReport()
        self.work_units: List[MetadataWorkUnit] = [
            MetadataWorkUnit(id="workunit-1", mce=get_initial_mce())
        ]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        assert not config_dict
        return cls()

    def get_workunits(self) -> Iterable[WorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


class FakeSourceWithWarnings(FakeSource):
    def __init__(self):
        super().__init__()
        self.source_report.report_warning("test_warning", "warning_text")

    def get_report(self) -> SourceReport:
        return self.source_report


class FakeSourceWithFailures(FakeSource):
    def __init__(self):
        super().__init__()
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
