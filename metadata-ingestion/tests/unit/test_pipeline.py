import unittest
from typing import Iterable, List, cast
from unittest.mock import patch

from datahub.ingestion.api.common import RecordEnvelope, WorkUnit
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline import Pipeline, PipelineContext
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)
from tests.test_helpers.sink_helpers import RecordingSinkReport


class PipelineTest(unittest.TestCase):
    @patch("datahub.ingestion.source.kafka.KafkaSource.get_workunits", autospec=True)
    @patch("datahub.ingestion.sink.console.ConsoleSink.close", autospec=True)
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

    def test_run_including_fake_transformation(self):

        pipeline = Pipeline.create(
            {
                "source": {"type": "tests.unit.test_pipeline.FakeSource"},
                "transformers": [
                    {"type": "tests.unit.test_pipeline.AddStatusRemovedTransformer"}
                ],
                "sink": {"type": "tests.test_helpers.sink_helpers.RecordingSink"},
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

        self.assertEqual(len(sink_report.received_records), 1)
        self.assertEqual(expected_mce, sink_report.received_records[0].record)

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


class AddStatusRemovedTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return cls()

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for record_envelope in record_envelopes:
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
        return FakeSource()

    def get_workunits(self) -> Iterable[WorkUnit]:
        return self.work_units

    def get_report(self) -> SourceReport:
        return self.source_report

    def close(self):
        pass


def get_initial_mce() -> MetadataChangeEventClass:
    return MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn="urn:li:dataset:(urn:li:dataPlatform:test_platform,test,PROD)",
            aspects=[
                DatasetPropertiesClass(
                    description="test.description",
                )
            ],
        )
    )


def get_status_removed_aspect() -> Status:
    return Status(removed=False)
