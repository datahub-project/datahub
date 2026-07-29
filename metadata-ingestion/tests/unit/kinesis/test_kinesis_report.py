from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.utilities.lossy_collections import LossyList


class TestKinesisSourceReport:
    def test_report_method_increments_streams_scanned(self):
        report = KinesisSourceReport()
        assert report.streams_scanned == 0
        report.report_stream_scanned()
        report.report_stream_scanned()
        assert report.streams_scanned == 2

    def test_report_method_appends_filtered_stream(self):
        report = KinesisSourceReport()
        report.report_stream_filtered("_internal_audit")
        assert "_internal_audit" in [str(x) for x in report.filtered_streams]

    def test_unsupported_destination_tracked(self):
        report = KinesisSourceReport()
        report.report_unsupported_destination(
            "DatadogDestinationDescription", "my-delivery-stream"
        )
        # The recorded entry should contain the destination type for diagnosis
        recorded = [str(x) for x in report.unsupported_destinations]
        assert any("Datadog" in r for r in recorded)

    def test_stream_failed_records_name_and_reason_in_composite_format(self):
        # The composite string lets users diagnose which stream failed and why
        # from the report summary alone, without consulting separate logs.
        report = KinesisSourceReport()
        report.report_stream_failed("events", "AccessDeniedException")
        recorded = [str(x) for x in report.streams_failed]
        assert len(recorded) == 1
        assert "events" in recorded[0]
        assert "AccessDeniedException" in recorded[0]


def test_firehose_glue_schema_lineage_fields_default_to_empty():
    """The report exposes:
      - firehose_glue_schema_lineage_emitted: counter of Glue lineage edges emitted
      - firehose_glue_schema_skipped: LossyList of "<stream>: <reason>" entries
        for delivery streams with SchemaConfiguration present but missing fields
    Both start at zero / empty so absent format-conversion configs leave the
    fields at their defaults.
    """
    report = KinesisSourceReport()
    assert report.firehose_glue_schema_lineage_emitted == 0
    assert isinstance(report.firehose_glue_schema_skipped, LossyList)
    assert list(report.firehose_glue_schema_skipped) == []


def test_firehose_glue_schema_helpers_mutate_fields():
    """Helpers preserve the report-class convention: every mutable field has a
    report_* helper that encapsulates the field-name + format-string. Direct
    attribute mutation at call sites would diverge from the established pattern
    used by all other report fields (streams_scanned, unsupported_destinations,
    schema_resolution_failures, etc.).
    """
    report = KinesisSourceReport()

    report.report_firehose_glue_schema_emitted()
    report.report_firehose_glue_schema_emitted()
    assert report.firehose_glue_schema_lineage_emitted == 2

    report.report_firehose_glue_schema_skipped("events-to-s3", "missing CatalogId")
    report.report_firehose_glue_schema_skipped("clicks-to-s3", "missing TableName")
    skipped = list(report.firehose_glue_schema_skipped)
    assert skipped == [
        "events-to-s3: missing CatalogId",
        "clicks-to-s3: missing TableName",
    ]
