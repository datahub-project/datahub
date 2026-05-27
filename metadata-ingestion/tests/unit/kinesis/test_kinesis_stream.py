from unittest.mock import MagicMock

from botocore.exceptions import ClientError, EndpointConnectionError

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_stream import KinesisStreamExtractor
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


def _make_extractor(
    stream_pattern=None, schema_registry=None
) -> KinesisStreamExtractor:
    config = KinesisSourceConfig.model_validate(
        {
            "aws_config": {"aws_region": "us-east-1"},
            "stream_pattern": (stream_pattern or {"allow": [".*"]}),
        }
    )
    report = KinesisSourceReport()
    session = MagicMock()
    return KinesisStreamExtractor(
        config=config,
        report=report,
        session=session,
        region_key=MagicMock(),
        schema_registry=schema_registry,
    )


class TestKinesisStreamExtractor:
    def test_internal_streams_filtered_by_default_deny_pattern(self):
        # The deny pattern in recipe.yml is "^_.*"; the extractor must honor whatever
        # stream_pattern is given in config and report filtered streams.
        ex = _make_extractor(stream_pattern={"deny": ["^_.*"]})
        ex._kinesis.get_paginator.return_value.paginate.return_value = [
            {"StreamNames": ["events", "clicks", "_internal_audit"]}
        ]

        kept = list(ex.list_stream_names())

        assert "events" in kept
        assert "clicks" in kept
        assert "_internal_audit" not in kept
        assert "_internal_audit" in [str(x) for x in ex.report.filtered_streams]

    def test_describe_stream_403_continues_with_warning(self):
        ex = _make_extractor()
        ex._kinesis.describe_stream.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "nope"}},
            "DescribeStream",
        )

        # Calling describe on a stream the user can't access should NOT raise --
        # it should warn and return None.
        result = ex.describe_stream("locked-down-stream")

        assert result is None
        assert any("locked-down-stream" in str(x) for x in ex.report.streams_failed)


class TestListStreamNamesErrorHandling:
    """list_stream_names mirrors the Firehose listing pattern (first-page
    failure → warning + skip section, mid-pagination failure → escalate to
    report.failure to surface the stateful-ingestion soft-delete risk).

    Without this, an IAM denial on kinesis:ListStreams crashes the whole
    get_workunits loop, and a mid-pagination ThrottlingException silently
    drops streams beyond the failure point — which stateful ingestion would
    then soft-delete on the next run.
    """

    def test_first_page_access_denied_yields_empty_with_warning_not_failure(self):
        ex = _make_extractor()
        # paginator.paginate() returns an iterator whose __next__ raises the
        # AWS error on the first call. Easiest mock: have .paginate() itself
        # raise via side_effect — the for-loop never enters the body, so
        # pages_fetched stays 0 and the warning path fires.
        ex._kinesis.get_paginator.return_value.paginate.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "ListStreams",
        )

        result = list(ex.list_stream_names())
        assert result == []
        # First-page failures are warnings, not failures — a user may legitimately
        # have IAM that excludes kinesis:ListStreams, and the connector should
        # still emit Firehose entities cleanly.
        assert list(ex.report.failures) == []
        warnings_text = " ".join(str(w) for w in ex.report.warnings)
        assert "Permission denied for Kinesis" in warnings_text
        assert "AccessDeniedException" in warnings_text

    def test_mid_pagination_failure_escalates_to_report_failure(self):
        """Critical: if page 1 yielded streams and page 2 fails, data is
        incomplete. Stateful ingestion would otherwise soft-delete every
        stream that wasn't on page 1 (because they weren't re-listed on this
        run). Must escalate to report.failure (not report.warning).
        """

        ex = _make_extractor()

        # Generator yields one good page, then raises on the second iteration.
        # Use a generator function so __next__ behavior is real Python iterator
        # semantics — no MagicMock iter() gymnastics.
        def _paginate():
            yield {"StreamNames": ["events", "clicks"]}
            raise ClientError(
                {
                    "Error": {
                        "Code": "ThrottlingException",
                        "Message": "slow down",
                    }
                },
                "ListStreams",
            )

        ex._kinesis.get_paginator.return_value.paginate.return_value = _paginate()

        result = list(ex.list_stream_names())
        # Page 1 streams were yielded successfully; iterator stops after the
        # page-2 failure.
        assert result == ["events", "clicks"]
        # Critical: the failure was escalated to report.failures, NOT warnings.
        failures_text = " ".join(str(f) for f in ex.report.failures)
        assert "mid-pagination" in failures_text
        assert "ThrottlingException" in failures_text
        # The user-facing message must call out the stateful-ingestion risk so
        # they know to re-run rather than treat the result as canonical.
        assert "soft-delete" in failures_text

    def test_first_page_botocore_error_also_skips_section(self):
        """Symmetric to ClientError test — covers the widened (ClientError,
        BotoCoreError) catch. A NoCredentialsError or
        EndpointConnectionError on the first page should skip the section
        gracefully, not crash the run.
        """

        ex = _make_extractor()
        ex._kinesis.get_paginator.return_value.paginate.side_effect = (
            EndpointConnectionError(
                endpoint_url="https://kinesis.us-east-1.amazonaws.com"
            )
        )

        result = list(ex.list_stream_names())
        assert result == []
        assert list(ex.report.failures) == []
        warnings_text = " ".join(str(w) for w in ex.report.warnings)
        # aws_error_code() returns the class name for BotoCoreError subclasses.
        assert "EndpointConnectionError" in warnings_text


def _stub_schema_metadata(stream_name: str) -> SchemaMetadataClass:
    """Build a minimal SchemaMetadataClass for tests — mirrors what
    KinesisGlueSchemaRegistry.get_schema_metadata would return after a
    successful GSR lookup + Avro parse. Uses OtherSchemaClass so we don't
    have to construct a real Avro string here.
    """
    return SchemaMetadataClass(
        schemaName=f"{stream_name}-schema",
        platform=make_data_platform_urn("kinesis"),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
        ],
    )


class TestEmitDatasetSchemaAttachment:
    """When the Glue Schema Registry resolves a schema for a stream, the
    emitted Dataset MUST carry the SchemaMetadata aspect — otherwise the
    schema lookup is wasted work. Regression: an earlier draft of
    _emit_dataset built the Dataset without passing `schema=...`, silently
    dropping resolved schemas from the emitted MCPs.
    """

    def test_resolved_schema_appears_in_emitted_workunits(self):
        # MagicMock with a get_schema_metadata returning our stub
        schema_registry = MagicMock()
        schema_registry.get_schema_metadata.return_value = _stub_schema_metadata(
            "events"
        )
        ex = _make_extractor(schema_registry=schema_registry)

        # Minimal StreamDescription — only the fields _custom_properties reads
        desc = {
            "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/events",
            "StreamStatus": "ACTIVE",
            "Shards": [{"ShardId": "shard-0"}],
            "RetentionPeriodHours": 24,
            "EncryptionType": "NONE",
        }
        wus = list(ex._emit_dataset("events", desc))  # type: ignore[arg-type]
        schema_aspects = [
            wu.metadata.aspect
            for wu in wus
            if wu.metadata.aspect is not None
            and type(wu.metadata.aspect).__name__ == "SchemaMetadataClass"
        ]
        assert schema_aspects, (
            "Dataset must emit a SchemaMetadata aspect when the schema registry "
            "resolved a schema for the stream — otherwise the GSR lookup is "
            "discarded silently."
        )
        # The schema we built has one field "id"
        field_paths = {f.fieldPath for f in schema_aspects[0].fields}
        assert "id" in field_paths
        schema_registry.get_schema_metadata.assert_called_once_with("events")

    def test_no_schema_when_registry_returns_none(self):
        """When GSR resolution returns None (naming-convention miss or
        explicit-map miss recorded elsewhere), the dataset should still emit
        but without a SchemaMetadata aspect. We don't want a bogus empty
        schema in the catalog.
        """
        schema_registry = MagicMock()
        schema_registry.get_schema_metadata.return_value = None
        ex = _make_extractor(schema_registry=schema_registry)

        desc = {
            "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/events",
            "StreamStatus": "ACTIVE",
            "Shards": [],
        }
        wus = list(ex._emit_dataset("events", desc))  # type: ignore[arg-type]
        schema_aspects = [
            wu.metadata.aspect
            for wu in wus
            if wu.metadata.aspect is not None
            and type(wu.metadata.aspect).__name__ == "SchemaMetadataClass"
        ]
        assert schema_aspects == [], (
            "Dataset must NOT emit an empty SchemaMetadata aspect when "
            "schema_registry.get_schema_metadata returns None."
        )

    def test_no_schema_registry_means_no_schema_aspect(self):
        """When GSR is disabled entirely (schema_registry=None — the
        LocalStack integration test path), _emit_dataset must NOT call
        get_schema_metadata. This is the contract that lets the LocalStack
        integration tests run without GSR (LocalStack's free tier lacks it).
        """
        ex = _make_extractor(schema_registry=None)
        desc = {"StreamARN": "arn:...", "StreamStatus": "ACTIVE", "Shards": []}
        wus = list(ex._emit_dataset("events", desc))  # type: ignore[arg-type]
        schema_aspects = [
            wu.metadata.aspect
            for wu in wus
            if wu.metadata.aspect is not None
            and type(wu.metadata.aspect).__name__ == "SchemaMetadataClass"
        ]
        assert schema_aspects == []
