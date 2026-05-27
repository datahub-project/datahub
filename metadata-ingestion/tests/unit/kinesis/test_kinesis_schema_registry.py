from unittest.mock import MagicMock

from botocore.exceptions import ClientError, EndpointConnectionError

from datahub.ingestion.source.kinesis.kinesis_config import (
    KinesisGlueSchemaRegistryConfig,
)
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_schema_registry import (
    KinesisGlueSchemaRegistry,
)

AVRO_SCHEMA_STR = (
    '{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}'
)


def _make_registry(stream_schema_map=None, use_naming_convention=True, enabled=True):
    # Note: `use_naming_convention=True` here is the TEST helper default, not the
    # CONFIG default. The config default flipped to False (no AWS-defined convention
    # for KDS); tests that exercise the naming-convention code path pass True
    # explicitly. See test_kinesis_config.py for the config-default coverage.
    cfg = KinesisGlueSchemaRegistryConfig(
        enabled=enabled,
        registry_name="default-registry",
        stream_schema_map=stream_schema_map or {},
        use_naming_convention=use_naming_convention,
    )
    glue_client = MagicMock()
    report = KinesisSourceReport()
    return KinesisGlueSchemaRegistry(
        config=cfg,
        glue_client=glue_client,
        platform_urn="urn:li:dataPlatform:kinesis",
        report=report,
    )


class TestGsrResolution:
    def test_explicit_stream_schema_map_wins_over_naming_convention(self):
        reg = _make_registry(stream_schema_map={"events": "events-schema-v2"})
        resolved = reg._resolve_schema_name("events")
        assert resolved == "events-schema-v2"

    def test_naming_convention_used_when_no_explicit_map(self):
        reg = _make_registry()
        resolved = reg._resolve_schema_name("clicks")
        assert resolved == "clicks"  # default: stream name == schema name

    def test_naming_convention_disabled_returns_none(self):
        reg = _make_registry(use_naming_convention=False)
        resolved = reg._resolve_schema_name("clicks")
        assert resolved is None

    def test_schema_not_found_returns_none(self):
        reg = _make_registry()
        reg._glue.get_schema_version.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
            "GetSchemaVersion",
        )
        result = reg.get_schema_metadata("events")
        assert result is None


class TestGsrAvroParsing:
    def test_avro_schema_parsed_to_schemafield_list(self):
        reg = _make_registry()
        reg._glue.get_schema_version.return_value = {
            "SchemaDefinition": AVRO_SCHEMA_STR,
            "DataFormat": "AVRO",
        }
        result = reg.get_schema_metadata("events")
        assert result is not None
        field_paths = {f.fieldPath for f in result.fields}
        # The Avro schema has one field "id"; schema_util.avro_schema_to_mce_fields
        # produces "id" or "[type=string]id"
        assert any("id" in fp for fp in field_paths)


class TestGsrReportWiring:
    def test_naming_convention_miss_records_as_quiet_miss_not_warning(self):
        """When use_naming_convention is true and the probed schema doesn't exist
        in the registry, that's the expected outcome for streams without schemas
        — NOT an error. The stream should be recorded in
        `gsr_naming_convention_misses` (info-only counter) and NOT in
        `schema_resolution_failures` (which is for real errors).
        """
        reg = _make_registry()
        reg._glue.get_schema_version.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
            "GetSchemaVersion",
        )
        result = reg.get_schema_metadata("events")
        assert result is None
        assert "events" in list(reg.report.gsr_naming_convention_misses)
        # And NOT in the error list
        assert list(reg.report.schema_resolution_failures) == []
        # No warnings emitted either — verified by absence of warnings (warnings
        # are mediated through reg.report.warning() which appends to source-level
        # warnings; default is empty).
        assert list(reg.report.warnings) == []

    def test_explicit_mapping_miss_still_records_as_error(self):
        """An EXPLICIT stream_schema_map miss IS an error — the user declared
        "this stream has this schema" and we couldn't find it. Different from
        a naming-convention probe miss; this should warn and land in
        schema_resolution_failures.
        """
        reg = _make_registry(stream_schema_map={"events": "events-schema-v2"})
        reg._glue.get_schema_version.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
            "GetSchemaVersion",
        )
        result = reg.get_schema_metadata("events")
        assert result is None
        recorded = [str(x) for x in reg.report.schema_resolution_failures]
        assert any("events" in r and "EntityNotFoundException" in r for r in recorded)
        assert list(reg.report.gsr_naming_convention_misses) == []

    def test_non_not_found_error_still_records_as_failure(self):
        """AccessDenied / Throttling / etc. are real failures even via naming
        convention — they should warn and land in schema_resolution_failures.
        Only EntityNotFoundException via naming convention is silent."""
        reg = _make_registry()
        reg._glue.get_schema_version.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "GetSchemaVersion",
        )
        result = reg.get_schema_metadata("events")
        assert result is None
        recorded = [str(x) for x in reg.report.schema_resolution_failures]
        assert any("events" in r and "AccessDeniedException" in r for r in recorded)
        assert list(reg.report.gsr_naming_convention_misses) == []

    def test_botocore_error_records_as_failure_not_crash(self):
        """The except clause was widened from `ClientError` to
        `(ClientError, BotoCoreError)` so that transient credential / network
        errors (NoCredentialsError, EndpointConnectionError,
        ConnectTimeoutError) on glue:GetSchemaVersion degrade to a per-stream
        warning rather than crashing the entire KDS extraction loop.

        This is the regression test for that widening: a BotoCoreError must
        NOT propagate out of get_schema_metadata, AND must land in
        schema_resolution_failures (not gsr_naming_convention_misses, since
        a credentials error is a real failure regardless of mapping type).
        """
        reg = _make_registry()
        reg._glue.get_schema_version.side_effect = EndpointConnectionError(
            endpoint_url="https://glue.us-east-1.amazonaws.com"
        )
        # Must NOT raise — degrade gracefully to None + report entry.
        result = reg.get_schema_metadata("events")
        assert result is None
        recorded = [str(x) for x in reg.report.schema_resolution_failures]
        # aws_error_code() returns the class name for BotoCoreError subclasses.
        assert any("events" in r and "EndpointConnectionError" in r for r in recorded)
        # Network errors are NOT naming-convention misses, even when probed via
        # naming convention — the silent counter is only for the legitimate
        # "no schema named X in registry Y" outcome.
        assert list(reg.report.gsr_naming_convention_misses) == []

    def test_parse_failure_records_resolution_failure(self):
        reg = _make_registry()
        reg._glue.get_schema_version.return_value = {
            "SchemaDefinition": "{not valid avro",
            "DataFormat": "AVRO",
        }
        result = reg.get_schema_metadata("events")
        assert result is None
        recorded = [str(x) for x in reg.report.schema_resolution_failures]
        assert any("events" in r and "parse-failed" in r for r in recorded)

    def test_unsupported_format_records_resolution_failure(self):
        reg = _make_registry()
        reg._glue.get_schema_version.return_value = {
            "SchemaDefinition": "",
            "DataFormat": "XML",  # not in {AVRO, JSON, PROTOBUF}
        }
        result = reg.get_schema_metadata("events")
        assert result is None
        recorded = [str(x) for x in reg.report.schema_resolution_failures]
        assert any(
            "events" in r and "unsupported-format" in r and "XML" in r for r in recorded
        )
