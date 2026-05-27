from typing import TYPE_CHECKING, Any, Dict, cast
from unittest.mock import MagicMock

from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_firehose import KinesisFirehoseExtractor
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.metadata.schema_classes import DataPlatformInstanceClass

if TYPE_CHECKING:
    from mypy_boto3_firehose.type_defs import DeliveryStreamDescriptionTypeDef


def _make_extractor() -> KinesisFirehoseExtractor:
    config = KinesisSourceConfig.model_validate(
        {"aws_config": {"aws_region": "us-east-1"}}
    )
    return KinesisFirehoseExtractor(
        config=config,
        report=KinesisSourceReport(),
        session=MagicMock(),
    )


class TestKinesisFirehoseExtractor:
    def test_dataflow_urn_uses_firehose_platform(self):
        """DataFlow's orchestrator is "kinesis-firehose", not "kinesis". AWS treats
        KDS and KDF as separate services with separate API namespaces / IAM prefixes /
        ARN formats; putting Firehose entities under their own platform makes the
        cross-service lineage edge (Stream(kinesis) -> DataJob(kinesis-firehose) ->
        S3(s3)) visually distinct in the lineage viewer.
        """
        ex = _make_extractor()
        urn = ex.dataflow_urn()
        assert urn == "urn:li:dataFlow:(kinesis-firehose,us-east-1-firehose,PROD)"

    def test_datajob_urn_under_dataflow(self):
        ex = _make_extractor()
        urn = ex.datajob_urn("events-to-s3")
        assert (
            urn
            == "urn:li:dataJob:(urn:li:dataFlow:(kinesis-firehose,us-east-1-firehose,PROD),events-to-s3)"
        )

    def test_dataflow_dataplatforminstance_aspect_uses_proper_urn(self):
        """Regression: DataPlatformInstanceClass.instance is a URN field.
        Passing the raw platform_instance string trips list_urns / guess_entity_type
        in the auto_materialize_referenced_tags_terms processor at the sink boundary
        ("urns must start with urn:li:"). The aspect must contain a fully-formed
        dataPlatformInstance URN, not the raw config value.
        """
        config = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "platform_instance": "us-east-1-prod",
            }
        )
        ex = KinesisFirehoseExtractor(
            config=config, report=KinesisSourceReport(), session=MagicMock()
        )
        wus = list(ex.get_dataflow_workunit())
        # The kinesis source only emits MCPWs; narrow with isinstance so
        # `.aspect` is reachable, then narrow the aspect itself by class.
        instance_aspects = [
            wu.metadata.aspect
            for wu in wus
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, DataPlatformInstanceClass)
        ]
        assert instance_aspects, "DataFlow must emit a DataPlatformInstance aspect"
        first_aspect = instance_aspects[0]
        assert isinstance(first_aspect, DataPlatformInstanceClass)
        instance_urn = first_aspect.instance
        assert instance_urn is not None and instance_urn.startswith(
            "urn:li:dataPlatformInstance:"
        ), f"instance must be a dataPlatformInstance URN, got: {instance_urn!r}"

    def test_filters_apply_to_delivery_streams(self):
        config = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "delivery_stream_pattern": {"deny": ["^_.*"]},
            }
        )
        report = KinesisSourceReport()
        session = MagicMock()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=session)
        # boto3 firehose has no paginator for list_delivery_streams; the extractor
        # uses manual `HasMoreDeliveryStreams` pagination instead. Cast the
        # client back to MagicMock — boto3 stubs type the methods as plain
        # Callables, which hides MagicMock's return_value / side_effect helpers.
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_delivery_streams.return_value = {
            "DeliveryStreamNames": ["events-to-s3", "clicks-to-s3", "_audit_pipe"],
            "HasMoreDeliveryStreams": False,
        }
        kept = list(ex.list_delivery_streams())
        assert "events-to-s3" in kept
        assert "_audit_pipe" not in kept
        assert any("_audit_pipe" in str(x) for x in report.filtered_delivery_streams)

    def test_list_delivery_streams_access_denied_yields_empty_with_warning(self):
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_delivery_streams.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "ListDeliveryStreams",
        )
        result = list(ex.list_delivery_streams())
        assert result == []
        # Warning recorded — the report should have a 'Permission denied for Firehose' entry
        warnings_text = " ".join(str(w) for w in ex.report.warnings)
        assert "Firehose" in warnings_text or "AccessDenied" in warnings_text

    def test_describe_delivery_stream_403_continues_with_warning(self):
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.describe_delivery_stream.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "DescribeDeliveryStream",
        )
        result = ex.describe_delivery_stream("locked-delivery-stream")
        assert result is None
        assert any(
            "locked-delivery-stream" in str(x)
            for x in ex.report.delivery_streams_failed
        )

    def test_mid_pagination_failure_escalates_to_report_failure(self):
        """Critical: if pagination fails mid-stream (page 1 succeeded, page 2
        threw), data is incomplete. Treating it as a warning would let stateful
        ingestion soft-delete every entity beyond the failure point. The
        failure must escalate to report.failure (not report.warning) so the
        pipeline aborts cleanly.
        """
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_delivery_streams.side_effect = [
            {"DeliveryStreamNames": ["ds-1", "ds-2"], "HasMoreDeliveryStreams": True},
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "slow down"}},
                "ListDeliveryStreams",
            ),
        ]
        result = list(ex.list_delivery_streams())
        # Page 1 yielded successfully; iterator stops after the page-2 failure.
        assert result == ["ds-1", "ds-2"]
        # The failure was escalated to report.failures, NOT warnings.
        failures_text = " ".join(str(f) for f in ex.report.failures)
        assert "mid-pagination" in failures_text
        assert "ThrottlingException" in failures_text

    def test_first_page_failure_skips_section_with_warning_not_failure(self):
        """If page 1 fails (typically IAM denied), no Firehose data exists yet.
        Treat as a warning + skip section — NOT a failure, since the connector
        can still emit KDS entities and the user may have intentionally
        omitted firehose:ListDeliveryStreams from the IAM policy.
        """
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_delivery_streams.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "ListDeliveryStreams",
        )
        result = list(ex.list_delivery_streams())
        assert result == []
        # No failures recorded; only a warning.
        assert list(ex.report.failures) == []
        warnings_text = " ".join(str(w) for w in ex.report.warnings)
        assert "Permission denied for Firehose" in warnings_text

    def test_list_delivery_streams_paginates_via_exclusive_start_name(self):
        """boto3's Firehose client has no paginator. The extractor must implement
        manual cursor pagination via ExclusiveStartDeliveryStreamName + HasMoreDeliveryStreams.
        """
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_delivery_streams.side_effect = [
            {"DeliveryStreamNames": ["ds-1", "ds-2"], "HasMoreDeliveryStreams": True},
            {"DeliveryStreamNames": ["ds-3"], "HasMoreDeliveryStreams": False},
        ]
        result = list(ex.list_delivery_streams())
        assert result == ["ds-1", "ds-2", "ds-3"]
        # The second call should have ExclusiveStartDeliveryStreamName="ds-2"
        calls = firehose_mock.list_delivery_streams.call_args_list
        assert len(calls) == 2
        second_kwargs = calls[1].kwargs
        assert second_kwargs.get("ExclusiveStartDeliveryStreamName") == "ds-2"


class TestFirehoseFetchTags:
    """Coverage for KinesisFirehoseExtractor.fetch_tags — mirrors the
    KinesisStreamExtractor.fetch_tags tests but exercises the Firehose-specific
    API (list_tags_for_delivery_stream) and the post-Phase-3 widened catch
    (ClientError, BotoCoreError) so credentialing failures don't crash the run.
    """

    def test_returns_tags_when_api_succeeds(self):
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_tags_for_delivery_stream.return_value = {
            "Tags": [
                {"Key": "owner", "Value": "data-team"},
                {"Key": "env", "Value": "prod"},
            ]
        }
        tags = ex.fetch_tags("events-to-s3")
        assert {"Key": "owner", "Value": "data-team"} in tags
        assert {"Key": "env", "Value": "prod"} in tags

    def test_short_circuits_when_neither_tags_nor_owners_enabled(self):
        """When both extract_tags and extract_owners are False, fetch_tags must
        NOT call boto3 at all — saves an IAM permission and an API round-trip
        per delivery stream when the user has explicitly disabled tag/owner
        extraction.
        """
        config = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "extract_tags": False,
                "extract_owners": False,
            }
        )
        ex = KinesisFirehoseExtractor(
            config=config, report=KinesisSourceReport(), session=MagicMock()
        )
        tags = ex.fetch_tags("events-to-s3")
        assert tags == []
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_tags_for_delivery_stream.assert_not_called()

    def test_client_error_yields_empty_with_warning(self):
        """AccessDeniedException on ListTagsForDeliveryStream is non-fatal:
        dataset emission must continue without tags / ownership. The error
        code (AccessDeniedException) should appear in the warning message so
        users can map the report back to a missing IAM permission.
        """
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_tags_for_delivery_stream.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "ListTagsForDeliveryStream",
        )
        tags = ex.fetch_tags("events-to-s3")
        assert tags == []
        warnings_text = " ".join(str(w) for w in ex.report.warnings)
        assert "AccessDeniedException" in warnings_text
        assert "events-to-s3" in warnings_text

    def test_botocore_error_yields_empty_with_warning(self):
        """BotoCoreError (e.g. EndpointConnectionError from a misconfigured
        region) is the other half of the widened (ClientError, BotoCoreError)
        catch. Without this branch, a transient endpoint failure during tag
        fetch would crash the whole Firehose extraction.
        """
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_tags_for_delivery_stream.side_effect = (
            EndpointConnectionError(
                endpoint_url="https://firehose.us-east-1.amazonaws.com"
            )
        )
        tags = ex.fetch_tags("events-to-s3")
        assert tags == []
        warnings_text = " ".join(str(w) for w in ex.report.warnings)
        # For BotoCoreError, the code is the exception's class name (not response code).
        assert "EndpointConnectionError" in warnings_text

    def test_coerces_missing_tag_fields_to_empty_strings(self):
        """boto3 TagTypeDef declares Key/Value as NotRequired[str] (the wire
        format may omit them). The extractor must materialise to Dict[str,str]
        with empty-string fallbacks so the shared kinesis_tagging helpers
        downstream never get a None they don't expect.
        """
        ex = _make_extractor()
        firehose_mock = cast(MagicMock, ex._firehose)
        firehose_mock.list_tags_for_delivery_stream.return_value = {
            "Tags": [
                {"Key": "owner"},
                {"Value": "prod"},
            ]  # each missing the other field
        }
        tags = ex.fetch_tags("events-to-s3")
        assert {"Key": "owner", "Value": ""} in tags
        assert {"Key": "", "Value": "prod"} in tags


class TestFirehoseLineage:
    def test_s3_destination_emits_input_output_edges(self):
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        report = KinesisSourceReport()
        session = MagicMock()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=session)

        delivery_desc = {
            "DeliveryStreamARN": "arn:aws:firehose:us-east-1:000000000000:deliverystream/events-to-s3",
            "DeliveryStreamName": "events-to-s3",
            "DeliveryStreamStatus": "ACTIVE",
            "DeliveryStreamType": "KinesisStreamAsSource",
            "Source": {
                "KinesisStreamSourceDescription": {
                    "KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/events"
                }
            },
            "Destinations": [
                {
                    "S3DestinationDescription": {
                        "BucketARN": "arn:aws:s3:::analytics-lake",
                        "Prefix": "events/",
                    }
                }
            ],
        }
        edges = ex.build_input_output(
            cast("DeliveryStreamDescriptionTypeDef", delivery_desc)
        )
        assert edges is not None
        # Stream URN's name is region-prefixed (`<region>.<stream>`) so cross-region
        # collisions are impossible; must match what KinesisStreamExtractor emits.
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:kinesis,us-east-1.events,PROD)"
            in edges.inputDatasets
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:s3,analytics-lake/events/,PROD)"
            in edges.outputDatasets
        )

    def test_matched_handler_but_empty_urns_emits_warning_and_counter(self):
        """Regression for the matched-but-empty-build_urns silent path. When a
        handler matches a destination but build_urns() returns [] (e.g.,
        BucketARN empty, JDBC URL unparseable, Snowflake missing one of
        db/schema/table), the orchestrator must:
          - Emit a `report.warning` so the user can debug the missing edge.
          - Increment the `destination_parse_failures` counter so the report
            distinguishes "handler matched but unbuildable" from
            "no handler at all" (which already exists as
            unsupported_destinations).

        Without this, a misconfigured Firehose produces a DataJob with no
        lineage and no diagnostic — the user has no way to tell whether the
        destination is unsupported, AWS returned malformed data, or the
        connector has a bug.
        """
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        report = KinesisSourceReport()
        session = MagicMock()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=session)
        delivery_desc = {
            "DeliveryStreamName": "events-to-redshift",
            "DeliveryStreamType": "KinesisStreamAsSource",
            "Source": {
                "KinesisStreamSourceDescription": {
                    "KinesisStreamARN": (
                        "arn:aws:kinesis:us-east-1:000000000000:stream/events"
                    )
                }
            },
            "Destinations": [
                {
                    # Redshift handler matches on RedshiftDestinationDescription,
                    # but the JDBC URL has no path (no database), so
                    # _parse_redshift_db returns None and build_urns returns [].
                    "RedshiftDestinationDescription": {
                        "ClusterJDBCURL": "jdbc:redshift://no-database-here",
                        "CopyCommand": {"DataTableName": "public.events"},
                    }
                }
            ],
        }
        edges = ex.build_input_output(
            cast("DeliveryStreamDescriptionTypeDef", delivery_desc)
        )
        # Output is empty because URN couldn't be built.
        assert edges.outputDatasets == []
        # The destination_parse_failures counter records the matched-but-empty
        # case, distinct from unsupported_destinations.
        parse_failures = [str(x) for x in report.destination_parse_failures]
        assert any(
            "events-to-redshift" in r and "RedshiftDestination" in r
            for r in parse_failures
        )
        # And NOT in unsupported_destinations (the handler DID match).
        assert list(report.unsupported_destinations) == []
        # A warning was emitted with the actionable diagnostic.
        warnings_text = " ".join(str(w) for w in report.warnings)
        assert "RedshiftDestination" in warnings_text
        assert "missing or malformed required fields" in warnings_text

    def test_unsupported_destination_emits_warning_and_no_output(self):
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        report = KinesisSourceReport()
        session = MagicMock()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=session)
        delivery_desc = {
            "DeliveryStreamName": "events-to-datadog",
            "DeliveryStreamType": "DirectPut",
            "Destinations": [
                {
                    "HttpEndpointDestinationDescription": {
                        "EndpointConfiguration": {"Url": "https://..."}
                    }
                }
            ],
        }
        edges = ex.build_input_output(
            cast("DeliveryStreamDescriptionTypeDef", delivery_desc)
        )
        # Inputs may be empty (DirectPut has no upstream stream); outputs empty
        # since destination is unsupported.
        assert edges.outputDatasets == []
        assert any(
            "events-to-datadog" in str(x) for x in report.unsupported_destinations
        )

    def test_destination_platform_map_overrides_snowflake_instance(self):
        config = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "destination_platform_map": {
                    "snowflake": {"platform_instance": "prod-sf", "env": "PROD"}
                },
            }
        )
        report = KinesisSourceReport()
        session = MagicMock()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=session)
        delivery_desc = {
            "DeliveryStreamName": "events-to-snowflake",
            "DeliveryStreamType": "KinesisStreamAsSource",
            "Source": {
                "KinesisStreamSourceDescription": {
                    "KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/events"
                }
            },
            "Destinations": [
                {
                    "SnowflakeDestinationDescription": {
                        "Database": "DB",
                        "Schema": "S",
                        "Table": "T",
                    }
                }
            ],
        }
        edges = ex.build_input_output(
            cast("DeliveryStreamDescriptionTypeDef", delivery_desc)
        )
        # `make_dataset_urn_with_platform_instance` folds platform_instance into
        # the name as a `<platform_instance>.<name>` prefix (not into a 4-tuple).
        # Test intent: platform_instance must appear in the URN.
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod-sf.db.s.t,PROD)"
            in edges.outputDatasets
        )


class TestDestinationUrnCaseFolding:
    """The orchestrator's `_destination_urn` lowercases the URN name by
    default. Per-destination override via
    `destination_platform_map.<platform>.convert_urns_to_lowercase: false`
    preserves the original case — needed for case-sensitive destinations
    (Iceberg, MongoDB) or Snowflake/Redshift sources ingested with
    `convert_urns_to_lowercase=False`.
    """

    def _snowflake_desc(self) -> dict:
        return {
            "DeliveryStreamName": "events-to-snowflake",
            "DeliveryStreamType": "DirectPut",
            "Destinations": [
                {
                    "SnowflakeDestinationDescription": {
                        "Database": "PROD_DB",
                        "Schema": "ANALYTICS",
                        "Table": "EVENTS",
                    }
                }
            ],
        }

    def test_default_lowercases_snowflake_urn(self):
        """No `destination_platform_map` entry: default behavior is to
        lowercase the URN name, matching the Snowflake DataHub source's
        default `convert_urns_to_lowercase=True`.
        """
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        ex = KinesisFirehoseExtractor(
            config=config, report=KinesisSourceReport(), session=MagicMock()
        )
        edges = ex.build_input_output(
            cast("DeliveryStreamDescriptionTypeDef", self._snowflake_desc())
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod_db.analytics.events,PROD)"
            in edges.outputDatasets
        )

    def test_override_to_false_preserves_original_case(self):
        """Set `convert_urns_to_lowercase: false` for a destination whose
        source ingested with that flag also False (or for case-sensitive
        destinations like Iceberg / MongoDB).
        """
        config = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "destination_platform_map": {
                    "snowflake": {"convert_urns_to_lowercase": False}
                },
            }
        )
        ex = KinesisFirehoseExtractor(
            config=config, report=KinesisSourceReport(), session=MagicMock()
        )
        edges = ex.build_input_output(
            cast("DeliveryStreamDescriptionTypeDef", self._snowflake_desc())
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD_DB.ANALYTICS.EVENTS,PROD)"
            in edges.outputDatasets
        )


class TestFirehoseSchemaConfigLineage:
    """SchemaConfiguration (Firehose's Parquet/ORC format conversion) references
    a Glue table. We surface that table as an upstream input on the DataJob's
    dataJobInputOutput aspect — the table's schema governs what gets written.
    S3 path stays as output."""

    def _delivery_desc_with_extended_s3(self, schema_cfg=None):
        """Helper: minimal Extended S3 delivery stream description with optional
        SchemaConfiguration. None = no format conversion at all."""
        extended_s3: Dict[str, Any] = {
            "BucketARN": "arn:aws:s3:::analytics-lake",
            "Prefix": "events/",
        }
        if schema_cfg is not None:
            extended_s3["DataFormatConversionConfiguration"] = {
                "SchemaConfiguration": schema_cfg,
            }
        return {
            "DeliveryStreamARN": "arn:aws:firehose:us-east-1:000000000000:deliverystream/events-to-s3",
            "DeliveryStreamName": "events-to-s3",
            "DeliveryStreamStatus": "ACTIVE",
            "DeliveryStreamType": "KinesisStreamAsSource",
            "Source": {
                "KinesisStreamSourceDescription": {
                    "KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/events"
                }
            },
            "Destinations": [{"ExtendedS3DestinationDescription": extended_s3}],
        }

    def test_extended_s3_with_complete_schema_config_emits_glue_as_input(self):
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        report = KinesisSourceReport()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=MagicMock())
        desc = self._delivery_desc_with_extended_s3(
            schema_cfg={
                "CatalogId": "123456789012",
                "DatabaseName": "analytics",
                "TableName": "events",
            }
        )
        edges = ex.build_input_output(desc)
        # Source Kinesis stream still upstream; Glue table NOW also upstream.
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:kinesis,us-east-1.events,PROD)"
            in edges.inputDatasets
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:glue,analytics.events,PROD)"
            in edges.inputDatasets
        )
        # S3 path stays as output.
        assert any("dataPlatform:s3" in u for u in edges.outputDatasets)
        assert report.firehose_glue_schema_lineage_emitted == 1
        assert list(report.firehose_glue_schema_skipped) == []

    def test_extended_s3_with_partial_schema_config_skips_with_report_entry(self):
        """If DatabaseName is missing, we can't build the Glue URN — skip and
        record a report entry naming both the delivery stream and the field.
        Note: CatalogId is intentionally NOT required (AWS omits it from
        DescribeDeliveryStream responses when it equals the caller's account).
        """
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        report = KinesisSourceReport()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=MagicMock())
        desc = self._delivery_desc_with_extended_s3(
            schema_cfg={
                "TableName": "events",
                # DatabaseName omitted — required for the URN's "<db>.<table>" name
            }
        )
        edges = ex.build_input_output(desc)
        # No Glue URN in inputs — only the source Kinesis stream.
        assert not any("dataPlatform:glue" in u for u in edges.inputDatasets)
        assert report.firehose_glue_schema_lineage_emitted == 0
        skipped = list(report.firehose_glue_schema_skipped)
        assert len(skipped) == 1
        assert "events-to-s3" in skipped[0] and "DatabaseName" in skipped[0]

    def test_extended_s3_without_schema_config_emits_no_glue_input_silently(self):
        """No format conversion at all (most Firehose deliveries). Glue extraction
        is silent — no skip-list entry, no counter increment."""
        config = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        report = KinesisSourceReport()
        ex = KinesisFirehoseExtractor(config=config, report=report, session=MagicMock())
        desc = self._delivery_desc_with_extended_s3(schema_cfg=None)
        edges = ex.build_input_output(desc)
        assert not any("dataPlatform:glue" in u for u in edges.inputDatasets)
        assert report.firehose_glue_schema_lineage_emitted == 0
        assert list(report.firehose_glue_schema_skipped) == []
