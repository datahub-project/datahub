import random
from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.kinesis.kinesis_firehose_destinations import (
    DESTINATION_HANDLERS,
    ExtendedS3Destination,
    IcebergDestination,
    MongoDBDestination,
    OpenSearchDestination,
    RedshiftDestination,
    S3Destination,
    SnowflakeDestination,
)
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport


def _urn_builder(
    platform: str, name: str, env: str = "PROD", platform_instance: Optional[str] = None
) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=platform, name=name, env=env, platform_instance=platform_instance
    )


class TestS3Destination:
    def test_basic_bucket_only(self):
        h = S3Destination()
        d = {"S3DestinationDescription": {"BucketARN": "arn:aws:s3:::analytics-lake"}}
        assert h.matches(d) is True
        urns = h.build_urns(d, _urn_builder)
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,analytics-lake,PROD)"]

    def test_bucket_with_prefix(self):
        h = S3Destination()
        d = {
            "S3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
                "Prefix": "events/",
            }
        }
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,analytics-lake/events/,PROD)"
        ]

    def test_rejects_extended_s3_responses_to_be_order_independent(self):
        """Modern Firehose returns BOTH `S3DestinationDescription` and
        `ExtendedS3DestinationDescription` on the same destination dict (the
        basic key is a read-only summary, the Extended key carries the full
        config). The legacy `S3Destination.matches` must explicitly reject
        these responses so the registry order in `DESTINATION_HANDLERS` is
        an optimization rather than a correctness requirement: anyone who
        reorders the list shouldn't accidentally break Glue lineage by
        having `S3Destination` win the dispatch.
        """
        h = S3Destination()
        d = {
            "S3DestinationDescription": {"BucketARN": "arn:aws:s3:::bk"},
            "ExtendedS3DestinationDescription": {"BucketARN": "arn:aws:s3:::bk"},
        }
        assert h.matches(d) is False

    def test_refuses_to_emit_when_bucket_arn_missing(self):
        """Defensive: an empty/missing BucketARN must not produce
        urn:li:dataset:(s3,,PROD) — that's the silent-malformed-URN pattern
        the connector's own design warns against.
        """
        h = S3Destination()
        d = {"S3DestinationDescription": {"BucketARN": ""}}
        assert h.build_urns(d, _urn_builder) == []


class TestRedshiftDestination:
    def test_full_db_schema_table_url(self):
        """Happy path: JDBC URL contains a parseable database name, and
        DataTableName is <schema>.<table>. URN is <db>.<schema>.<table>.
        """
        h = RedshiftDestination()
        d = {
            "RedshiftDestinationDescription": {
                "ClusterJDBCURL": "jdbc:redshift://host.example.com:5439/analytics",
                "CopyCommand": {"DataTableName": "public.events"},
            }
        }
        assert h.matches(d) is True
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:redshift,analytics.public.events,PROD)"
        ]

    def test_refuses_to_emit_when_jdbc_has_no_database(self):
        """When the JDBC URL is malformed (no path), the database cannot be
        parsed and the handler MUST refuse to emit rather than producing
        urn:li:dataset:(redshift,events,PROD) — a syntactically valid but
        semantically wrong URN that wouldn't resolve in DataHub.
        """
        h = RedshiftDestination()
        d = {
            "RedshiftDestinationDescription": {
                "ClusterJDBCURL": "jdbc:redshift://no-database-here",
                "CopyCommand": {"DataTableName": "public.events"},
            }
        }
        assert h.build_urns(d, _urn_builder) == []

    def test_refuses_to_emit_when_table_lacks_schema_prefix(self):
        """When DataTableName is a bare table name (no `.`), we can't construct
        the full <db>.<schema>.<table> URN. Return None — orchestrator will
        report this as an unsupported destination.
        """
        h = RedshiftDestination()
        d = {
            "RedshiftDestinationDescription": {
                "ClusterJDBCURL": "jdbc:redshift://host:5439/analytics",
                "CopyCommand": {"DataTableName": "events"},  # no schema prefix
            }
        }
        assert h.build_urns(d, _urn_builder) == []


class TestOpenSearchDestination:
    def test_index_only(self):
        h = OpenSearchDestination()
        d = {
            "AmazonopensearchserviceDestinationDescription": {
                "IndexName": "events-index",
                "DomainARN": "arn:aws:es:us-east-1:000000000000:domain/my-domain",
            }
        }
        assert h.matches(d) is True
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,events-index,PROD)"
        ]


class TestSnowflakeDestination:
    def test_handler_emits_name_as_is(self):
        """The handler itself no longer case-folds — that's
        delegated to ``KinesisFirehoseExtractor._destination_urn`` so it
        can be controlled by
        ``destination_platform_map.<platform>.convert_urns_to_lowercase``.
        See the orchestrator-level lowercasing tests in
        ``test_kinesis_firehose.py``.
        """
        h = SnowflakeDestination()
        d = {
            "SnowflakeDestinationDescription": {
                "Database": "PROD_DB",
                "Schema": "ANALYTICS",
                "Table": "EVENTS",
            }
        }
        assert h.matches(d) is True
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD_DB.ANALYTICS.EVENTS,PROD)"
        ]


class TestIcebergDestination:
    def test_namespace_table(self):
        h = IcebergDestination()
        d = {
            "IcebergDestinationDescription": {
                "CatalogConfiguration": {"CatalogARN": "arn:..."},
                "DestinationTableConfigurationList": [
                    {
                        "DestinationTableName": "events",
                        "DestinationDatabaseName": "lake",
                    }
                ],
            }
        }
        assert h.matches(d) is True
        urns = h.build_urns(d, _urn_builder)
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,PROD)"]

    def test_multi_table_emits_one_urn_per_table(self):
        """Firehose's Iceberg destination is the only V1 destination type that
        targets multiple downstream tables from a single delivery stream
        (`DestinationTableConfigurationList` is a list). The handler emits one
        lineage URN per configured table — no silent drop, just all the edges
        the user configured.
        """
        h = IcebergDestination()
        d = {
            "IcebergDestinationDescription": {
                "CatalogConfiguration": {"CatalogARN": "arn:..."},
                "DestinationTableConfigurationList": [
                    {
                        "DestinationTableName": "events",
                        "DestinationDatabaseName": "lake",
                    },
                    {
                        "DestinationTableName": "clicks",
                        "DestinationDatabaseName": "lake",
                    },
                    {
                        "DestinationTableName": "errors",
                        "DestinationDatabaseName": "lake",
                    },
                ],
            }
        }
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.clicks,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.errors,PROD)",
        ]

    def test_skips_malformed_table_entry_but_keeps_valid_ones(self):
        """A table entry without a TableName can't form a URN; skip just that
        entry, keep the valid ones. The orchestrator's outputDatasets will
        still get every well-formed edge.
        """
        h = IcebergDestination()
        d = {
            "IcebergDestinationDescription": {
                "CatalogConfiguration": {"CatalogARN": "arn:..."},
                "DestinationTableConfigurationList": [
                    {
                        "DestinationTableName": "events",
                        "DestinationDatabaseName": "lake",
                    },
                    {"DestinationDatabaseName": "lake"},  # missing TableName
                    {
                        "DestinationTableName": "errors",
                        "DestinationDatabaseName": "lake",
                    },
                ],
            }
        }
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.events,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,lake.errors,PROD)",
        ]


class TestMongoDBDestination:
    def test_db_collection(self):
        h = MongoDBDestination()
        d = {
            "AmazonopensearchserviceDestinationDescription": None,
            "S3DestinationDescription": None,
            # Firehose's MongoDB destination puts info under a vendor-specific block.
            # The Firehose API uses different keys per vendor; consult docs at impl time.
            # This test asserts the URN shape, regardless of exact upstream key.
            "MongoDBDestinationDescription": {
                "DatabaseName": "analytics",
                "CollectionName": "events",
            },
        }
        assert h.matches(d) is True
        urns = h.build_urns(d, _urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:mongodb,analytics.events,PROD)"
        ]


class TestDestinationHandlerRegistry:
    def test_registry_lists_expected_handlers(self):
        """Registry has 7 handlers: 6 distinct platforms (s3, redshift,
        elasticsearch, snowflake, iceberg, mongodb) plus an ExtendedS3Destination
        alongside the legacy S3Destination. Both S3 handlers share platform="s3"
        but match mutually exclusive description keys.
        """
        assert len(DESTINATION_HANDLERS) == 7
        platforms = {h.platform for h in DESTINATION_HANDLERS}
        assert platforms == {
            "s3",
            "redshift",
            "elasticsearch",
            "snowflake",
            "iceberg",
            "mongodb",
        }

    def test_unsupported_destination_returns_none(self):
        # If no handler matches, the caller should get None (and report_unsupported_destination).
        unknown = {"DatadogDestinationDescription": {"Url": "..."}}
        for h in DESTINATION_HANDLERS:
            assert h.matches(unknown) is False

    def test_extended_s3_wins_dispatch_regardless_of_order(self):
        """Modern Firehose returns BOTH `S3DestinationDescription` and
        `ExtendedS3DestinationDescription` keys on the same dict. The
        orchestrator picks the first matching handler via `next(...
        h.matches(d))`, so handler order in DESTINATION_HANDLERS would
        normally be load-bearing.

        S3Destination.matches explicitly rejects responses that ALSO carry
        the Extended key, which makes order an optimization rather than a
        correctness requirement. This test simulates the orchestrator's
        dispatch over a shuffled handler list to prove the property.
        """
        d = {
            "S3DestinationDescription": {"BucketARN": "arn:aws:s3:::bk"},
            "ExtendedS3DestinationDescription": {"BucketARN": "arn:aws:s3:::bk"},
        }
        # 50 shuffled orderings — a deterministic seed would defeat the point.
        for _ in range(50):
            shuffled = list(DESTINATION_HANDLERS)
            random.shuffle(shuffled)
            winner = next((h for h in shuffled if h.matches(d)), None)
            assert winner is not None
            assert isinstance(winner, ExtendedS3Destination), (
                f"Expected ExtendedS3Destination to win, got {type(winner).__name__}. "
                f"S3Destination.matches must reject Extended responses to keep "
                f"DESTINATION_HANDLERS order an optimization, not a correctness "
                f"requirement."
            )


class TestExtendedS3Destination:
    """Parallel to S3Destination but matches ExtendedS3DestinationDescription.
    Modern Firehose deliveries return this shape; basic S3DestinationDescription
    is legacy. Both produce the same S3 URN shape (bucket + prefix)."""

    def _urn_builder(self, platform, name, env=None):
        # Matches the existing _destination_urn callable signature in
        # kinesis_firehose.py — no platform_instance, env fallback to PROD.
        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name,
            platform_instance=None,
            env=env or "PROD",
        )

    def test_matches_extended_description_key(self):
        d = {"ExtendedS3DestinationDescription": {"BucketARN": "arn:aws:s3:::bk"}}
        assert ExtendedS3Destination().matches(d) is True

    def test_does_not_match_basic_description_key(self):
        d = {"S3DestinationDescription": {"BucketARN": "arn:aws:s3:::bk"}}
        assert ExtendedS3Destination().matches(d) is False

    def test_build_urn_uses_bucket_and_prefix(self):
        d = {
            "ExtendedS3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
                "Prefix": "events/",
            }
        }
        urns = ExtendedS3Destination().build_urns(d, self._urn_builder)
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,analytics-lake/events/,PROD)"
        ]

    def test_build_urn_without_prefix_uses_bucket_alone(self):
        d = {
            "ExtendedS3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
            }
        }
        urns = ExtendedS3Destination().build_urns(d, self._urn_builder)
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,analytics-lake,PROD)"]

    def test_extract_schema_config_returns_glue_urn_when_complete(self):
        d = {
            "ExtendedS3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
                "DataFormatConversionConfiguration": {
                    "SchemaConfiguration": {
                        "CatalogId": "123456789012",
                        "DatabaseName": "analytics",
                        "TableName": "events",
                        "Region": "us-east-1",
                        "VersionId": "LATEST",
                    },
                },
            }
        }
        report = KinesisSourceReport()
        urn = ExtendedS3Destination().extract_schema_config_glue_urn(
            d, self._urn_builder, report, "events-to-s3"
        )
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:glue,analytics.events,PROD)"
        # No skip recorded — emit path doesn't write to the report.
        assert list(report.firehose_glue_schema_skipped) == []

    def test_extract_schema_config_succeeds_without_catalog_id(self):
        """AWS omits CatalogId from DescribeDeliveryStream responses when it
        equals the caller's account (per AWS docs, CatalogId is an input-side
        default). DataHub's Glue URN doesn't include CatalogId, so its absence
        on the AWS side has no effect on URN correctness — must NOT skip.
        """
        d = {
            "ExtendedS3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
                "DataFormatConversionConfiguration": {
                    "SchemaConfiguration": {
                        "DatabaseName": "analytics",
                        "TableName": "events",
                        # CatalogId omitted (AWS's default behavior)
                    },
                },
            }
        }
        report = KinesisSourceReport()
        urn = ExtendedS3Destination().extract_schema_config_glue_urn(
            d, self._urn_builder, report, "events-to-s3"
        )
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:glue,analytics.events,PROD)"
        assert list(report.firehose_glue_schema_skipped) == []

    def test_extract_schema_config_returns_skip_reason_when_database_name_missing(self):
        """If DatabaseName is missing we can't build the Glue URN — the
        handler returns None AND records a skip-reason in the report so the
        user can debug the misconfiguration.
        """
        d = {
            "ExtendedS3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
                "DataFormatConversionConfiguration": {
                    "SchemaConfiguration": {
                        "TableName": "events",
                        # DatabaseName omitted
                    },
                },
            }
        }
        report = KinesisSourceReport()
        urn = ExtendedS3Destination().extract_schema_config_glue_urn(
            d, self._urn_builder, report, "events-to-s3"
        )
        assert urn is None
        skipped = [str(x) for x in report.firehose_glue_schema_skipped]
        assert any("events-to-s3" in s and "DatabaseName" in s for s in skipped)

    def test_extract_schema_config_returns_none_silently_when_format_conversion_missing(
        self,
    ):
        """Distinguish 'no format conversion configured' (silent, returns
        None without writing to report) from 'format conversion configured
        but incomplete' (returns None AND records a skip-reason).
        """
        d = {
            "ExtendedS3DestinationDescription": {
                "BucketARN": "arn:aws:s3:::analytics-lake",
                # No DataFormatConversionConfiguration at all
            }
        }
        report = KinesisSourceReport()
        urn = ExtendedS3Destination().extract_schema_config_glue_urn(
            d, self._urn_builder, report, "events-to-s3"
        )
        assert urn is None
        # Critical: the silent-absent path must NOT write to the report —
        # otherwise every non-format-converting destination would emit a
        # spurious "skipped" entry.
        assert list(report.firehose_glue_schema_skipped) == []
