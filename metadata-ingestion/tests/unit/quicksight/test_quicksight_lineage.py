from types import SimpleNamespace
from unittest import mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.quicksight.processors.data_sources import (
    ResolvedDataSource,
)
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_lineage import (
    QuickSightLineageExtractor,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)

_QS_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:quicksight,064369473231.ds,PROD)"
_ATHENA_ARN = "arn:aws:quicksight:us-east-1:064369473231:datasource/athena"


def _athena_source() -> ResolvedDataSource:
    return ResolvedDataSource(
        arn=_ATHENA_ARN,
        data_source_id="athena-uuid",
        name="Athena DS",
        quicksight_type="ATHENA",
        platform="athena",
        dialect="athena",
    )


def _extractor(config_dict=None, data_source_map=None) -> QuickSightLineageExtractor:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    if data_source_map is None:
        data_source_map = {_ATHENA_ARN: _athena_source()}
    return QuickSightLineageExtractor(
        config,
        QuickSightSourceReport(),
        PipelineContext(run_id="test"),
        data_source_map=data_source_map,
    )


def _relational(catalog, schema, name, arn=_ATHENA_ARN):
    return {
        "t1": {
            "RelationalTable": {
                "DataSourceArn": arn,
                "Catalog": catalog,
                "Schema": schema,
                "Name": name,
            }
        }
    }


def test_relational_table_drops_athena_default_catalog():
    extractor = _extractor()
    lineage = extractor.get_upstream_lineage(
        _QS_DATASET_URN, _relational("AwsDataCatalog", "datahub_test", "sales_orders")
    )

    assert lineage is not None
    upstream = lineage.upstreams[0].dataset
    # AwsDataCatalog is dropped -> two-part athena URN (schema.table).
    assert "datahub_test.sales_orders" in upstream
    assert "AwsDataCatalog" not in upstream
    assert "dataPlatform:athena" in upstream


def test_relational_table_keeps_non_default_catalog():
    redshift = ResolvedDataSource(
        arn=_ATHENA_ARN,
        data_source_id="rs-uuid",
        name="RS",
        quicksight_type="REDSHIFT",
        platform="redshift",
        dialect="redshift",
    )
    extractor = _extractor(data_source_map={_ATHENA_ARN: redshift})
    lineage = extractor.get_upstream_lineage(
        _QS_DATASET_URN, _relational("prod_db", "public", "orders")
    )

    assert lineage is not None
    assert "prod_db.public.orders" in lineage.upstreams[0].dataset


def test_extract_lineage_disabled_returns_none():
    extractor = _extractor({"extract_lineage": False})
    assert (
        extractor.get_upstream_lineage(
            _QS_DATASET_URN, _relational("AwsDataCatalog", "s", "t")
        )
        is None
    )


def test_unknown_data_source_arn_is_skipped():
    extractor = _extractor(data_source_map={})
    assert (
        extractor.get_upstream_lineage(
            _QS_DATASET_URN, _relational("AwsDataCatalog", "s", "t")
        )
        is None
    )


def test_s3_source_uses_manifest_uri():
    s3 = ResolvedDataSource(
        arn=_ATHENA_ARN,
        data_source_id="s3-uuid",
        name="S3",
        quicksight_type="S3",
        platform="s3",
        dialect=None,
        s3_manifest_uri="my-bucket/data/manifest.json",
    )
    extractor = _extractor(data_source_map={_ATHENA_ARN: s3})
    lineage = extractor.get_upstream_lineage(
        _QS_DATASET_URN, {"t1": {"S3Source": {"DataSourceArn": _ATHENA_ARN}}}
    )

    assert lineage is not None
    upstream = lineage.upstreams[0].dataset
    assert "dataPlatform:s3" in upstream
    assert "my-bucket/data/manifest.json" in upstream


def test_external_override_applies_platform_instance_and_env():
    extractor = _extractor(
        {
            "external_data_sources": {
                "athena-uuid": {"platform_instance": "prod-athena", "env": "DEV"}
            }
        }
    )
    lineage = extractor.get_upstream_lineage(
        _QS_DATASET_URN, _relational("AwsDataCatalog", "datahub_test", "orders")
    )

    assert lineage is not None
    upstream = lineage.upstreams[0].dataset
    assert "prod-athena" in upstream
    assert upstream.endswith(",DEV)")


def test_custom_sql_emits_table_and_column_lineage():
    extractor = _extractor()
    parsed = SimpleNamespace(
        debug_info=SimpleNamespace(table_error=None, column_error=None),
        in_tables=[
            "urn:li:dataset:(urn:li:dataPlatform:athena,datahub_test.sales_orders,PROD)"
        ],
        column_lineage=[
            SimpleNamespace(
                downstream=SimpleNamespace(column="total_revenue"),
                upstreams=[
                    SimpleNamespace(
                        table="urn:li:dataset:(urn:li:dataPlatform:athena,datahub_test.sales_orders,PROD)",
                        column="revenue",
                    )
                ],
            )
        ],
    )

    physical_table_map = {
        "t1": {
            "CustomSql": {
                "DataSourceArn": _ATHENA_ARN,
                "SqlQuery": "SELECT SUM(revenue) AS total_revenue FROM datahub_test.sales_orders",
            }
        }
    }
    with mock.patch(
        "datahub.ingestion.source.quicksight.quicksight_lineage.create_lineage_sql_parsed_result",
        return_value=parsed,
    ):
        lineage = extractor.get_upstream_lineage(_QS_DATASET_URN, physical_table_map)

    assert lineage is not None
    assert lineage.upstreams[0].dataset == parsed.in_tables[0]
    assert lineage.fineGrainedLineages
    fgl = lineage.fineGrainedLineages[0]
    assert "total_revenue" in fgl.downstreams[0]
    assert "revenue" in fgl.upstreams[0]
    assert extractor.report.num_column_lineage_edges == 1


def test_custom_sql_parse_failure_is_counted_and_skipped():
    extractor = _extractor()
    parsed = SimpleNamespace(
        debug_info=SimpleNamespace(table_error="boom", column_error=None),
        in_tables=[],
        column_lineage=None,
    )
    physical_table_map = {
        "t1": {"CustomSql": {"DataSourceArn": _ATHENA_ARN, "SqlQuery": "SELECT bad"}}
    }
    with mock.patch(
        "datahub.ingestion.source.quicksight.quicksight_lineage.create_lineage_sql_parsed_result",
        return_value=parsed,
    ):
        lineage = extractor.get_upstream_lineage(_QS_DATASET_URN, physical_table_map)

    assert lineage is None
    assert extractor.report.num_sqlglot_parse_failures == 1
