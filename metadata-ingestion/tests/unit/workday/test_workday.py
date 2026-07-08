from datahub.ingestion.source.workday.config import (
    DataSourcePlatformConfig,
    WorkdayConfig,
)
from datahub.ingestion.source.workday.lineage import resolve_external_upstream_urn
from datahub.ingestion.source.workday.mapper import WorkdayMapper
from datahub.ingestion.source.workday.models import (
    PrismDataSource,
    PrismField,
    PrismTable,
)
from datahub.ingestion.source.workday.report import WorkdayReport


def _config(**overrides: object) -> WorkdayConfig:
    base = {
        "base_url": "https://wd2-impl-services1.workday.com/",
        "tenant": "acme",
        "client_id": "client",
        "client_secret": "secret",
    }
    base.update(overrides)
    return WorkdayConfig.model_validate(base)


def test_base_url_trailing_slash_is_normalized() -> None:
    assert _config().base_url == "https://wd2-impl-services1.workday.com"


def test_field_normalizer_resolves_aliases_and_type() -> None:
    field = PrismField.model_validate(
        {
            "displayName": "as_of_date",
            "type": {"descriptor": "Date"},
            "isPrimaryKey": True,
        }
    )
    assert field.name == "as_of_date"
    assert field.type_descriptor == "Date"
    assert field.is_primary_key is True


def test_table_normalizer_pulls_source_type_and_ids() -> None:
    table = PrismTable.model_validate(
        {
            "id": "tbl-1",
            "name": "Worker_Headcount",
            "sourceType": "Workday",
            "schema": {"fields": [{"name": "count", "type": "numeric"}]},
            "dataSources": [{"id": "ds-1"}, "ds-2"],
            "datasetId": "dset-1",
        }
    )
    assert table.source_type == "Workday"
    assert table.dataset_id == "dset-1"
    assert table.data_source_ids == ["ds-1", "ds-2"]
    assert [f.name for f in table.fields] == ["count"]


def test_external_upstream_resolves_only_when_mapped() -> None:
    config = _config(
        data_source_platform_mapping={
            "External Snowflake": DataSourcePlatformConfig(
                platform="snowflake",
                dataset_name="DB.Schema.Headcount",
            )
        },
        env="PROD",
    )
    # Mapped: resolves to a lowercased Snowflake dataset URN.
    urn = resolve_external_upstream_urn("External Snowflake", config)
    assert urn is not None
    assert "snowflake" in urn
    assert "db.schema.headcount" in urn
    # Unmapped: stays a native Workday object (no external URN).
    assert resolve_external_upstream_urn("Some Workday Source", config) is None


def test_external_upstream_preserves_case_when_configured() -> None:
    config = _config(
        data_source_platform_mapping={
            "S": DataSourcePlatformConfig(
                platform="snowflake",
                dataset_name="DB.Schema.T",
                convert_urns_to_lowercase=False,
            )
        }
    )
    urn = resolve_external_upstream_urn("S", config)
    assert urn is not None
    assert "DB.Schema.T" in urn


def test_unknown_field_type_falls_back_to_string_and_is_reported() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    schema = mapper._schema_aspect(
        "urn:li:dataset:(x)",
        [PrismField.model_validate({"name": "weird", "type": "quaternion"})],
    )
    assert schema is not None
    assert schema.fields[0].fieldPath == "weird"
    assert report.unknown_field_types == 1


def test_data_source_report_vs_external_subtype() -> None:
    report = WorkdayReport()
    mapper = WorkdayMapper(_config(), report)
    workday_source = PrismDataSource.model_validate(
        {"id": "ds-1", "name": "Headcount Report", "sourceType": "Workday"}
    )
    id_to_urn = {"ds-1": mapper.dataset_urn("source", "ds-1")}
    list(mapper.map_data_source(workday_source, id_to_urn))
    assert report.reports_scanned == 1
    assert report.data_sources_scanned == 0
