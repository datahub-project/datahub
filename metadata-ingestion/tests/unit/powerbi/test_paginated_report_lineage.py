"""Unit tests for paginated-report (RDL) lineage resolution.

These exercise the branches that, in CI, are only reached by the powerbi
integration tests (which don't contribute to patch coverage):

- ``Mapper.paginated_report_datasource_urns`` (powerbi.py)
- ``PowerBiAPI._resolve_paginated_report_lineage`` (powerbi_api.py)
- ``RegularAPIResolver.get_report_datasources`` (data_resolver.py)
"""

from typing import List, Optional
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Report,
    ReportDatasource,
    ReportType,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    RegularAPIResolver,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI

PBI_DATASET_DB = "sobe_wowvirtualserver-a8149dea-aa76-44e7-b6a2-cb888b71443f"


def _config(**overrides: object) -> PowerBiDashboardSourceConfig:
    base = {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "convert_lineage_urns_to_lowercase": False,
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.parse_obj(base)


def _paginated_report(datasources: List[ReportDatasource]) -> Report:
    return Report(
        id="r1",
        name="paginated-report",
        type=ReportType.PaginatedReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=None,
        dataset=None,
        pages=[],
        users=[],
        tags=[],
        datasources=datasources,
    )


def _mapper(config: PowerBiDashboardSourceConfig) -> Mapper:
    return Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )


# --- Mapper.paginated_report_datasource_urns --------------------------------


def test_datasource_urns_for_external_sql() -> None:
    mapper = _mapper(_config())
    report = _paginated_report(
        [ReportDatasource("Sql", "sales-sqlserver.internal", "MarketingDB")]
    )
    urns = mapper.paginated_report_datasource_urns(report)
    assert urns == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,"
        "sales-sqlserver.internal.MarketingDB,PROD)"
    ]


def test_datasource_urns_normalises_spaced_platform_name() -> None:
    # "Amazon Redshift" -> enum key "AmazonRedshift".
    mapper = _mapper(_config())
    report = _paginated_report([ReportDatasource("Amazon Redshift", "host", "db")])
    urns = mapper.paginated_report_datasource_urns(report)
    assert urns == ["urn:li:dataset:(urn:li:dataPlatform:redshift,host.db,PROD)"]


def test_datasource_urns_skips_pbi_dataset_binding() -> None:
    # AnalysisServices binding to a shared dataset is handled via report.dataset.
    mapper = _mapper(_config())
    report = _paginated_report(
        [
            ReportDatasource(
                "AnalysisServices", "pbiazure://api.powerbi.com/", PBI_DATASET_DB
            )
        ]
    )
    assert mapper.paginated_report_datasource_urns(report) == []


def test_datasource_urns_skips_unmapped_platform() -> None:
    reporter = PowerBiDashboardSourceReport()
    config = _config()
    mapper = Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=reporter,
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )
    report = _paginated_report([ReportDatasource("Db2", "db2host", "SAMPLE")])
    assert mapper.paginated_report_datasource_urns(report) == []


def test_datasource_urns_skips_platform_excluded_from_mapping() -> None:
    # "Sql" is a valid platform but excluded from a narrowed dataset_type_mapping.
    mapper = _mapper(_config(dataset_type_mapping={"PostgreSql": "postgres"}))
    report = _paginated_report([ReportDatasource("Sql", "host", "db")])
    assert mapper.paginated_report_datasource_urns(report) == []


# --- PowerBiAPI._resolve_paginated_report_lineage ---------------------------


def _api(
    *,
    admin_apis_only: bool = False,
    datasources: Optional[List[ReportDatasource]] = None,
    raise_error: bool = False,
    dataset_registry: Optional[dict] = None,
) -> PowerBiAPI:
    api = PowerBiAPI.__new__(PowerBiAPI)
    # Name-mangled private config attribute.
    api._PowerBiAPI__config = _config(  # type: ignore[attr-defined]
        admin_apis_only=admin_apis_only
    )
    api.reporter = PowerBiDashboardSourceReport()
    api.dataset_registry = dataset_registry or {}
    # log_http_error inspects sys.exc_info(); stub it (it's not under test here).
    api.log_http_error = MagicMock()  # type: ignore[method-assign]

    resolver = MagicMock()
    if raise_error:
        resolver.get_report_datasources.side_effect = requests.HTTPError("500")
    else:
        resolver.get_report_datasources.return_value = datasources or []
    api._get_resolver = MagicMock(return_value=resolver)  # type: ignore[method-assign]
    return api


def _workspace() -> MagicMock:
    ws = MagicMock()
    ws.id = "w1"
    ws.name = "demo-workspace"
    return ws


def test_resolve_lineage_admin_only_emits_distinct_event() -> None:
    api = _api(admin_apis_only=True)
    report = _paginated_report([])
    api._resolve_paginated_report_lineage(report, _workspace())
    assert report.datasources == []
    assert any(
        i.title == "Paginated Report Lineage Unavailable" for i in api.reporter.infos
    )


def test_resolve_lineage_handles_fetch_error() -> None:
    api = _api(raise_error=True)
    report = _paginated_report([])
    api._resolve_paginated_report_lineage(report, _workspace())
    assert any(
        w.title == "Paginated Report Datasources Fetch Failed"
        for w in api.reporter.warnings
    )


def test_resolve_lineage_binds_to_known_dataset() -> None:
    dataset = MagicMock()
    api = _api(
        datasources=[
            ReportDatasource(
                "AnalysisServices", "pbiazure://api.powerbi.com/", PBI_DATASET_DB
            )
        ],
        dataset_registry={"a8149dea-aa76-44e7-b6a2-cb888b71443f": dataset},
    )
    report = _paginated_report([])
    api._resolve_paginated_report_lineage(report, _workspace())
    assert report.dataset_id == "a8149dea-aa76-44e7-b6a2-cb888b71443f"
    assert report.dataset is dataset


def test_resolve_lineage_binding_to_unscanned_dataset() -> None:
    api = _api(
        datasources=[
            ReportDatasource(
                "AnalysisServices", "pbiazure://api.powerbi.com/", PBI_DATASET_DB
            )
        ],
        dataset_registry={},
    )
    report = _paginated_report([])
    api._resolve_paginated_report_lineage(report, _workspace())
    assert report.dataset is None


def test_resolve_lineage_empty_datasources() -> None:
    api = _api(datasources=[])
    report = _paginated_report([])
    api._resolve_paginated_report_lineage(report, _workspace())
    assert report.dataset_id is None


# --- RegularAPIResolver.get_report_datasources ------------------------------


def _regular_resolver(session: MagicMock) -> RegularAPIResolver:
    resolver = RegularAPIResolver.__new__(RegularAPIResolver)
    resolver._base_url = "https://api.powerbi.com/v1.0/myorg/groups"
    resolver._request_session = session
    resolver.get_authorization_header = MagicMock(return_value={})  # type: ignore[method-assign]
    return resolver


def test_regular_resolver_parses_datasources() -> None:
    session = MagicMock()
    response = MagicMock()
    response.json.return_value = {
        "value": [
            {
                "datasourceType": "Sql",
                "connectionDetails": {"server": "host", "database": "db"},
            },
            {"datasourceType": None},  # dropped by from_raw
        ]
    }
    session.get.return_value = response

    result = _regular_resolver(session).get_report_datasources(
        workspace=_workspace(), report_id="r1"
    )
    assert len(result) == 1
    assert result[0].datasource_type == "Sql"


def test_regular_resolver_raises_on_http_error() -> None:
    session = MagicMock()
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError("500")
    session.get.return_value = response

    with pytest.raises(requests.HTTPError):
        _regular_resolver(session).get_report_datasources(
            workspace=_workspace(), report_id="r1"
        )
