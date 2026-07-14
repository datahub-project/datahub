from typing import Any, Dict, Iterator, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.models import Project
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport
from datahub.ingestion.source.microstrategy.source import MicroStrategySource
from datahub.ingestion.source.microstrategy.usage import (
    MicroStrategyUsageExtractor,
    build_usage_query_body,
    parse_usage_date_ms,
    parse_usage_page,
    resolve_usage_query_plan,
)
from datahub.metadata.schema_classes import (
    ChartUsageStatisticsClass,
    DashboardUsageStatisticsClass,
)

# Shapes below mirror real /api/v2/cubes responses captured from a Platform
# Analytics (Agg) cube, trimmed to the fields the extractor reads.
_CUBE_DEFINITION: Dict[str, Any] = {
    "definition": {
        "availableObjects": {
            "attributes": [
                {
                    "name": "Date",
                    "id": "DATE-ATTR",
                    "forms": [{"name": "ID", "id": "DATE-ID-FORM"}],
                },
                {
                    "name": "Project",
                    "id": "PROJECT-ATTR",
                    "forms": [
                        {"name": "Name", "id": "PROJECT-NAME-FORM"},
                        {"name": "GUID", "id": "PROJECT-GUID-FORM"},
                    ],
                },
                {
                    "name": "Object",
                    "id": "OBJECT-ATTR",
                    "forms": [
                        {"name": "Name", "id": "OBJECT-NAME-FORM"},
                        {"name": "GUID", "id": "OBJECT-GUID-FORM"},
                    ],
                },
                {
                    "name": "User",
                    "id": "USER-ATTR",
                    "forms": [
                        {"name": "Name", "id": "USER-NAME-FORM"},
                        {"name": "Login", "id": "USER-LOGIN-FORM"},
                    ],
                },
            ],
            "metrics": [
                {"name": "Row Count - lu_project", "id": "ROWCOUNT-METRIC"},
                {"name": "Num Executions", "id": "EXECUTIONS-METRIC"},
            ],
        }
    }
}


def _instance_page(
    headers: List[List[int]],
    metric_values: List[List[float]],
    total: int,
) -> Dict[str, Any]:
    return {
        "instanceId": "instance-1",
        "definition": {
            "grid": {
                "rows": [
                    {
                        "name": "Date",
                        "elements": [
                            {"formValues": ["5/6/2026"]},
                            {"formValues": ["5/7/2026"]},
                        ],
                    },
                    {
                        "name": "Project",
                        "elements": [{"formValues": ["PROJECT-GUID-1"]}],
                    },
                    {
                        "name": "Object",
                        "elements": [
                            {"formValues": ["DASH-1", "Sales Dashboard"]},
                            {"formValues": ["REPORT-1", "Sales Report"]},
                        ],
                    },
                    {
                        "name": "User",
                        "elements": [
                            {"formValues": ["alice"]},
                            {"formValues": ["bob"]},
                        ],
                    },
                ]
            }
        },
        "data": {
            "paging": {"total": total},
            "headers": {"rows": headers},
            "metricValues": {"raw": metric_values},
        },
    }


def test_resolve_usage_query_plan_by_names() -> None:
    resolution = resolve_usage_query_plan(_CUBE_DEFINITION)
    plan = resolution.plan

    assert resolution.error is None
    assert plan is not None
    assert plan.date_form_id == "DATE-ID-FORM"
    assert plan.project_form_id == "PROJECT-GUID-FORM"
    assert plan.object_guid_form_id == "OBJECT-GUID-FORM"
    assert plan.object_name_form_id == "OBJECT-NAME-FORM"
    # Login is preferred over Name for the user identity.
    assert plan.user_form_id == "USER-LOGIN-FORM"
    assert plan.metric_id == "EXECUTIONS-METRIC"

    body = build_usage_query_body(plan, "2026-05-01")
    assert body["viewFilter"]["operands"][1]["value"] == "2026-05-01"
    requested = body["requestedObjects"]
    assert [a["id"] for a in requested["attributes"]] == [
        "DATE-ATTR",
        "PROJECT-ATTR",
        "OBJECT-ATTR",
        "USER-ATTR",
    ]


def test_resolve_usage_query_plan_reports_missing_objects() -> None:
    resolution = resolve_usage_query_plan(
        {"definition": {"availableObjects": {"attributes": [], "metrics": []}}}
    )

    assert resolution.plan is None
    assert resolution.error is not None
    assert "Date" in resolution.error


def test_parse_usage_page_decodes_header_indices() -> None:
    page = _instance_page(
        headers=[[0, 0, 0, 0], [0, 0, 1, 1], [1, 0, 0, 1]],
        metric_values=[[4.0], [2.0], [1.0]],
        total=3,
    )

    result = parse_usage_page(page)

    assert result.total == 3
    assert [
        (r.date_text, r.object_guid, r.object_name, r.user, r.count)
        for r in result.rows
    ] == [
        ("5/6/2026", "DASH-1", "Sales Dashboard", "alice", 4),
        ("5/6/2026", "REPORT-1", "Sales Report", "bob", 2),
        ("5/7/2026", "DASH-1", "Sales Dashboard", "bob", 1),
    ]


def test_parse_usage_date_ms_formats() -> None:
    # 2026-05-06 UTC midnight
    assert parse_usage_date_ms("5/6/2026") == 1778025600000
    assert parse_usage_date_ms("2026-05-06") == 1778025600000
    assert parse_usage_date_ms("not a date") is None
    assert parse_usage_date_ms("") is None


def _usage_config(extra: Dict[str, Any] | None = None) -> MicroStrategyConfig:
    config = {
        "base_url": "https://mstr.example.com/MicroStrategyLibrary",
        "extract_usage_statistics": True,
    }
    config.update(extra or {})
    return MicroStrategyConfig.model_validate(config)


class _FakeUsageClient:
    def __init__(self, pages: List[Dict[str, Any]]):
        self.pages = pages
        self.executed_bodies: List[Dict[str, Any]] = []
        self.page_offsets: List[int] = []

    def search_objects_by_name(
        self, project_id: str, object_type: int, name: str
    ) -> Iterator[Dict[str, Any]]:
        return iter(
            [
                {"id": "REPORT-DECOY", "name": name, "subtype": "768"},
                {"id": "CUBE-1", "name": name, "subtype": "779"},
            ]
        )

    def get_cube_definition(self, project_id: str, cube_id: str) -> Dict[str, Any]:
        assert cube_id == "CUBE-1"
        return _CUBE_DEFINITION

    def execute_cube_query(
        self,
        project_id: str,
        cube_id: str,
        body: Dict[str, Any],
        limit: int,
        offset: int = 0,
    ) -> Dict[str, Any]:
        self.executed_bodies.append(body)
        return self.pages[0]

    def get_cube_instance_page(
        self,
        project_id: str,
        cube_id: str,
        instance_id: str,
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        self.page_offsets.append(offset)
        page_index = offset // limit
        if page_index < len(self.pages):
            return self.pages[page_index]
        return _instance_page(headers=[], metric_values=[], total=0)


def test_extractor_aggregates_rows_into_daily_buckets() -> None:
    page = _instance_page(
        headers=[[0, 0, 0, 0], [0, 0, 0, 1], [1, 0, 1, 1]],
        metric_values=[[3.0], [2.0], [5.0]],
        total=3,
    )
    client = _FakeUsageClient([page])
    report = MicroStrategyReport()
    extractor = MicroStrategyUsageExtractor(
        client,  # type: ignore[arg-type]
        _usage_config(),
        report,
    )

    buckets = extractor.fetch_usage_buckets("pa-project")

    assert len(buckets) == 2
    dash = next(b for b in buckets if b.object_guid == "DASH-1")
    assert dash.view_count == 5
    assert dash.user_counts == {"alice": 3, "bob": 2}
    report_bucket = next(b for b in buckets if b.object_guid == "REPORT-1")
    assert report_bucket.view_count == 5
    assert report_bucket.user_counts == {"bob": 5}
    assert report.usage_rows_scanned == 3


def test_source_emits_usage_aspects_for_ingested_entities() -> None:
    page = _instance_page(
        headers=[[0, 0, 0, 0], [0, 0, 1, 1], [0, 0, 2, 0]],
        metric_values=[[4.0], [2.0], [9.0]],
        total=3,
    )
    # Third object row is outside this run's ingested entities.
    page["definition"]["grid"]["rows"][2]["elements"].append(
        {"formValues": ["OTHER-1", "Someone Elses Dashboard"]}
    )
    fake_client = _FakeUsageClient([page])
    source = MicroStrategySource(_usage_config(), PipelineContext(run_id="usage-test"))
    source.client = fake_client  # type: ignore[assignment]

    pa_project = Project.model_validate(
        {"id": "PA-PROJECT", "name": "Platform Analytics"}
    )
    source._register_usage_target(
        "dash-1",
        "dashboard",
        source.mapper.dashboard_urn("project-1", "DASH-1"),
    )
    source._register_usage_target(
        "report-1",
        "chart",
        source.mapper.report_urn("project-1", "REPORT-1"),
    )

    workunits = list(source._process_usage_statistics([pa_project]))

    aspects_by_type = {type(wu.metadata.aspect): wu for wu in workunits}  # type: ignore[union-attr]
    assert len(workunits) == 2
    dash_aspect = aspects_by_type[DashboardUsageStatisticsClass].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(dash_aspect, DashboardUsageStatisticsClass)
    assert dash_aspect.viewsCount == 4
    assert dash_aspect.uniqueUserCount == 1
    assert dash_aspect.userCounts is not None
    assert dash_aspect.userCounts[0].user == "urn:li:corpuser:alice"
    chart_aspect = aspects_by_type[ChartUsageStatisticsClass].metadata.aspect  # type: ignore[union-attr]
    assert isinstance(chart_aspect, ChartUsageStatisticsClass)
    assert chart_aspect.viewsCount == 2
    # The unmatched third object is counted, not emitted.
    assert source.report.usage_objects_unmatched == 1
    assert source.report.usage_buckets_emitted == 2


def test_source_warns_when_platform_analytics_project_missing() -> None:
    source = MicroStrategySource(_usage_config(), PipelineContext(run_id="usage-test"))
    other = Project.model_validate({"id": "p1", "name": "Sales Analytics"})

    workunits = list(source._process_usage_statistics([other]))

    assert workunits == []
    assert len(source.report.warnings) == 1


def test_usage_targets_not_tracked_when_disabled() -> None:
    source = MicroStrategySource(
        _usage_config({"extract_usage_statistics": False}),
        PipelineContext(run_id="usage-test"),
    )
    source._register_usage_target("dash-1", "dashboard", "urn:li:dashboard:x")
    assert source._usage_targets == {}


def test_usage_pagination_fetches_all_pages() -> None:
    page1 = _instance_page(headers=[[0, 0, 0, 0]], metric_values=[[1.0]], total=2)
    page2 = _instance_page(headers=[[1, 0, 1, 1]], metric_values=[[2.0]], total=2)
    client = _FakeUsageClient([page1, page2])
    report = MicroStrategyReport()
    config = _usage_config({"page_size": 1})
    extractor = MicroStrategyUsageExtractor(
        client,  # type: ignore[arg-type]
        config,
        report,
    )

    buckets = extractor.fetch_usage_buckets("pa-project")

    assert client.page_offsets == [1]
    assert report.usage_rows_scanned == 2
    assert {b.object_guid for b in buckets} == {"DASH-1", "REPORT-1"}


def test_usage_search_ignores_non_cube_matches() -> None:
    class NoCubeClient(_FakeUsageClient):
        def search_objects_by_name(
            self, project_id: str, object_type: int, name: str
        ) -> Iterator[Dict[str, Any]]:
            return iter([{"id": "REPORT-DECOY", "name": name, "subtype": "768"}])

    report = MicroStrategyReport()
    extractor = MicroStrategyUsageExtractor(
        NoCubeClient([]),  # type: ignore[arg-type]
        _usage_config(),
        report,
    )

    assert extractor.fetch_usage_buckets("pa-project") == []
    assert len(report.warnings) == 1
