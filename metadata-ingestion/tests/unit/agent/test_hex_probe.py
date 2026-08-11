"""Hex's probe: titles for verdicts, categories alongside them, and an allowlist
whose absences are the point.
"""

from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.introspect import pattern_field_for_config
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    _enforce_gates,
    _iter_specs,
)
from datahub.ingestion.agent.verdicts import ProbeSoftError
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.hex.api import HexApiConnection, HexApiReport
from datahub.ingestion.source.hex.config import HexSourceConfig
from datahub.ingestion.source.hex.constants import HEX_CATEGORY_KIND
from datahub.ingestion.source.hex.hex_probe import HexMetadataProbe
from datahub.ingestion.source.hex.model import (
    Analytics,
    Category,
    Collection,
    Component,
    Owner,
    Project,
    RunRecord,
    Status,
)

# A real-shaped Hex id: UUID, which is why "export" is not a possible id.
PROJECT_ID = "c8f815c8-88c2-4dea-981f-69f544d6165d"


class _FakeApi:
    """Answers only what the getters call, and counts sweeps of /projects."""

    base_url = "https://app.hex.tech/api/v1"

    def __init__(self, enterprise: bool = True) -> None:
        self.session = type("_S", (), {"close": lambda self: None})()
        self.sweeps: List[bool] = []
        self.queried_ids: List[str] = []
        self.enterprise = enterprise
        self.report = HexApiReport()

    def _auth_header(self) -> Dict[str, str]:
        return {"Authorization": "Bearer tok"}

    def fetch_projects(
        self, include_components: bool = False
    ) -> Iterator[Union[Project, Component]]:
        self.sweeps.append(include_components)
        yield Project(
            id=PROJECT_ID,
            # A comma in a title: --name must carry it whole.
            title="Revenue, EMEA",
            description=None,
            status=Status(name="Published"),
            categories=[Category(name="Finance"), Category(name="Internal")],
        )
        yield Project(id="p2", title="Scratch", description=None, categories=None)
        if include_components:
            yield Component(
                id="c1",
                title="Header",
                description=None,
                categories=[Category(name="Design")],
            )

    def fetch_single_project(
        self, project_id: str
    ) -> Optional[Union[Project, Component]]:
        if project_id != PROJECT_ID:
            return None
        return Project(
            id=PROJECT_ID,
            title="Revenue, EMEA",
            description="Quarterly revenue",
            status=Status(name="Published"),
            categories=[Category(name="Finance")],
            collections=[Collection(name="Exec")],
            owner=Owner(email="owner@example.com"),
            creator=Owner(email="creator@example.com"),
            analytics=Analytics(
                appviews_all_time=42,
                appviews_last_7_days=1,
                appviews_last_14_days=2,
                appviews_last_30_days=3,
                last_viewed_at=None,
            ),
        )

    def fetch_queried_tables(self, hex_item_id: str) -> Optional[List[dict]]:
        self.queried_ids.append(hex_item_id)
        if self.enterprise:
            return [{"tableName": "ANALYTICS.PUBLIC.ORDERS"}, {"tableName": None}]
        # What the real one does on a non-Enterprise workspace: records why and
        # returns None rather than raising, so the caller can distinguish "no
        # tables" from "this tier cannot answer".
        self.report.warning(
            title="queriedTables unavailable on this workspace",
            message="The endpoint requires a Hex Enterprise workspace.",
        )
        return None

    def fetch_latest_run(self, project_id: str) -> Optional[RunRecord]:
        return RunRecord(
            run_id="r1",
            status="COMPLETED",
            start_time=datetime(2026, 1, 1),
            elapsed_seconds=12.5,
        )

    def fetch_workspace_id(self) -> Optional[str]:
        return "ws-123"

    def fetch_connections(self) -> Dict[str, HexApiConnection]:
        # The type the real fetch_connections returns -- NOT model.HexConnection,
        # which has a `platform` field this one does not. The first version of this
        # fake used the wrong one and the test passed anyway; mypy caught it.
        return {
            "dc1": HexApiConnection(
                name="Snowflake prod",
                type="snowflake",
                default_database="ANALYTICS",
                default_schema="PUBLIC",
            )
        }


def _config(**overrides: Any) -> HexSourceConfig:
    base: Dict[str, Any] = {"workspace_name": "ws", "token": "t"}
    base.update(overrides)
    return HexSourceConfig(**base)


def _probe_for(api: "_FakeApi", **config_overrides: Any) -> HexMetadataProbe:
    return HexMetadataProbe(api, _config(**config_overrides))  # type: ignore[arg-type]


def _probe() -> HexMetadataProbe:
    return _probe_for(_FakeApi())


def _spec(command: str) -> ProbeMethodSpec:
    spec = getattr(getattr(HexMetadataProbe, command), "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    return spec


def test_projects_are_listed_with_the_title_a_pattern_is_matched_against():
    # Hex filters on item.title (hex.py), so the reported name must be the title
    # -- not the id, which is what a raw API record would tempt a caller into.
    assert [p["name"] for p in _probe().projects()] == ["Revenue, EMEA", "Scratch"]


def test_a_denied_project_is_still_listed():
    # Reporting only what survives would make a workspace look emptier than it is,
    # and leave `probe filter` nothing to explain.
    titles = [p["name"] for p in _probe().projects()]
    assert "Scratch" in titles


def test_each_record_carries_its_categories():
    # category_pattern drops an item on its categories rather than its title, so a
    # title verdict alone is not the whole answer; the categories travel with it.
    records = {p["name"]: p["categories"] for p in _probe().projects()}
    assert records["Revenue, EMEA"] == ["Finance", "Internal"]
    assert records["Scratch"] == []


def test_listing_projects_does_not_also_fetch_components():
    # /projects is paged and rate limited, so components asked for and discarded
    # are real requests against a 57-per-minute budget.
    api = _FakeApi()
    _probe_for(api).projects()
    assert api.sweeps == [False]


def test_components_are_a_separate_listing():
    assert [c["name"] for c in _probe().components()] == ["Header"]


def test_categories_are_collected_from_the_items():
    # Hex has no category listing endpoint, so these come from the items -- and
    # must be deduplicated, since categories repeat across projects.
    assert _probe().categories() == ["Finance", "Internal", "Design"]


def test_connections_are_keyed_the_way_connection_platform_map_is():
    # An unmapped connection produces no lineage silently, so the probe reports the
    # ids that map must cover.
    assert _probe().connections() == {
        "dc1": {
            "name": "Snowflake prod",
            # Hex's own connection type: the value connection_platform_map keys on.
            "type": "snowflake",
            "default_database": "ANALYTICS",
            "default_schema": "PUBLIC",
        }
    }


@pytest.mark.parametrize(
    "path",
    [
        "/cells",
        f"/projects/{PROJECT_ID}/cells",
        "/projects/export",
        "/users",
        f"/projects/{PROJECT_ID}",
        f"/projects/{PROJECT_ID}/runs",
        f"/projects/{PROJECT_ID}/queriedTables",
    ],
)
def test_the_endpoints_carrying_cell_sql_or_pii_are_unreachable(path: str) -> None:
    """The allowlist's absences are the design.

    /cells and the export both carry SqlCell.sql_source -- raw cell SQL, so a WHERE
    literal is a row value by another route. /users is a directory. And
    /projects/{id} is absent because a {placeholder} matches any single segment, so
    listing it would also permit /projects/export.
    """
    with pytest.raises(ApiScopeError):
        _enforce_gates(_spec("api"), _probe(), {"path": path})


@pytest.mark.parametrize("path", ["/projects", "/data-connections"])
def test_the_listed_read_endpoints_are_reachable(path: str) -> None:
    # Only the two listings. Everything Hex addresses by id has a typed command,
    # so no {placeholder} appears in the allowlist at all -- which is what keeps
    # /projects/export out of it (a placeholder matches any single segment).
    _enforce_gates(_spec("api"), _probe(), {"path": path})


def test_the_passthrough_uses_hexs_own_auth_and_session():
    # Not a bare requests call: HexApi installs a 57-per-minute limiter by patching
    # session.request, and a direct request would escape it.
    api = _FakeApi()
    probe = _probe_for(api)
    assert probe.api_session is api.session
    assert probe.api_base_url == api.base_url
    assert probe.api_headers() == {"Authorization": "Bearer tok"}


def test_every_command_declares_the_kind_it_returns_except_the_passthroughs():
    kinds = {command: spec.kind for command, spec in _iter_specs(HexMetadataProbe)}
    assert kinds["projects"] == BIAssetSubTypes.HEX_PROJECT
    assert kinds["components"] == BIAssetSubTypes.HEX_COMPONENT
    assert kinds["categories"] == HEX_CATEGORY_KIND
    # `api` returns whatever the endpoint returns, and `connections` is a map, not
    # a listing of one kind of name.
    assert kinds["api"] is None
    assert kinds["connections"] is None


def test_the_config_declares_which_pattern_governs_which_kind():
    # Hex names its fields after Hex's vocabulary, so nothing derivable from the
    # subtype finds them: without the Filters declarations `probe filter` cannot
    # resolve a pattern field for any Hex kind at all.
    config = HexSourceConfig(workspace_name="ws", token="t")  # type: ignore[arg-type]
    assert pattern_field_for_config(config, "Project") == "project_title_pattern"
    assert pattern_field_for_config(config, "Component") == "component_title_pattern"
    assert pattern_field_for_config(config, HEX_CATEGORY_KIND) == "category_pattern"


def test_verdicts_are_judged_on_the_bare_title_as_ingestion_judges_them():
    recipe: Dict[str, Any] = {
        "workspace_name": "ws",
        "token": "t",
        "project_title_pattern": {"allow": ["^Revenue.*"]},
    }
    result = check_filters(
        source_type="hex",
        config_dict=recipe,
        kind="Project",
        parent_path=[],
        names=["Revenue, EMEA", "Scratch"],
    )
    assert result.pattern_field == "project_title_pattern"
    kept = {v.name: v.included for v in result.results}
    assert kept == {"Revenue, EMEA": True, "Scratch": False}
    # Hex matches the title itself, so the target is the bare name -- no container
    # to qualify it with, unlike the SQL family.
    assert [v.target for v in result.results] == ["Revenue, EMEA", "Scratch"]


def test_the_provider_closes_the_session_it_opened():
    api = _FakeApi()
    closed: List[bool] = []
    api.session.close = lambda: closed.append(True)  # type: ignore[method-assign]
    with _probe_for(api):  # type: ignore[arg-type]
        pass
    assert closed == [True]


def test_queried_tables_delegates_to_hexs_own_fetcher():
    # Reaching /queriedTables as a raw path would work and would lose the tier
    # handling below, which is the reason this command exists at all.
    api = _FakeApi(enterprise=True)
    probe = _probe_for(api)
    assert probe.queried_tables("Revenue, EMEA") == ["ANALYTICS.PUBLIC.ORDERS"]
    # Addressed by title: the id was resolved here, so no caller ever holds one.
    assert api.queried_ids == [PROJECT_ID]


def test_a_non_enterprise_workspace_reports_why_rather_than_looking_empty():
    # fetch_queried_tables returns None on 403 and records that the endpoint needs
    # Enterprise. Raw, that same call is an HTTPError with no explanation; here the
    # empty result arrives with the connector's own reason attached.
    api = _FakeApi(enterprise=False)
    probe = _probe_for(api)
    assert probe.queried_tables("Revenue, EMEA") == []
    assert any("Enterprise" in w for w in probe.warnings)


def test_an_unknown_title_degrades_with_a_warning_not_a_false_empty():
    probe = _probe()
    with pytest.raises(ProbeSoftError, match="NoSuchProject"):
        probe.queried_tables("NoSuchProject")


def test_latest_run_reports_what_include_run_history_would_ingest():
    assert _probe().latest_run("Revenue, EMEA") == {
        "run_id": "r1",
        "status": "COMPLETED",
        "start_time": "2026-01-01 00:00:00",
        "elapsed_seconds": 12.5,
    }


def test_workspace_reports_the_id_every_urn_is_scoped_by():
    assert _probe().workspace() == {"workspace_id": "ws-123"}


def test_the_probe_surfaces_the_connectors_own_warnings():
    # run_probe_method reads `warnings` back after each command, so anything HexApi
    # recorded while serving it reaches the caller.
    api = _FakeApi(enterprise=True)
    probe = _probe_for(api)
    assert probe.warnings == []
    api.report.warning(title="Something degraded", message="and here is why")
    assert probe.warnings == ["Something degraded: and here is why"]


def test_project_detail_reports_what_ingestion_would_emit():
    detail = _probe().project("Revenue, EMEA")
    assert detail["name"] == "Revenue, EMEA"
    assert detail["description"] == "Quarterly revenue"
    # Counts are metadata about the asset, unlike its contents.
    assert detail["appviews_all_time"] == 42


def test_project_tags_follow_the_flags_that_govern_them():
    # An empty `tags` must mean "the flags are off", not "the project is untagged" --
    # otherwise the probe answers a question ingestion never asked.
    all_on = _probe_for(
        _FakeApi(),
        categories_as_tags=True,
        collections_as_tags=True,
        status_as_tag=True,
    ).project("Revenue, EMEA")
    assert all_on["tags"] == ["Finance", "Exec", "Published"]

    all_off = _probe_for(
        _FakeApi(),
        categories_as_tags=False,
        collections_as_tags=False,
        status_as_tag=False,
    ).project("Revenue, EMEA")
    assert all_off["tags"] == []


def test_owners_are_reported_only_when_the_recipe_would_set_them():
    on = _probe_for(_FakeApi(), set_ownership_from_email=True).project("Revenue, EMEA")
    assert on["owners"] == ["owner@example.com", "creator@example.com"]

    off = _probe_for(_FakeApi(), set_ownership_from_email=False).project(
        "Revenue, EMEA"
    )
    assert "owners" not in off


def test_project_detail_needs_no_allowlist_entry():
    # The whole reason this command exists rather than a raw /projects/{id} path: it
    # resolves the id from the title, so no placeholder enters the allowlist -- and a
    # placeholder is what would have re-admitted /projects/export.
    assert HexMetadataProbe.api_allowlist == ("GET /projects", "GET /data-connections")
    with pytest.raises(ApiScopeError):
        _enforce_gates(_spec("api"), _probe(), {"path": f"/projects/{PROJECT_ID}"})
