"""Hex's probe: titles for verdicts, categories alongside them, and an allowlist
whose absences are the point.
"""

from typing import Any, Dict, Iterator, List, Union

import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.introspect import pattern_field_for_config
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    _enforce_gates,
    _iter_specs,
)
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.hex.api import HexApiConnection
from datahub.ingestion.source.hex.config import HexSourceConfig
from datahub.ingestion.source.hex.constants import HEX_CATEGORY_KIND
from datahub.ingestion.source.hex.hex_probe import HexMetadataProbe
from datahub.ingestion.source.hex.model import Category, Component, Project, Status

# A real-shaped Hex id: UUID, which is why "export" is not a possible id.
PROJECT_ID = "c8f815c8-88c2-4dea-981f-69f544d6165d"


class _FakeApi:
    """Answers only what the getters call, and counts sweeps of /projects."""

    base_url = "https://app.hex.tech/api/v1"

    def __init__(self) -> None:
        self.session = type("_S", (), {"close": lambda self: None})()
        self.sweeps: List[bool] = []

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


def _probe() -> HexMetadataProbe:
    return HexMetadataProbe(_FakeApi())  # type: ignore[arg-type]


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
    HexMetadataProbe(api).projects()  # type: ignore[arg-type]
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


@pytest.mark.parametrize(
    "path",
    [
        "/projects",
        f"/projects/{PROJECT_ID}/runs",
        f"/projects/{PROJECT_ID}/queriedTables",
        "/data-connections",
    ],
)
def test_the_listed_read_endpoints_are_reachable(path: str) -> None:
    _enforce_gates(_spec("api"), _probe(), {"path": path})


def test_the_passthrough_uses_hexs_own_auth_and_session():
    # Not a bare requests call: HexApi installs a 57-per-minute limiter by patching
    # session.request, and a direct request would escape it.
    api = _FakeApi()
    probe = HexMetadataProbe(api)  # type: ignore[arg-type]
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
    with HexMetadataProbe(api):  # type: ignore[arg-type]
        pass
    assert closed == [True]
