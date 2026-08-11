from typing import Dict, List, Type, Union

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.rest_passthrough import RestApiPassthrough
from datahub.ingestion.agent.verdicts import ProbeSoftError
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.hex.api import HexApi, HexApiReport
from datahub.ingestion.source.hex.config import HexSourceConfig
from datahub.ingestion.source.hex.constants import HEX_CATEGORY_KIND
from datahub.ingestion.source.hex.model import Component, Project


class HexMetadataProbe(RestApiPassthrough):
    """Metadata-only probe over Hex's REST API.

    Reads through HexApi rather than a bare session so a probe call is rate
    limited and retried as ingestion is -- HexApi patches session.request with a
    57-per-minute limiter in its constructor, and a direct request would escape
    it and risk 429s for the real run.
    """

    # Read endpoints `probe api` may reach. Everything else is refused, so the
    # notable content here is what is deliberately absent:
    #
    #   /cells, /projects/{id}/cells -- SqlCell.sql_source is the raw SQL of a
    #       notebook cell, so a WHERE literal is a row value arriving by another
    #       route. Same rule sql_gate applies to pg_stat_statements.
    #   /projects/export             -- returns the project YAML, which embeds
    #       that same cell SQL. It is also a POST, which the gate refuses anyway.
    #   /users                       -- a user directory is PII, not metadata.
    #
    # No id-addressed entry at all. Everything Hex addresses by project id --
    # detail, runs, queriedTables -- has a typed command below that takes the
    # project TITLE and resolves the id itself, so no id needs to leave the probe
    # and no {placeholder} needs to appear here. That matters twice over: a
    # placeholder matches any single segment, so "GET /projects/{id}" would also
    # permit "GET /projects/export", and a raw path bypasses what api.py's
    # fetchers add (see queried_tables below).
    #
    # What remains is the two listings, kept because a raw record carries fields a
    # typed command projects away -- the escape hatch for a question no command
    # anticipated.
    api_allowlist = (
        "GET /projects",
        "GET /data-connections",
    )

    def __init__(self, api: HexApi) -> None:
        self._api = api
        self.api_session = api.session
        self.api_base_url = api.base_url

    @classmethod
    def for_config(cls, config: HexSourceConfig) -> "HexMetadataProbe":
        # Mirrors what HexSource.test_connection already builds, and safe for the
        # same reason: HexApi.__init__ opens no connection and emits no telemetry,
        # so this needs none of the uninitialized-instance care the SQLAlchemy
        # family does.
        return cls(
            HexApi(
                report=HexApiReport(),
                token=config.token.get_secret_value(),
                base_url=config.base_url,
                page_size=config.page_size,
            )
        )

    def __enter__(self) -> "HexMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        self._api.session.close()

    def api_headers(self) -> Dict[str, str]:
        # HexApi's own header builder rather than restating "Bearer {token}":
        # if the connector's auth scheme changes, a probe request changes with it.
        return self._api._auth_header()

    @property
    def warnings(self) -> List[str]:
        """What HexApi recorded while serving this command.

        run_probe_method reads this back, so a command that came back empty
        carries the connector's own reason for it rather than looking like an
        empty workspace -- fetch_queried_tables returning None because the
        workspace is not Enterprise is the case that matters.
        """
        return [
            f"{entry.title}: {entry.message}" if entry.message else str(entry.title)
            for entry in self._api.report.warnings
        ]

    @probe_method(kind=BIAssetSubTypes.HEX_PROJECT, row_limit_param="limit")
    def projects(self, limit: int = 200) -> List[Dict[str, object]]:
        """Projects in this workspace, including ones project_title_pattern would
        exclude -- a denied project is reported, not hidden, so `probe filter` can
        explain it. Each record also carries its categories, because
        category_pattern drops an item on those rather than on its title: judge
        them separately with `probe filter --kind HexCategory`. Metadata only --
        titles and status, never cell contents or query output."""
        return self._items(Project, limit, include_components=False)

    @probe_method(kind=BIAssetSubTypes.HEX_COMPONENT, row_limit_param="limit")
    def components(self, limit: int = 200) -> List[Dict[str, object]]:
        """Reusable components in this workspace, judged by
        component_title_pattern. Same record shape as `projects`."""
        return self._items(Component, limit, include_components=True)

    @probe_method(kind=HEX_CATEGORY_KIND, row_limit_param="limit")
    def categories(self, limit: int = 200) -> List[str]:
        """Every category in use across projects and components, so
        category_pattern can be judged directly. Hex exposes no category listing
        endpoint, so these are collected from the items themselves."""
        seen: List[str] = []
        for item in self._api.fetch_projects(include_components=True):
            for category in item.categories or []:
                if category.name not in seen:
                    seen.append(category.name)
                    if len(seen) >= limit:
                        return seen
        return seen

    @probe_method()
    def connections(self) -> Dict[str, object]:
        """Warehouse data connections this workspace can query, keyed by the id
        connection_platform_map is keyed on -- so an unmapped connection, which
        would silently produce no lineage, is visible before a run. `type` is
        Hex's own connection type, the value that map translates to a DataHub
        platform; the defaults are what unqualified `FROM table` refs in a SQL
        cell resolve against."""
        return {
            connection_id: {
                "name": conn.name,
                "type": conn.type,
                "default_database": conn.default_database,
                "default_schema": conn.default_schema,
            }
            for connection_id, conn in self._api.fetch_connections().items()
        }

    @probe_method(row_limit_param="limit")
    def queried_tables(self, project: str, limit: int = 200) -> List[str]:
        """Warehouse tables Hex itself resolved for one project, by project title
        -- the lineage question, answered without parsing any SQL. Empty with a
        warning when the workspace is not on Hex's Enterprise tier, which is the
        tier this endpoint needs; ingestion then falls back to parsing cell SQL,
        so an empty result here does not mean lineage will be absent."""
        rows = self._api.fetch_queried_tables(self._project_id_or_raise(project))
        return [
            str(row.get("tableName")) for row in (rows or []) if row.get("tableName")
        ][:limit]

    @probe_method()
    def latest_run(self, project: str) -> Dict[str, object]:
        """The most recent run of one project, by project title: status, start
        time and elapsed seconds. What include_run_history would ingest."""
        run = self._api.fetch_latest_run(self._project_id_or_raise(project))
        if run is None:
            return {}
        return {
            "run_id": run.run_id,
            "status": run.status,
            "start_time": str(run.start_time),
            "elapsed_seconds": run.elapsed_seconds,
        }

    @probe_method()
    def workspace(self) -> Dict[str, object]:
        """The workspace id this token resolves to -- what ingestion looks up
        first, and what every emitted URN is scoped by."""
        return {"workspace_id": self._api.fetch_workspace_id()}

    def _project_id_or_raise(self, title: str) -> str:
        """Resolve a project title to the id Hex addresses it by.

        The probe addresses objects by the title a pattern is matched against, so
        a caller never needs an id -- which is also why no id-addressed path is in
        the allowlist. Costs one /projects sweep, the same inherent cost Mode pays
        resolving a space name to a token.
        """
        for item in self._api.fetch_projects(include_components=True):
            if item.title == title:
                return item.id
        raise ProbeSoftError(
            f"no project or component titled '{title}' found in this workspace"
        )

    def _items(
        self,
        item_type: Type[Union[Project, Component]],
        limit: int,
        include_components: bool,
    ) -> List[Dict[str, object]]:
        # Stops at the limit rather than paging the whole workspace and slicing:
        # /projects is paged and rate limited, so the discarded pages are real
        # requests. include_components is passed through for the same reason --
        # asking for components while listing projects doubles the payload.
        out: List[Dict[str, object]] = []
        for item in self._api.fetch_projects(include_components=include_components):
            if not isinstance(item, item_type):
                continue
            out.append(
                {
                    "name": item.title,
                    "categories": [c.name for c in item.categories or []],
                    "status": item.status.name if item.status else None,
                }
            )
            if len(out) >= limit:
                break
        return out
