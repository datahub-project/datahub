from typing import Dict, List, Type, Union

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.rest_passthrough import RestApiPassthrough
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
    # /projects/{id} is absent for a different reason: a {placeholder} matches any
    # single segment, so allowlisting it would also allow GET /projects/export.
    # The typed commands already return project metadata, so the entry buys
    # nothing and costs that.
    #
    # /projects is the bootstrap: the only route to a project id, which the two
    # id-addressed entries need and which no typed command returns (they return
    # the title a pattern is matched against).
    api_allowlist = (
        "GET /projects",
        "GET /projects/{id}/runs",
        "GET /projects/{id}/queriedTables",
        "GET /data-connections",
        "GET /users/me",
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
