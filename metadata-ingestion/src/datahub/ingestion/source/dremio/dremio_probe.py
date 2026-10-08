from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import requests

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    PersonalWithholding,
    ProbeProviderBase,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import ProbeReadFailed
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_models import DremioEntityContainerType
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.dremio.dremio_selection import (
    DatasetFacts,
    container_verdict,
    dataset_verdict,
    folder_verdict,
    sql_schema_filter_value,
)

# A home space is one user's, named "@<username>"; Dremio reserves the "@".
_HOME_PREFIX = "@"
# A listing query waits this long for Dremio's job, not ingestion's hour.
_QUERY_TIMEOUT_SECONDS = 300


@dataclass(frozen=True)
class _Root:
    name: str
    root_id: Optional[str]
    container_type: str

    @property
    def is_home(self) -> bool:
        return (
            self.container_type == DremioEntityContainerType.HOME.value
            or self.name.startswith(_HOME_PREFIX)
        )


@dataclass
class _WalkCount:
    not_found: int = 0


@dataclass(frozen=True)
class _Dataset:
    schema: str
    table: str
    has_columns: bool

    @property
    def path(self) -> List[str]:
        return self.schema.split(".") if self.schema else []


def _count(value: object) -> int:
    # The job results API sends a COUNT as a JSON number; anything else reads
    # as no columns, which ingestion's join would also find.
    return value if isinstance(value, int) else 0


class DremioMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over Dremio's REST API, through the connector's own
    DremioAPIOperations: its Cloud or Software base URL, PAT or password login,
    TLS settings and retrying session.

    Containers come from the catalog walk ingestion makes (/catalog, then
    /catalog/{id} down every folder); datasets from INFORMATION_SCHEMA, the
    catalog ingestion's dataset query reads on Community edition. Every name
    is the object's full dotted path, which is what ingestion's filters read.
    """

    # dremio_api logs request URLs, SQL and response bodies, which carry no
    # credential shape for the log guard to catch.
    silenced_loggers = ("datahub.ingestion.source.dremio.dremio_api",)

    def __init__(self, config: DremioSourceConfig) -> None:
        self._config = config
        self._report = DremioSourceReport()

    @classmethod
    def for_config(cls, config: DremioSourceConfig) -> "DremioMetadataProbe":
        return cls(config)

    @property
    def probe_report(self) -> object:
        """The connector's report: a failed listing query records its failure
        there before it raises."""
        return self._report

    def _api(self) -> DremioAPIOperations:
        # Logs in (password) and reads the edition, so it is built in a
        # command, where a bad credential or host belongs.
        return self._open_once(
            "api",
            lambda: DremioAPIOperations(self._config, self._report),
            close=lambda api: api.session.close(),
        )

    def _get_json(self, path: str) -> Dict[str, object]:
        """GET on the connector's authenticated session, failing on an HTTP
        error. DremioAPIOperations.get returns an error body as if it were the
        answer, which would make a refused catalog read look empty."""
        api = self._api()
        response = api.session.get(
            f"{api.base_url}{path}", verify=api._verify, timeout=api._timeout
        )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise ProbeReadFailed(f"Dremio answered {path} with a non-object body")
        return body

    def _roots(self) -> List[_Root]:
        """The top-level catalog entries ingestion walks: sources, spaces and
        home spaces, by the name its filters read."""
        roots: List[_Root] = []
        data = self._get_json("/catalog").get("data")
        for entry in data if isinstance(data, list) else []:
            if not isinstance(entry, dict):
                continue
            container_type = entry.get("containerType")
            if container_type not in (
                DremioEntityContainerType.SOURCE.value,
                DremioEntityContainerType.SPACE.value,
                DremioEntityContainerType.HOME.value,
            ):
                continue
            path = entry.get("path")
            name = path[0] if isinstance(path, list) and path else entry.get("name")
            if not isinstance(name, str) or not name:
                continue
            root_id = entry.get("id")
            roots.append(
                _Root(
                    name=name,
                    root_id=root_id if isinstance(root_id, str) else None,
                    container_type=str(container_type),
                )
            )
        return roots

    def _root_withholding(self) -> PersonalWithholding[_Root]:
        return PersonalWithholding[_Root](
            is_personal=lambda root: root.is_home,
            would_ingest=lambda root: (
                container_verdict(self._config, [root.name]).included
            ),
        )

    def _note_withheld(self, withheld: int, count_text: str, what: str) -> None:
        if withheld:
            self._warn(
                f"{count_text} {what} seen and not listed: a home "
                f"space is named after its user, and ingestion does not emit "
                f"these under this recipe"
            )

    def _root_listing(
        self, container_types: Tuple[str, ...], limit: int
    ) -> List[Dict[str, object]]:
        withholding = self._root_withholding()
        kept = take(
            (r for r in self._roots() if r.container_type in container_types),
            limit,
            keep=withholding.keep,
        )
        self._note_withheld(
            withholding.withheld,
            withholding.count_text(stopped_early=len(kept) >= limit),
            "home space(s)",
        )
        return [{"name": r.name, "home": r.is_home} for r in kept]

    @probe_method(kind=DatasetContainerSubTypes.DREMIO_SOURCE, row_limit_param="limit")
    def sources(self, limit: int = 200) -> List[Dict[str, object]]:
        """Dremio sources (external connections), by name, including ones
        schema_pattern would exclude. Judge them with `probe filter --kind
        "Dremio Source"`, which applies ingestion's root rule: a source also
        passes as the first segment of a dotted schema_pattern allow entry.
        Metadata only."""
        return self._root_listing((DremioEntityContainerType.SOURCE.value,), limit)

    @probe_method(kind=DatasetContainerSubTypes.DREMIO_SPACE, row_limit_param="limit")
    def spaces(self, limit: int = 200) -> List[Dict[str, object]]:
        """Dremio spaces, by name, including ones schema_pattern would exclude.
        Home spaces ("@<user>") are ingested as spaces too and are listed with
        home=true when the recipe ingests them; one it does not is counted in
        a warning, not listed. Metadata only."""
        return self._root_listing(
            (
                DremioEntityContainerType.SPACE.value,
                DremioEntityContainerType.HOME.value,
            ),
            limit,
        )

    def _catalog_entry(self, location_id: str) -> Optional[Dict[str, object]]:
        """One catalog entry, or None when Dremio answers 404 for it."""
        try:
            return self._get_json(f"/catalog/{location_id}")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def _walk(self, root: _Root, walk: _WalkCount) -> Iterator[Sequence[str]]:
        """Each folder's path under one root, as ingestion's catalog walk
        (get_containers_for_location) finds them: depth first, into every
        child container, whether or not its parent folder passed."""
        stack: List[Tuple[str, Sequence[str]]] = [
            (root.root_id or root.name, [root.name])
        ]
        while stack:
            location_id, path = stack.pop()
            body: Optional[Dict[str, object]] = {}
            with soft_listing(
                self._warn, 403, context=f"catalog entry '{'.'.join(path)}'"
            ):
                body = self._catalog_entry(location_id)
            if body is None:
                # A file source lists its unpromoted directories as children
                # but answers 404 when one is looked up. Ingestion's walk gets
                # the same 404 and emits nothing there, so nothing is missing.
                walk.not_found += 1
                continue
            if body.get("entityType") == DremioEntityContainerType.FOLDER.lower():
                yield path
            children = body.get("children")
            for child in reversed(children if isinstance(children, list) else []):
                if not isinstance(child, dict):
                    continue
                child_path = child.get("path")
                child_id = child.get("id")
                if (
                    child.get("type") == DremioEntityContainerType.CONTAINER
                    and isinstance(child_id, str)
                    and isinstance(child_path, list)
                ):
                    stack.append((child_id, [str(p) for p in child_path]))

    @probe_method(kind=DatasetContainerSubTypes.DREMIO_FOLDER, row_limit_param="limit")
    def folders(
        self, container: Optional[str] = None, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Folders, each named by its full dotted path ("space.folder.sub") --
        what schema_pattern is matched against -- under one source or space
        (by name), or under every one when none is given, including folders
        schema_pattern would exclude and folders under an excluded root.
        `root` is the source or space it sits in: ingestion walks only roots
        that pass, so `probe filter` judges a folder by its root too. Folders
        in a home space the recipe does not ingest are counted, not listed.
        Metadata only."""
        withholding = self._root_withholding()
        roots = [r for r in self._roots() if withholding.keep(r)]
        if container is not None:
            roots = [
                resolve_name(
                    container,
                    roots,
                    key=lambda r: r.name,
                    kind="source or space",
                    list_command="probe run sources` or `probe run spaces",
                ).record
            ]

        walk = _WalkCount()

        def folders_of() -> Iterator[Tuple[_Root, Sequence[str]]]:
            for root in roots:
                for path in self._walk(root, walk):
                    yield root, path

        folder_withholding = PersonalWithholding[Tuple[_Root, Sequence[str]]](
            is_personal=lambda found: found[0].is_home,
            would_ingest=lambda found: folder_verdict(self._config, found[1]).included,
        )
        kept = take(folders_of(), limit, keep=folder_withholding.keep)
        stopped_early = len(kept) >= limit
        self._note_withheld(
            withholding.withheld,
            withholding.count_text(stopped_early=stopped_early),
            "home space(s)",
        )
        self._note_withheld(
            folder_withholding.withheld,
            folder_withholding.count_text(stopped_early=stopped_early),
            "folder(s) in home spaces",
        )
        if walk.not_found:
            self._warn(
                f"{walk.not_found} catalog container(s) answered 404 when looked "
                f"up and are not listed; a file source lists its unpromoted "
                f"directories this way, and ingestion skips them too"
            )
        return [
            {"name": ".".join(path), "root": root.name, "folder": path[-1]}
            for root, path in kept
        ]

    def _datasets(self, table_type: str, limit: int) -> List[Dict[str, object]]:
        api = self._api()
        edition = api.edition.value
        # `table_type` and `limit` are this module's constant and the
        # framework's clamped int, never caller text. System tables are left
        # out as ingestion's dataset query leaves them out, and the column
        # count is that query's COLUMNS join, as a fact rather than a filter.
        rows = api.execute_query(
            "SELECT T.TABLE_SCHEMA, T.TABLE_NAME, "
            "COUNT(C.COLUMN_NAME) AS COLUMN_COUNT "
            'FROM INFORMATION_SCHEMA."TABLES" T '
            "LEFT JOIN INFORMATION_SCHEMA.COLUMNS C "
            "ON C.TABLE_CATALOG = T.TABLE_CATALOG "
            "AND C.TABLE_SCHEMA = T.TABLE_SCHEMA "
            "AND C.TABLE_NAME = T.TABLE_NAME "
            f"WHERE T.TABLE_TYPE = '{table_type}' "
            "GROUP BY T.TABLE_SCHEMA, T.TABLE_NAME "
            "ORDER BY T.TABLE_SCHEMA, T.TABLE_NAME "
            f"LIMIT {int(limit)}",
            timeout=_QUERY_TIMEOUT_SECONDS,
        )
        datasets = [
            _Dataset(
                schema=str(row.get("TABLE_SCHEMA") or ""),
                table=str(name),
                has_columns=_count(row.get("COLUMN_COUNT")) > 0,
            )
            for row in rows
            if (name := row.get("TABLE_NAME"))
        ]
        withholding = PersonalWithholding[_Dataset](
            is_personal=lambda d: d.schema.startswith(_HOME_PREFIX),
            would_ingest=lambda d: (
                dataset_verdict(
                    self._config,
                    DatasetFacts(
                        path=d.path,
                        name=d.table,
                        schema_filter_value=sql_schema_filter_value(
                            edition, d.schema, d.table
                        ),
                        has_columns=d.has_columns,
                    ),
                ).included
            ),
        )
        kept = [d for d in datasets if withholding.keep(d)]
        self._note_withheld(
            withholding.withheld,
            withholding.count_text(stopped_early=len(rows) >= limit),
            "dataset(s) in home spaces",
        )
        return [
            {
                "name": f"{d.schema}.{d.table}" if d.schema else d.table,
                "schema": d.schema,
                "edition": edition,
                "has_columns": d.has_columns,
            }
            for d in kept
        ]

    @probe_method(kind=DatasetSubTypes.TABLE, row_limit_param="limit")
    def tables(self, limit: int = 200) -> List[Dict[str, object]]:
        """Tables (physical datasets), each named by its full dotted path
        ("source.folder.table"), including ones schema_pattern or
        dataset_pattern would exclude. Judge a saved run with `probe filter
        --kind Table --from-run`: `edition` matters, since ingestion's dataset
        query matches schema_pattern anywhere in the upper-cased schema on
        Community, and in the whole path, the dataset's own name included, on
        Enterprise and Cloud. Read from INFORMATION_SCHEMA."TABLES" with one
        SQL job; ingestion on Enterprise and Cloud lists from SYS views, which
        name the same datasets. `has_columns` is false for a dataset Dremio
        holds no column metadata for (a source table nothing has queried
        yet), which ingestion's query skips. Tables in a home space the recipe
        does not ingest are counted, not listed. Metadata only."""
        return self._datasets("TABLE", limit)

    @probe_method(kind=DatasetSubTypes.VIEW, row_limit_param="limit")
    def views(self, limit: int = 200) -> List[Dict[str, object]]:
        """Views (virtual datasets), named and judged as `tables` are. Names
        only: view SQL is not read. Metadata only."""
        return self._datasets("VIEW", limit)
