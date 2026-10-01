import logging
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Optional, TypeVar

import tableauserverclient as TSC
from tableauserverclient import Server
from tableauserverclient.server.endpoint.exceptions import ServerResponseError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeSoftError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSourceReport,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Tableau error codes are six digits whose first three are the HTTP status.
_SOFT_STATUSES = ("403", "404")


@contextmanager
def _soft_on_tsc(context: str) -> Iterator[None]:
    """soft_on_status for TSC. It raises ServerResponseError with a string code
    such as "403069" and no .response, so the shared helper never fires on it."""
    try:
        yield
    except ServerResponseError as exc:
        if str(exc.code)[:3] in _SOFT_STATUSES:
            raise ProbeSoftError(
                f"{context} returned Tableau error {exc.code} ({exc.summary}); "
                f"treating it as empty."
            ) from exc
        raise


class TableauMetadataProbe:
    """Metadata-only probe over one Tableau site.

    Holds a TableauSiteSource, ingestion's per-site worker, and not a Source:
    its constructor opens nothing (the signed-in server is handed in) and emits
    no telemetry. The probe's fetches therefore go through _get_all_project and
    get_connection_objects, with the same paging, retry and re-authentication
    as a run.

    Deliberately no `api` or `sql` passthrough: the REST surfaces one would open
    include view data and crosstab exports (row values), workbook and data
    source downloads (custom SQL, Initial SQL, extracts) and the user and group
    directories (PII); the Metadata API is a POST and exposes raw custom SQL.
    """

    # Read back by run_probe_method after each command: soft 403/404 reads.
    warnings: List[str]

    def __init__(self, config: TableauConfig, server: Server) -> None:
        self._config = config
        self._report = TableauSourceReport()
        self.warnings = []
        # Its constructor makes one users.get_by_id call (report_user_role),
        # which records the "Insufficient Permissions" warning: the most useful
        # permission diagnosis the probe can give.
        self._site = TableauSiteSource(
            config=config,
            ctx=PipelineContext(run_id="tableau-probe"),
            site=SiteIdContentUrl(site_id=server.site_id, site_content_url=config.site),
            report=self._report,
            server=server,
            platform="tableau",
        )

    @classmethod
    def for_config(cls, config: TableauConfig) -> "TableauMetadataProbe":
        # make_tableau_client is ingestion's own sign-in: the retry adapter,
        # ssl_verify and session_trust_env all come with it. A Tableau session
        # cannot be opened lazily, so sign-in happens here.
        return cls(config, config.make_tableau_client(config.site))

    def __enter__(self) -> "TableauMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        # self._site.server, not the server we were given: _re_authenticate
        # replaces it during a long Metadata API call.
        try:
            self._site.server.auth.sign_out()
        except Exception as ex:
            logger.warning("Tableau probe sign-out failed (%s); continuing", ex)

    @property
    def probe_report(self) -> object:
        """The report the reused ingestion code writes into: "Incomplete
        project hierarchy" and "Insufficient Permissions" reach the caller."""
        return self._report

    def _listing(self, fetch: Callable[[], List[T]]) -> List[T]:
        try:
            return fetch()
        except ProbeSoftError as exc:
            self._warn(str(exc))
            return []

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _all_projects(self) -> Dict[str, TableauProject]:
        with _soft_on_tsc("projects listing"):
            return self._site._get_all_project()

    def _path(self, project: TableauProject) -> str:
        return self._config.project_path_separator.join(project.path)

    @probe_method()
    def site(self) -> Dict[str, object]:
        """The site this recipe signs in to, and whether the credential's site
        role is enough for a complete ingestion (Site Administrator Explorer or
        above). The user name is withheld."""
        name: Optional[str] = None
        content_url: Optional[str] = None
        state: Optional[str] = None
        try:
            with _soft_on_tsc("site details"):
                item = self._site.server.sites.get_by_id(self._site.server.site_id)
            name, content_url, state = item.name, item.content_url, item.state
        except ProbeSoftError as exc:
            self._warn(str(exc))
        users = self._report.logged_in_user
        user = users[-1] if users else None
        return {
            "name": name,
            "content_url": content_url,
            "state": state,
            "site_role": user.site_role if user else None,
            "site_administrator_explorer": (
                user.has_site_administrator_explorer_privileges() if user else None
            ),
            "api_version": self._site.server.version,
        }

    @probe_method(kind=BIContainerSubTypes.TABLEAU_SITE, row_limit_param="limit")
    def sites(self, limit: int = 200) -> List[Dict[str, object]]:
        """Sites this credential can see, including ones site_name_pattern
        would exclude. `name` is what site_name_pattern matches, `content_url`
        is what the recipe's `site` takes, and a site whose `state` is not
        Active is skipped whatever the pattern says. site_name_pattern applies
        only with ingest_multiple_sites (Tableau Server only). A 403, as on
        Tableau Cloud or below server administrator, degrades to [] with a
        warning."""

        def fetch() -> List[Dict[str, object]]:
            out: List[Dict[str, object]] = []
            with _soft_on_tsc("sites listing"):
                for item in TSC.Pager(self._site.server.sites):
                    out.append(
                        {
                            "name": item.name,
                            "content_url": item.content_url,
                            "state": item.state,
                        }
                    )
                    if len(out) >= limit:
                        break
            return out

        return self._listing(fetch)

    @probe_method(kind=BIContainerSubTypes.TABLEAU_PROJECT, row_limit_param="limit")
    def projects(self, limit: int = 200) -> List[str]:
        """Every project on this site as its full path: the project names joined
        by project_path_separator. That is exactly what project_path_pattern is
        matched on and what `workbooks` takes. Includes projects the recipe
        would exclude; judge them with `probe filter --kind Project --name
        <path>`. All projects are fetched whatever the limit, because a path
        needs every ancestor. A project whose parent this credential cannot see
        is reported at the root, as ingestion treats it, and the result says so
        in its warnings. Owners and descriptions are withheld."""
        return self._listing(
            lambda: sorted(self._path(p) for p in self._all_projects().values())[
                :limit
            ]
        )
