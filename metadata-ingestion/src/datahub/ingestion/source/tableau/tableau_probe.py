import logging
import re
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional

import tableauserverclient as TSC
from tableauserverclient import Server
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    ServerResponseError,
    TableauError,
)

from datahub.ingestion.agent.error_policy import http_status_code
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    echoed,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeSoftError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauProject,
    TableauSiteSource,
    TableauSourceReport,
    parse_database_server_hostname,
)
from datahub.ingestion.source.tableau.tableau_common import (
    database_servers_graphql_query,
)

logger = logging.getLogger(__name__)

# A Tableau REST error code: six digits, the HTTP status then Tableau's own
# sub-code (403069). Checked for that shape, since the server sends it.
_TSC_CODE = re.compile(r"[0-9]{6}")
_SOFT_STATUSES = ("403", "404")


def _tsc_code(exc: BaseException) -> Optional[str]:
    """The code TSC puts on `.code` of a REST error (ServerResponseError, and
    FailedSignInError at sign-in), when it has the documented shape. Read off
    TableauError, the base every supported TSC version has."""
    code = getattr(exc, "code", None) if isinstance(exc, TableauError) else None
    return code if isinstance(code, str) and _TSC_CODE.fullmatch(code) else None


@contextmanager
def _tsc_soft_statuses(context: str) -> Iterator[None]:
    """A 403 or 404 from TSC as ProbeSoftError, for soft_listing to degrade.
    TSC raises ServerResponseError with the status inside a string code and no
    `.response`, the attribute soft_listing's own HTTP mapping reads."""
    try:
        yield
    except ServerResponseError as exc:
        code = _tsc_code(exc)
        if code is not None and code[:3] in _SOFT_STATUSES:
            # The code only: summary and detail are the server's text.
            raise ProbeSoftError(
                f"{context} returned Tableau error {code}; treating it as empty."
            ) from exc
        raise


class TableauMetadataProbe(ProbeProviderBase):
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

    def __init__(self, config: TableauConfig, server: Server) -> None:
        self._config = config
        self._report = TableauSourceReport()
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

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """TSC's code for a REST error ("Tableau 403069"), or the HTTP status
        of a 5xx ("HTTP 500"). TSC keeps both on `.code`, which the generic
        readers do not read; elsewhere `.code` is anything."""
        if isinstance(exc, InternalServerError):
            return http_status_code(exc.code)
        code = _tsc_code(exc)
        return f"Tableau {code}" if code else None

    def __exit__(self, *exc: object) -> None:
        # self._site.server, not the server we were given: _re_authenticate
        # replaces it during a long Metadata API call.
        try:
            self._site.server.auth.sign_out()
        except Exception as ex:
            logger.warning(
                "Tableau probe sign-out failed (%s); continuing", type(ex).__name__
            )
        super().__exit__(*exc)

    @property
    def probe_report(self) -> object:
        """The report the reused ingestion code writes into: "Incomplete
        project hierarchy" and "Insufficient Permissions" reach the caller."""
        return self._report

    def _all_projects(self) -> Dict[str, TableauProject]:
        with _tsc_soft_statuses("projects listing"):
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
        with soft_listing(self._warn), _tsc_soft_statuses("site details"):
            item = self._site.server.sites.get_by_id(self._site.server.site_id)
            name, content_url, state = item.name, item.content_url, item.state
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
        with soft_listing(self._warn), _tsc_soft_statuses("sites listing"):
            return take(
                (
                    {"name": i.name, "content_url": i.content_url, "state": i.state}
                    for i in TSC.Pager(self._site.server.sites)
                ),
                limit,
            )
        return []

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
        with soft_listing(self._warn):
            return sorted(self._path(p) for p in self._all_projects().values())[:limit]
        return []

    # Tableau REST filter expressions are "field:op:value" joined by commas, so
    # a value containing either delimiter cannot be sent as a filter.
    _FILTER_DELIMITERS = (",", ":")

    @probe_method(
        kind=BIContainerSubTypes.TABLEAU_WORKBOOK,
        row_limit_param="limit",
        parent_params=("project_path",),
    )
    def workbooks(self, project_path: str, limit: int = 200) -> List[str]:
        """Workbooks directly in one project, addressed by its path as
        `projects` reports it. Workbooks in nested projects are not included;
        ask for each project. Workbooks have no name filter: one is ingested
        exactly when its project is, so judge with `probe filter --kind
        Workbook --parent <project_path>`. Resolved by the project's LUID,
        because project names repeat across parents."""
        # _project_or_raise runs inside the block, so a 403 on the projects
        # listing degrades rather than reading as a bad argument.
        with soft_listing(self._warn):
            project = self._project_or_raise(project_path)
            options = TSC.RequestOptions()
            if not any(d in project.name for d in self._FILTER_DELIMITERS):
                # Only narrows the listing; the LUID check below decides.
                options.filter.add(
                    TSC.Filter(
                        TSC.RequestOptions.Field.ProjectName,
                        TSC.RequestOptions.Operator.Equals,
                        project.name,
                    )
                )
            context = f"workbooks listing for project {echoed(project_path)}"
            with _tsc_soft_statuses(context):
                return take(
                    (
                        item.name
                        for item in TSC.Pager(self._site.server.workbooks, options)
                        # As _init_workbook_registry: membership is by project id.
                        if item.project_id == project.id and item.name
                    ),
                    limit,
                )
        return []

    def _project_or_raise(self, project_path: str) -> TableauProject:
        for project in self._all_projects().values():
            if self._path(project) == project_path:
                return project
        raise ProbeArgumentError(
            f"no project with path {echoed(project_path)} on this site; "
            f"`probe run projects` lists them"
        )

    @probe_method(row_limit_param="limit")
    def database_servers(self, limit: int = 200) -> List[Dict[str, object]]:
        """Upstream database servers the Metadata API knows for this site: id,
        name, host (reduced as ingestion reduces it) and connection type. These
        are the keys of database_id_to_platform_instance_map and
        database_hostname_to_platform_instance_map, so a server whose tables
        would land in the wrong platform instance is visible before a run.
        Needs the Metadata API to be enabled; an error there is raised, not
        treated as empty."""
        servers = self._site.get_connection_objects(
            query=database_servers_graphql_query,
            connection_type=c.DATABASE_SERVERS_CONNECTION,
            page_size=self._config.effective_database_server_page_size,
        )
        return take(
            (
                {
                    "id": server.get(c.ID),
                    "name": server.get(c.NAME),
                    "host_name": parse_database_server_hostname(
                        server.get(c.HOST_NAME)
                    ),
                    "connection_type": server.get(c.CONNECTION_TYPE),
                }
                for server in servers
            ),
            limit,
        )
