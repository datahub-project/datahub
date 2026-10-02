from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)
from urllib.parse import urlparse

import requests

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    PersonalWithholding,
    ProbeProviderBase,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.rest_passthrough import RestApiPassthrough
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
)
from datahub.ingestion.source.powerbi.config import (
    NON_ADDRESSABLE_WORKSPACE_TYPES,
    Constant,
    PowerBiDashboardSourceConfig,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Workspace
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    DataResolverBase,
    RegularAPIResolver,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import (
    groups_filter,
    make_resolver,
    workspace_from_group,
)

_R = TypeVar("_R", bound=DataResolverBase)


class PowerBiMetadataProbe(ProbeProviderBase, RestApiPassthrough):
    """Metadata-only probe over the PowerBI REST API.

    Goes through the connector's own resolvers rather than PowerBiAPI:
    PowerBiAPI.get_workspaces swallows every HTTP error and returns [], which
    is right for an ingestion run and exactly wrong for a diagnostic. The
    resolvers carry the same pager, retry adapter and timeout ingestion uses.
    Never touches the async admin scanner (getInfo / scanStatus / scanResult).
    """

    # Regular-API listings only. What is deliberately absent, and why:
    #   /admin/groups (any form) -- the whole tenant's personal workspaces,
    #       named after their owners. /groups is allowed because
    #       api_fetch_json withholds those records as `workspaces` does.
    #   $expand on anything      -- $expand=users returns email addresses.
    #   /groups/{id}/datasets    -- each record carries configuredBy (an email).
    #   .../datasets/{id}/parameters, .../reports/{id}/datasources -- parameter
    #       values and connection details.
    #   /admin/workspaces/getInfo|scanStatus|scanResult -- the scanner; its
    #       result embeds M expressions, DAX and native SQL, a route for WHERE
    #       literals (the rule hex_probe applies to /cells).
    #   executeQueries, Export*, users, activityevents -- rows, content, PII.
    api_allowlist = (
        "GET /groups?$top&$skip&$filter",
        "GET /groups/{id}/reports",
        "GET /groups/{id}/dashboards",
    )

    def __init__(self, config: PowerBiDashboardSourceConfig) -> None:
        self._config = config
        self._withholding: PersonalWithholding[Workspace] = self._new_withholding()
        self.api_base_url = DataResolverBase.my_org_url_for(config.environment)
        self._groups_path = urlparse(self.api_base_url).path.rstrip("/") + "/groups"

    @classmethod
    def for_config(cls, config: PowerBiDashboardSourceConfig) -> "PowerBiMetadataProbe":
        return cls(config)

    def api_fetch_json(self, url: str) -> object:
        # The connector's retrying, timed session and its token refresh, so a
        # probe request behaves as an ingestion request does on 429/5xx.
        resolver = self._resolver(RegularAPIResolver)
        response = resolver.request_session.get(
            url, headers=resolver.get_authorization_header()
        )
        response.raise_for_status()
        body = response.json()
        if urlparse(url).path.rstrip("/") == self._groups_path:
            return self._without_withheld_groups(body)
        return body

    def _is_personal_type(self, workspace_type: Optional[str]) -> bool:
        # Named after its owner.
        return workspace_type in NON_ADDRESSABLE_WORKSPACE_TYPES

    def _ingests_type(self, workspace_type: Optional[str]) -> bool:
        return workspace_type in self._config.workspace_type_filter

    def _new_withholding(self) -> PersonalWithholding[Workspace]:
        """Withholds a personal workspace the recipe does not ingest: named
        after its owner, and never emitted by ingestion."""
        return PersonalWithholding[Workspace](
            is_personal=lambda ws: self._is_personal_type(ws.type),
            would_ingest=lambda ws: self._ingests_type(ws.type),
        )

    def _without_withheld_groups(self, body: object) -> object:
        # The raw listing must withhold what `workspaces` withholds, or the
        # `api` route would hand back the owner names that command keeps out.
        if not isinstance(body, dict) or not isinstance(body.get("value"), list):
            return body
        withholding = PersonalWithholding[object](
            is_personal=lambda g: (
                isinstance(g, dict) and self._is_personal_type(g.get("type"))
            ),
            would_ingest=lambda g: (
                isinstance(g, dict) and self._ingests_type(g.get("type"))
            ),
        )
        kept = [group for group in body["value"] if withholding.keep(group)]
        self._note_withheld(withholding)
        return {**body, "value": kept}

    def _resolver(self, resolver_cls: Type[_R]) -> _R:
        # Built lazily: DataResolverBase.__init__ fetches an MSAL token, and a
        # command is where a bad credential should surface, not construction.
        return self._open_once(
            resolver_cls,
            lambda: make_resolver(self._config, resolver_cls),
            close=lambda resolver: resolver.request_session.close(),
        )

    def _listing_resolver(self) -> DataResolverBase:
        # The same choice as PowerBiAPI._get_resolver.
        if self._config.admin_apis_only:
            return self._resolver(AdminAPIResolver)
        return self._resolver(RegularAPIResolver)

    def _modified_filter(self) -> Dict[str, str]:
        """The $filter PowerBiAPI.get_workspaces builds from modified_since."""
        if not self._config.modified_since:
            return {}
        # Outside the try: a token failure building the resolver is also a
        # ConfigurationError, and it is a connection problem, not a bad value.
        admin = self._resolver(AdminAPIResolver)
        try:
            ids = admin.get_modified_workspaces(self._config.modified_since)
        except ConfigurationError as exc:
            # The resolver raises this for PowerBI's 400 (e.g. a date older
            # than 30 days): a recipe value to fix. Ingestion logs it and lists
            # every workspace instead; a probe must not present that as
            # filtered.
            raise ValueError(
                f"PowerBI refused modified_since="
                f"{self._config.modified_since!r} ({type(exc).__name__} from "
                f"the modified-workspaces request). Ingestion would log "
                f"this and fall back to listing every workspace."
            ) from exc
        except requests.exceptions.RequestException as exc:
            # Anything else -- a 401/403 without admin API access, a timeout --
            # PowerBiAPI.get_modified_workspaces swallows, and ingestion lists
            # every workspace unfiltered. Do the same, and say so.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            reason = f"HTTP {status}" if status is not None else type(exc).__name__
            self._warn(
                f"could not read the workspaces modified since modified_since "
                f"({reason}), so ingestion would log this and fall back to "
                f"listing every workspace; this listing does the same"
            )
            return {}
        if not ids:
            self._warn(
                "modified_since is set but PowerBI reports no workspace modified "
                "since then, so ingestion applies no id filter and lists every "
                "workspace; this listing does the same"
            )
        return groups_filter(ids)

    def _visible_workspaces(self) -> Iterator[Tuple[Workspace, Optional[str]]]:
        """Every workspace the recipe's API path lists, as (workspace, state).

        A personal workspace whose type the recipe does not ingest is counted,
        not yielded: the admin API names it after its owner, and ingestion
        never emits it. Once the recipe opts into that type, ingestion emits
        it and so does this.
        """
        resolver = self._listing_resolver()
        self._withholding = self._new_withholding()
        pages = resolver.itr_pages(
            endpoint=resolver.get_groups_endpoint(),
            parameter_override=self._modified_filter(),
        )
        for page in pages:
            for group in page:
                workspace = workspace_from_group(group, self._config.environment)
                if not self._withholding.keep(workspace):
                    continue
                yield workspace, group.get(Constant.STATE)

    def _note_withheld(
        self,
        withholding: Optional[PersonalWithholding[Any]] = None,
        *,
        stopped_early: bool = False,
    ) -> None:
        w = withholding or self._withholding
        if w.withheld:
            self._warn(
                f"{w.count_text(stopped_early=stopped_early)} personal "
                f"workspace(s) seen and not listed: their type is not in workspace_type_filter, so "
                f"ingestion skips them, and a personal workspace is named after "
                f"its owner"
            )

    @probe_method(kind=BIContainerSubTypes.POWERBI_WORKSPACE, row_limit_param="limit")
    def workspaces(self, limit: int = 200) -> List[Dict[str, object]]:
        """Workspaces this recipe's credential lists -- the regular API's
        member workspaces, or the whole tenant with admin_apis_only --
        including ones the recipe's workspace filters would exclude. A
        workspace must pass workspace_name_pattern, workspace_id_pattern and
        workspace_type_filter together: save this listing with `--report-to`
        and judge it with `probe filter --kind Workspace --from-run <report>`,
        which applies all three, and also excludes a workspace whose `state`
        is not Active, as ingestion's scan does. `type_allowed` is the
        workspace_type_filter verdict per record. `state` is set by the admin
        API only. Narrowed by
        modified_since exactly as ingestion narrows it. Personal workspaces
        the recipe does not ingest are counted in a warning, not listed.
        Metadata only."""
        # The framework asks for limit+1; take stopping there is what keeps
        # the remaining $top=1000 pages from being requested at all.
        visible = take(self._visible_workspaces(), limit)
        rows: List[Dict[str, object]] = [
            {
                "name": workspace.name,
                "id": workspace.id,
                "type": workspace.type,
                "type_allowed": workspace.type in self._config.workspace_type_filter,
                "state": state,
            }
            for workspace, state in visible
        ]
        self._note_withheld(stopped_early=len(visible) >= limit)
        return rows

    def _workspace_or_raise(self, name: str) -> Workspace:
        """Resolve a workspace name with one groups sweep. Names, not ids,
        because the name is what workspace_name_pattern and --parent carry."""
        # The records are the withheld-filtered sweep, so a "did you mean"
        # hint can never print a personal workspace's (owner's) name.
        workspace = resolve_name(
            name,
            (ws for ws, _ in self._visible_workspaces()),
            key=lambda ws: ws.name,
            distinguish=lambda ws: ws.id,
            kind="workspace",
            where="listed for this recipe",
            list_command="probe run workspaces",
            on_ambiguous=(
                "the probe addresses workspaces by name, so rename one to probe it"
            ),
            # Only here: a withheld personal workspace may be the one named.
            on_miss=self._note_withheld,
        ).record
        # probe filter judges --parent on workspace_name_pattern only, since
        # --parent carries no id or type; get_allowed_workspaces also requires
        # these two, so say when either drops the workspace.
        if not self._config.workspace_id_pattern.allowed(workspace.id):
            self._warn(
                f"workspace '{name}' (id {workspace.id}) is excluded by "
                f"workspace_id_pattern, so ingestion reads nothing in it"
            )
        if workspace.type not in self._config.workspace_type_filter:
            self._warn(
                f"workspace '{name}' has type '{workspace.type}', which "
                f"workspace_type_filter excludes, so ingestion reads nothing in it"
            )
        return workspace

    def _scoped(
        self, fetch: Callable[[], List[Dict[str, object]]], context: str
    ) -> List[Dict[str, object]]:
        # 403/404 on one workspace's listing degrades; auth and 5xx raise.
        with soft_listing(self._warn, 403, 404, context=context):
            return fetch()
        return []

    @probe_method(kind=BIAssetSubTypes.REPORT, parent_params=("workspace",))
    def reports(self, workspace: str) -> List[Dict[str, object]]:
        """Reports in one workspace, by workspace name. `type` is Report or
        PaginatedReport, the subtype ingestion emits. Both kinds share one
        verdict (their workspace's, and the extract_reports switch), so a
        saved listing judged with `probe filter --from-run` as Report answers
        for the paginated rows too. App-published duplicates are dropped as
        ingestion drops them. Read from the workspace's report listing;
        ingestion can also see objects through the admin scan, so a shorter
        list here can point at the credential's workspace membership. Nothing
        filters reports themselves -- their workspace's verdict decides.
        Metadata only."""
        ws = self._workspace_or_raise(workspace)
        if not self._config.extract_reports:
            self._warn("extract_reports is false, so ingestion emits none of these")
        return self._scoped(
            lambda: [
                {"name": r.name, "id": r.id, "type": r.type.value}
                for r in self._listing_resolver().get_reports(ws)
            ],
            context=f"reports listing for workspace '{workspace}'",
        )

    # Kind "Dashboard": PowerBI dashboards are emitted with no SubTypes
    # aspect, so the entity type is the only name a caller has for them.
    @probe_method(kind=BIAssetSubTypes.DASHBOARD, parent_params=("workspace",))
    def dashboards(self, workspace: str) -> List[Dict[str, object]]:
        """Dashboards in one workspace, by workspace name, with app-published
        duplicates dropped as ingestion drops them. Nothing filters dashboards
        themselves -- their workspace's verdict decides. Metadata only."""
        ws = self._workspace_or_raise(workspace)
        if not self._config.extract_dashboards:
            self._warn("extract_dashboards is false, so ingestion emits none of these")
        return self._scoped(
            lambda: [
                {"name": d.displayName, "id": d.id}
                for d in self._listing_resolver().get_dashboards(ws)
            ],
            context=f"dashboards listing for workspace '{workspace}'",
        )

    @probe_method()
    def admin_api_access(self) -> Dict[str, object]:
        """Whether this credential can call PowerBI's read-only admin APIs.
        Ingestion always uses them for the workspace scan -- scan-derived
        lineage, endorsements, apps -- even without admin_apis_only, and on
        "denied" it still runs without those; paginated-report datasource
        lineage still comes through the regular API. Checked with one single-row
        admin workspace listing; the scanner itself is never called, so
        "granted" is a strong signal rather than proof the scan will
        succeed."""
        resolver = self._resolver(AdminAPIResolver)
        pages = resolver.itr_pages(
            endpoint=resolver.get_groups_endpoint(), parameter_override={"$top": 1}
        )
        try:
            # First page only: advancing further would page the whole tenant
            # one row at a time.
            next(pages, None)
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (401, 403):
                return {
                    "admin_api": "denied",
                    "status": status,
                    "admin_apis_only": self._config.admin_apis_only,
                }
            raise
        return {
            "admin_api": "granted",
            "status": 200,
            "admin_apis_only": self._config.admin_apis_only,
        }
