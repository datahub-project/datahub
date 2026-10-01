from typing import (
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.rest_passthrough import RestApiPassthrough
from datahub.ingestion.agent.verdicts import ProbeSoftError, soft_on_status
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


class PowerBiMetadataProbe(RestApiPassthrough):
    """Metadata-only probe over the PowerBI REST API.

    Goes through the connector's own resolvers rather than PowerBiAPI:
    PowerBiAPI.get_workspaces swallows every HTTP error and returns [], which
    is right for an ingestion run and exactly wrong for a diagnostic. The
    resolvers carry the same pager, retry adapter and timeout ingestion uses.
    Never touches the async admin scanner (getInfo / scanStatus / scanResult).
    """

    warnings: List[str]

    def __init__(self, config: PowerBiDashboardSourceConfig) -> None:
        self._config = config
        # Built lazily: DataResolverBase.__init__ fetches an MSAL token, and a
        # command is where a bad credential should surface, not construction.
        self._resolvers: Dict[type, DataResolverBase] = {}
        self._withheld_personal = 0
        self.warnings = []
        self.api_base_url = DataResolverBase.my_org_url_for(config.environment)

    @classmethod
    def for_config(
        cls, config: PowerBiDashboardSourceConfig
    ) -> "PowerBiMetadataProbe":
        return cls(config)

    def __enter__(self) -> "PowerBiMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        for resolver in self._resolvers.values():
            resolver.request_session.close()

    def _resolver(self, resolver_cls: Type[_R]) -> _R:
        if resolver_cls not in self._resolvers:
            self._resolvers[resolver_cls] = make_resolver(self._config, resolver_cls)
        return cast(_R, self._resolvers[resolver_cls])

    def _listing_resolver(self) -> DataResolverBase:
        # The same choice as PowerBiAPI._get_resolver.
        if self._config.admin_apis_only:
            return self._resolver(AdminAPIResolver)
        return self._resolver(RegularAPIResolver)

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

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
                f"{self._config.modified_since!r}: {exc} Ingestion would log "
                f"this and fall back to listing every workspace."
            ) from exc
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
        self._withheld_personal = 0
        pages = resolver.itr_pages(
            endpoint=resolver.get_groups_endpoint(),
            parameter_override=self._modified_filter(),
        )
        for page in pages:
            for group in page:
                workspace = workspace_from_group(group, self._config.environment)
                if (
                    workspace.type in NON_ADDRESSABLE_WORKSPACE_TYPES
                    and workspace.type not in self._config.workspace_type_filter
                ):
                    self._withheld_personal += 1
                    continue
                yield workspace, group.get(Constant.STATE)

    def _note_withheld(self) -> None:
        if self._withheld_personal:
            self._warn(
                f"{self._withheld_personal} personal workspace(s) seen and not "
                f"listed: their type is not in workspace_type_filter, so "
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
        which applies all three. `type_allowed` is the workspace_type_filter
        verdict per record. `state` is set by the admin API only; ingestion's
        scan also skips workspaces that are not Active. Narrowed by
        modified_since exactly as ingestion narrows it. Personal workspaces
        the recipe does not ingest are counted in a warning, not listed.
        Metadata only."""
        rows: List[Dict[str, object]] = []
        for workspace, state in self._visible_workspaces():
            rows.append(
                {
                    "name": workspace.name,
                    "id": workspace.id,
                    "type": workspace.type,
                    "type_allowed": workspace.type
                    in self._config.workspace_type_filter,
                    "state": state,
                }
            )
            # The framework asks for limit+1; stopping here is what keeps the
            # remaining $top=1000 pages from being requested at all.
            if len(rows) >= limit:
                break
        self._note_withheld()
        return rows

    def _workspace_or_raise(self, name: str) -> Workspace:
        """Resolve a workspace name with one groups sweep. Names, not ids,
        because the name is what workspace_name_pattern and --parent carry."""
        matches = [ws for ws, _ in self._visible_workspaces() if ws.name == name]
        if not matches:
            # Only here: a withheld personal workspace may be the one named.
            self._note_withheld()
            raise ValueError(f"no workspace named '{name}' is listed for this recipe")
        if len(matches) > 1:
            raise ValueError(
                f"{len(matches)} workspaces are named '{name}' "
                f"(ids: {', '.join(ws.id for ws in matches)}); the probe "
                f"addresses workspaces by name, so rename one to probe it"
            )
        workspace = matches[0]
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
        try:
            with soft_on_status(403, 404, context=context):
                return fetch()
        except ProbeSoftError as exc:
            self._warn(str(exc))
            return []

    @probe_method(kind=BIAssetSubTypes.REPORT, parent_params=("workspace",))
    def reports(self, workspace: str) -> List[Dict[str, object]]:
        """Reports in one workspace, by workspace name. `type` is Report or
        PaginatedReport, the subtype ingestion emits; filter a paginated one
        with `--kind PaginatedReport`. App-published duplicates are dropped as
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
            self._warn(
                "extract_dashboards is false, so ingestion emits none of these"
            )
        return self._scoped(
            lambda: [
                {"name": d.displayName, "id": d.id}
                for d in self._listing_resolver().get_dashboards(ws)
            ],
            context=f"dashboards listing for workspace '{workspace}'",
        )
