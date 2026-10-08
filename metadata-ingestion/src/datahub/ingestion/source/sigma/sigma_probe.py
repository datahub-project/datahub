from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional, TypeVar

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    PersonalWithholding,
    ProbeProviderBase,
    resolve_name,
    take,
)
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.sigma.config import (
    Constant,
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.data_classes import (
    PERSONAL_WORKSPACE_NAME,
    SigmaDataModel,
    Workbook,
    Workspace,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.ingestion.source.sigma.sigma_selection import (
    DataModelFacts,
    WorkbookFacts,
    data_model_verdict,
    workbook_verdict,
)

# A personal file's /files path starts here when no workspace answers for it.
_PERSONAL_PATH_ROOT = "My Documents"

_T = TypeVar("_T")


@dataclass(frozen=True)
class _Placed:
    """One workbook or Data Model with the facts its verdict reads."""

    name: str
    object_id: str
    path: Optional[str]
    workspace: Optional[Workspace]
    # Workbooks only: False when /files does not list it, which drops it from
    # ingestion. None for a Data Model, which needs no /files row.
    in_files: Optional[bool] = None

    def record(self) -> Dict[str, object]:
        row: Dict[str, object] = {
            "name": self.name,
            "id": self.object_id,
            "has_workspace": self.workspace is not None,
        }
        if self.in_files is not None:
            row["in_files"] = self.in_files
        if self.workspace is not None:
            row["workspace"] = self.workspace.name
        return row


def _is_personal_workspace(workspace: Workspace) -> bool:
    return workspace.name == PERSONAL_WORKSPACE_NAME


def _is_personal(placed: _Placed) -> bool:
    if placed.workspace is not None:
        return _is_personal_workspace(placed.workspace)
    # Fails closed: a file with no readable workspace and no path may be
    # anyone's.
    return not placed.path or placed.path.split("/")[0] == _PERSONAL_PATH_ROOT


class SigmaMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over Sigma's REST API, through the connector's own
    SigmaAPI: its token exchange, refresh on 401, retrying session, and the
    /files walk that places each workbook and Data Model in a workspace."""

    # SigmaAPI logs request URLs and exception text from Sigma's responses,
    # which carry no credential shape for the log guard to catch.
    silenced_loggers = ("datahub.ingestion.source.sigma.sigma_api",)

    def __init__(self, config: SigmaSourceConfig) -> None:
        self._config = config
        self._report = SigmaSourceReport()

    @classmethod
    def for_config(cls, config: SigmaSourceConfig) -> "SigmaMetadataProbe":
        return cls(config)

    @property
    def probe_report(self) -> object:
        """SigmaAPI's report. Its listings record a failed read there and
        return what they had, so without it a refused /workspaces would read
        as a tenant with no workspaces."""
        return self._report

    def _api(self) -> SigmaAPI:
        # SigmaAPI.__init__ exchanges the client credentials for a token, so
        # it is built in a command, where a bad credential belongs.
        return self._open_once(
            "api",
            lambda: SigmaAPI(self._config, self._report),
            close=lambda api: api.session.close(),
        )

    def _all_workspaces(self) -> List[Workspace]:
        api = self._api()
        api.fill_workspaces()
        return list(api.workspaces.values())

    def _workspace_withholding(self) -> PersonalWithholding[Workspace]:
        return PersonalWithholding[Workspace](
            is_personal=_is_personal_workspace,
            would_ingest=lambda ws: self._config.workspace_pattern.allowed(ws.name),
        )

    def _note_withheld(
        self, withholding: PersonalWithholding[_T], what: str, *, stopped_early: bool
    ) -> None:
        if withholding.withheld:
            self._warn(
                f"{withholding.count_text(stopped_early=stopped_early)} {what} "
                f"seen and not listed: ingestion does not emit them under this "
                f"recipe, and a personal space ('{PERSONAL_WORKSPACE_NAME}') "
                f"holds one person's own documents"
            )

    def _resolve_workspace(self, workspace: str) -> Workspace:
        withholding = self._workspace_withholding()
        return resolve_name(
            workspace,
            (ws for ws in self._all_workspaces() if withholding.keep(ws)),
            key=lambda ws: ws.name,
            distinguish=lambda ws: ws.workspaceId,
            kind="workspace",
            list_command="probe run workspaces",
            on_ambiguous="the probe addresses workspaces by name",
        ).record

    @probe_method(kind=BIContainerSubTypes.SIGMA_WORKSPACE, row_limit_param="limit")
    def workspaces(self, limit: int = 200) -> List[Dict[str, object]]:
        """Workspaces this client can list, by name -- what workspace_pattern
        is matched against -- including ones it would exclude. A personal
        space is listed as 'My documents', the name ingestion matches; one the
        recipe does not ingest is counted in a warning, not listed. Metadata
        only."""
        withholding = self._workspace_withholding()
        kept = take(self._all_workspaces(), limit, keep=withholding.keep)
        self._note_withheld(
            withholding, "personal space(s)", stopped_early=len(kept) >= limit
        )
        return [{"name": ws.name, "id": ws.workspaceId} for ws in kept]

    def _scoped(
        self,
        placed: Iterator[_Placed],
        workspace: Optional[str],
        would_ingest: Callable[[_Placed], bool],
        what: str,
        limit: int,
    ) -> List[Dict[str, object]]:
        if workspace is not None:
            wanted = self._resolve_workspace(workspace).workspaceId
            placed = (
                p
                for p in placed
                if p.workspace is not None and p.workspace.workspaceId == wanted
            )
        withholding = PersonalWithholding[_Placed](
            is_personal=_is_personal, would_ingest=would_ingest
        )
        kept = take(placed, limit, keep=withholding.keep)
        self._note_withheld(withholding, what, stopped_early=len(kept) >= limit)
        return [p.record() for p in kept]

    def _placed_workbooks(self) -> Iterator[_Placed]:
        api = self._api()
        # Ingestion lists workspaces first, so get_workspace answers a listed
        # one from that cache; only an unlisted one costs a lookup.
        api.fill_workspaces()
        # The listing and the /files map get_sigma_workbooks reads; the
        # workspace lookup is its get_workspace, so a workspace that refuses
        # (403) leaves the workbook unplaced, as it does for ingestion.
        workbooks = api._paginated_entries(
            f"{self._config.api_url}/workbooks",
            Workbook,
            "Unable to fetch sigma workbooks.",
            enumerates_entities=True,
        )
        files = api._get_files_metadata(file_type=Constant.WORKBOOK)
        for workbook in workbooks:
            file = files.get(workbook.workbookId)
            workspace_id = file.workspaceId if file else None
            yield _Placed(
                name=workbook.name,
                object_id=workbook.workbookId,
                path=file.path if file else workbook.path,
                in_files=file is not None,
                workspace=api.get_workspace(workspace_id) if workspace_id else None,
            )

    def _would_ingest_workbook(self, placed: _Placed) -> bool:
        return workbook_verdict(
            self._config,
            WorkbookFacts(
                placed.name,
                in_files=placed.in_files is not False,
                workspace_name=placed.workspace.name if placed.workspace else None,
            ),
        ).included

    @probe_method(
        kind=BIContainerSubTypes.SIGMA_WORKBOOK,
        row_limit_param="limit",
        parent_params=("workspace",),
    )
    def workbooks(
        self, workspace: Optional[str] = None, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Workbooks, by name -- what workbook_pattern is matched against --
        in one workspace (by name), or across every workspace when none is
        given. Each record carries the facts ingestion also reads:
        `workspace` (absent, with has_workspace false, when no readable
        workspace holds it, so ingest_shared_entities decides) and `in_files`
        (false when Sigma's /files listing omits it, which drops it from
        ingestion: add this client's user to its workspace). Judge a saved run
        with `probe filter --kind "Sigma Workbook" --from-run`. Workbooks in a
        personal space the recipe does not ingest are counted, not listed.
        Metadata only."""
        return self._scoped(
            self._placed_workbooks(),
            workspace,
            self._would_ingest_workbook,
            "workbook(s) in a personal space",
            limit,
        )

    def _placed_data_models(self) -> Iterator[_Placed]:
        api = self._api()
        api.fill_workspaces()
        # get_data_models' listing and its workspace choice: the /files row's
        # workspace, else the /dataModels payload's own.
        data_models = api._paginated_entries(
            f"{self._config.api_url}/dataModels",
            SigmaDataModel,
            "Unable to fetch sigma data models.",
            dedup_key=lambda dm: dm.dataModelId,
            enumerates_entities=True,
            optional_feature="ingest_data_models=False",
        )
        files = api._get_files_metadata(file_type=Constant.DATA_MODEL)
        for data_model in data_models:
            file = files.get(data_model.dataModelId)
            workspace_id = (file.workspaceId if file else None) or (
                data_model.workspaceId
            )
            yield _Placed(
                name=data_model.name,
                object_id=data_model.dataModelId,
                path=file.path if file else data_model.path,
                workspace=api.get_workspace(workspace_id) if workspace_id else None,
            )

    def _would_ingest_data_model(self, placed: _Placed) -> bool:
        if not self._config.ingest_data_models:
            return False
        return data_model_verdict(
            self._config,
            DataModelFacts(
                placed.name,
                workspace_name=placed.workspace.name if placed.workspace else None,
            ),
        ).included

    @probe_method(
        kind=BIContainerSubTypes.SIGMA_DATA_MODEL,
        row_limit_param="limit",
        parent_params=("workspace",),
    )
    def data_models(
        self, workspace: Optional[str] = None, limit: int = 200
    ) -> List[Dict[str, object]]:
        """Data Models /dataModels lists, by name -- what data_model_pattern
        is matched against -- in one workspace (by name), or across every
        workspace when none is given. Records carry `workspace` and
        `has_workspace` as `workbooks` does. A personal-space Data Model that
        /dataModels does not list, which ingestion reaches only through
        another Data Model's lineage with ingest_shared_entities on, is not
        here. Data Models in a personal space the recipe does not ingest are
        counted, not listed. Metadata only."""
        if not self._config.ingest_data_models:
            self._warn("ingest_data_models is false, so ingestion emits none of these")
        return self._scoped(
            self._placed_data_models(),
            workspace,
            self._would_ingest_data_model,
            "Data Model(s) in a personal space",
            limit,
        )
