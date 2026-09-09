import functools
import logging
import sys
from collections import deque
from collections.abc import Hashable
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
from urllib.parse import quote, urlencode

import requests
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.sigma.config import (
    Constant,
    SigmaSourceConfig,
    SigmaSourceReport,
)
from datahub.ingestion.source.sigma.data_classes import (
    CustomSqlEntry,
    DataModelElementUpstream,
    DatasetUpstream,
    Element,
    ElementUpstream,
    File,
    Page,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
    SigmaDataset,
    WarehouseInodeRaw,
    WarehouseTableUpstream,
    Workbook,
    WorkbookLineageTableEntry,
    Workspace,
)
from datahub.ingestion.source.sigma.spec_parser import (
    key_skeleton,
)

# Logger instance
logger = logging.getLogger(__name__)

# Workbook element types ingested as Charts. An element outside this set is
# dropped before it is indexed, so a chart formula naming it can never resolve
# and falls back to a self-reference.
#
# 'pivot-table' and 'input-table' were added after a tenant showed 992 and 201
# of them dropped: both hold real columns that other elements' formulas
# reference, and both are things a user sees on the page, so representing them
# as Charts is consistent with how 'table' is treated. They cost the same two
# per-element calls (/lineage and /query) as any other admitted element.
# Layout/UI elements Sigma returns alongside data elements. They carry no name,
# so the model rejects them -- correctly, but they are not malformed data.
_NON_DATA_ELEMENT_TYPES = frozenset({"control", "divider", "text", "image", "button"})

# Lineage nodes that combine inputs and hold no data of their own, so the walk
# continues through them to whatever feeds them. 'union' was found unhandled on
# a live tenant (2026-09): every element behind one lost its upstreams entirely,
# for the same reason 'join' would have before it was handled.
_PASS_THROUGH_NODE_TYPES = frozenset({"join", "union"})

# A lineage nodeId that names a stored file rather than an element.
_INODE_PREFIX = "inode-"
_SEMANTIC_VIEW_TABLE = "semanticViewTable"
_CONNECTION_ID = "connectionId"

# Sigma explains a 4xx in the response body; the exception text carries only
# "400 Client Error: Bad Request for url: ...", which is what 12 workbooks
# aborted with on one tenant (2026-09) while costing 37,655 chart columns their
# formulas. Bounded because a body is not guaranteed to be a short message.
_MAX_ERROR_BODY_CHARS = 400

BASE_ELEMENT_TYPES = frozenset({"table", "visualization"})
INGESTED_ELEMENT_TYPES = BASE_ELEMENT_TYPES | frozenset({"pivot-table", "input-table"})

T = TypeVar("T", bound=BaseModel)


def _error_body(response: Optional[requests.Response]) -> Optional[str]:
    """The server's explanation for a 4xx, bounded.

    ``requests`` puts only the status line into the exception text, so a 400
    that Sigma explains in its body reads as an unexplained failure. Reading
    the body must never itself raise -- the call has already failed.
    """
    if response is None:
        return None
    try:
        text = (response.text or "").strip()
    except Exception:
        return None
    if not text:
        return None
    return text[:_MAX_ERROR_BODY_CHARS].replace("\n", " ")


class SigmaAPI:
    def __init__(self, config: SigmaSourceConfig, report: SigmaSourceReport) -> None:
        self.config = config
        self.report = report
        self.workspaces: Dict[str, Workspace] = {}
        self.users: Dict[str, str] = {}
        # Track source_type values we've already warned about to keep the
        # report summary readable on large tenants with repeated unknown
        # node types.
        self._unknown_lineage_node_types_warned: Set[str] = set()
        # Workbooks whose /columns fetch aborted, so their column formulas are
        # missing or incomplete through no fault of the resolver. Read at emit
        # time to keep those columns out of the "Sigma reported no formula"
        # bucket. Public because SigmaSource, not the API client, is what
        # attributes a column to a cause.
        self.column_formulas_incomplete_workbooks: Set[str] = set()
        # /spec fails identically for every model when the token lacks the
        # scope; warn once and let the counter carry the magnitude.
        self._spec_unavailable_warned: bool = False
        self._element_fetch_failed_warned: bool = False
        # Public: callers log which types were admitted, and naming the module
        # constant instead would report types this run never accepted.
        self.ingested_element_types = (
            INGESTED_ELEMENT_TYPES
            if config.ingest_pivot_and_input_tables
            else BASE_ELEMENT_TYPES
        )
        self.session = requests.Session()

        # Configure retry strategy for 429/503 with exponential backoff.
        # raise_on_status=False must stay False: get_data_model_by_url_id
        # inspects response.status_code to surface 429 explicitly; if True,
        # exhausted retries raise MaxRetryError and bypass that branch.
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 503],
            backoff_factor=2,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.refresh_token: Optional[str] = None
        # Test connection by generating access token
        logger.info(f"Trying to connect to {self.config.api_url}")
        self._generate_token()

    def _generate_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret.get_secret_value(),
        }
        response = self.session.post(f"{self.config.api_url}/auth/token", data=data)
        response.raise_for_status()
        response_dict = response.json()
        self.refresh_token = response_dict[Constant.REFRESH_TOKEN]
        self.session.headers.update(
            {
                "Authorization": f"Bearer {response_dict[Constant.ACCESS_TOKEN]}",
                "Content-Type": "application/json",
            }
        )

    def _log_http_error(self, message: str, *, report_warning: bool = True) -> Any:
        """Record a failed Sigma API call.

        This is the terminal handler for most ``except`` blocks in this class,
        so anything it drops is invisible. It used to log a context-free
        ``HTTP status-code = 404`` at WARNING and put the only identifying
        detail on a DEBUG line -- an operator not running with ``--debug`` saw
        a bare status code and nothing about which call failed, and the
        ingestion report showed nothing at all. On one tenant (2026-09) that
        hid 26 failures across three status codes.

        ``message`` already names the resource at every call site, so it is
        passed through as the warning context. The title is fixed so LossyList
        groups them, and the counter beside it carries the true total after
        that list truncates.
        """
        _, e, _ = sys.exc_info()
        response = (
            e.response
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None
            else None
        )
        status = response.status_code if response is not None else None
        body = _error_body(response)
        retry_after = (
            response.headers.get("Retry-After") if response is not None else None
        )
        key = str(status) if status is not None else type(e).__name__
        self.report.api_call_failures_by_status[key] = (
            self.report.api_call_failures_by_status.get(key, 0) + 1
        )
        if not report_warning:
            # The caller emits its own, better-scoped warning for this failure
            # (pagination aborts name the endpoint, the URL and how many rows
            # survived). The counter above still fires, so the failure is
            # never invisible -- only un-duplicated.
            logger.debug(msg=message, exc_info=e)
            return e
        self.report.warning(
            title="Sigma API call failed",
            message="A Sigma API call failed. The affected objects are emitted "
            "without whatever that call would have provided; see "
            "api_call_failures_by_status for the totals by status code.",
            context=(
                f"{message} (http_status={status}"
                + (f", retry_after={retry_after}" if retry_after else "")
                + (f", body={body}" if body else "")
                + ")"
            ),
        )
        logger.debug(msg=message, exc_info=e)
        return e

    def _refresh_access_token(self):
        try:
            data = {
                "grant_type": Constant.REFRESH_TOKEN,
                "refresh_token": self.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret.get_secret_value(),
            }
            post_response = self.session.post(
                f"{self.config.api_url}/auth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
            )
            post_response.raise_for_status()
            response_dict = post_response.json()
            self.refresh_token = response_dict[Constant.REFRESH_TOKEN]
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {response_dict[Constant.ACCESS_TOKEN]}",
                    "Content-Type": "application/json",
                }
            )
        except Exception as e:
            self._log_http_error(
                message=f"Unable to refresh access token. Exception: {e}"
            )

    def _get_api_call(self, url: str) -> requests.Response:
        """Make an API call with token refresh on 401.

        The session adapter retries 429/503 for every endpoint; nothing else is
        retried. A 409 on the Data Model endpoints was tried and reverted --
        it proved persistent rather than transient on a real tenant.
        """
        get_response = self.session.get(url)

        # Handle token refresh on 401
        if get_response.status_code == 401 and self.refresh_token:
            logger.debug("Access token might expired. Refreshing access token.")
            self._refresh_access_token()
            get_response = self.session.get(url)

        return get_response

    def get_workspace(self, workspace_id: str) -> Optional[Workspace]:
        if workspace_id in self.workspaces:
            return self.workspaces[workspace_id]

        logger.debug(f"Fetching workspace metadata with id '{workspace_id}'")
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workspaces/{workspace_id}"
            )
            if response.status_code == 403:
                logger.debug(f"Workspace {workspace_id} not accessible.")
                self.report.non_accessible_workspaces_count += 1
                return None
            response.raise_for_status()
            workspace = Workspace.model_validate(response.json())
            self.workspaces[workspace.workspaceId] = workspace
            return workspace
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch workspace '{workspace_id}'. Exception: {e}"
            )
        return None

    def fill_workspaces(self) -> None:
        logger.debug("Fetching all accessible workspaces metadata.")
        workspace_url = url = f"{self.config.api_url}/workspaces?limit=50"
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for workspace_dict in response_dict[Constant.ENTRIES]:
                    self.workspaces[workspace_dict[Constant.WORKSPACEID]] = (
                        Workspace.model_validate(workspace_dict)
                    )
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{workspace_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
        except Exception as e:
            self._log_http_error(message=f"Unable to fetch workspaces. Exception: {e}")

    @functools.lru_cache()
    def _get_users(self) -> Dict[str, str]:
        logger.debug("Fetching all accessible users metadata.")
        try:
            users: Dict[str, str] = {}
            members_url = url = f"{self.config.api_url}/members?limit=50"
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for user_dict in response_dict[Constant.ENTRIES]:
                    users[user_dict[Constant.MEMBERID]] = user_dict[Constant.EMAIL]
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{members_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            return users
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch users details. Exception: {e}"
            )
            return {}

    def get_user_name(self, user_id: str) -> Optional[str]:
        return self._get_users().get(user_id)

    @functools.lru_cache()
    def get_workspace_id_from_file_path(
        self, parent_id: str, path: str
    ) -> Optional[str]:
        try:
            path_list = path.split("/")
            while len(path_list) != 1:  # means current parent id is folder's id
                response = self._get_api_call(
                    f"{self.config.api_url}/files/{parent_id}"
                )
                response.raise_for_status()
                parent_id = response.json()[Constant.PARENTID]
                path_list.pop()
            return parent_id
        except Exception as e:
            self.report.workspace_id_lookup_failed += 1
            logger.error(
                f"Unable to find workspace id using file path '{path}'. Exception: {e}"
            )
            # Was a bare logger.error, so it never reached the ingestion report
            # -- an operator reading the report saw nothing at all.
            self.report.warning(
                title="Sigma workspace id lookup failed",
                message=(
                    "Could not walk a file path back to its workspace. The "
                    "affected entity is emitted without workspace attribution, "
                    "so it will be missing from workspace browse paths and from "
                    "the per-workspace counts."
                ),
                context=f"path={path!r}, remaining_segments={len(path_list)}",
                exc=e,
            )
            return None

    @functools.lru_cache
    def _get_files_metadata(self, file_type: str) -> Dict[str, File]:
        logger.debug(f"Fetching file metadata with type {file_type}.")
        file_url = url = (
            f"{self.config.api_url}/files?permissionFilter=view&typeFilters={file_type}"
        )
        try:
            files_metadata: Dict[str, File] = {}
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for file_dict in response_dict[Constant.ENTRIES]:
                    file = File.model_validate(file_dict)
                    file.workspaceId = self.get_workspace_id_from_file_path(
                        file.parentId, file.path
                    )
                    files_metadata[file_dict[Constant.ID]] = file
                if response_dict[Constant.NEXTPAGE]:
                    url = f"{file_url}&page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            self.report.number_of_files_metadata[file_type] = len(files_metadata)
            return files_metadata
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch files metadata. Exception: {e}"
            )
            return {}

    def get_connections(self) -> List[Dict[str, Any]]:
        """Fetch all Sigma Connections (paginated). Returns raw API payloads.

        Mapping to SigmaConnectionRecord happens in
        connection_registry.SigmaConnectionRegistry.build().
        """
        return self._paginated_raw_entries(
            f"{self.config.api_url}/connections",
            "Unable to fetch Sigma connections.",
        )

    def get_sigma_datasets(self) -> List[SigmaDataset]:
        logger.debug("Fetching all accessible datasets metadata.")
        dataset_url = url = f"{self.config.api_url}/datasets"
        dataset_files_metadata = self._get_files_metadata(file_type=Constant.DATASET)
        try:
            datasets: List[SigmaDataset] = []
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for dataset_dict in response_dict[Constant.ENTRIES]:
                    dataset = SigmaDataset.model_validate(dataset_dict)

                    if dataset.datasetId not in dataset_files_metadata:
                        self.report.datasets.dropped(
                            f"{dataset.name} ({dataset.datasetId}) (missing file metadata)"
                        )
                        continue

                    dataset.workspaceId = dataset_files_metadata[
                        dataset.datasetId
                    ].workspaceId

                    dataset.path = dataset_files_metadata[dataset.datasetId].path
                    dataset.badge = dataset_files_metadata[dataset.datasetId].badge

                    workspace = None
                    if dataset.workspaceId:
                        workspace = self.get_workspace(dataset.workspaceId)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.datasets.processed(
                                f"{dataset.name} ({dataset.datasetId}) in {workspace.name}"
                            )
                            datasets.append(dataset)
                        else:
                            self.report.datasets.dropped(
                                f"{dataset.name} ({dataset.datasetId}) in {workspace.name}"
                            )
                    elif self.config.ingest_shared_entities:
                        # If no workspace for dataset we can consider it as shared entity
                        self.report.datasets_without_workspace += 1
                        self.report.datasets.processed(
                            f"{dataset.name} ({dataset.datasetId}) in workspace id {dataset.workspaceId or 'unknown'}"
                        )
                        datasets.append(dataset)
                    else:
                        self.report.datasets.dropped(
                            f"{dataset.name} ({dataset.datasetId}) in workspace id {dataset.workspaceId or 'unknown'}"
                        )

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{dataset_url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break

            return datasets
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma datasets. Exception: {e}"
            )
            return []

    def _process_lineage_node(
        self,
        source_node_id: str,
        source_node: Dict,
        upstream_sources: Dict[str, "ElementUpstream"],
        queue: "Deque[str]",
        element: Element,
        workbook: Workbook,
    ) -> None:
        """Dispatch one BFS node into upstream_sources or re-enqueue it (join)."""
        source_type = source_node.get(Constant.TYPE)
        if source_type == "dataset":
            try:
                upstream_sources[source_node_id] = DatasetUpstream(
                    name=source_node.get(Constant.NAME),
                )
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "sheet":
            element_id = source_node.get(Constant.ELEMENTID)
            if element_id is None:
                self.report.warning(
                    title="Sigma sheet lineage node missing elementId",
                    message="Sheet upstream node missing elementId",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                )
                return
            try:
                upstream_sources[source_node_id] = SheetUpstream(
                    name=source_node.get(Constant.NAME),
                    element_id=element_id,
                )
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "data-model":
            # Node id shape is "<dataModelUrlId>/<opaque_suffix>"; we carry
            # the prefix and the DM-side ``name`` for name-based matching
            # at emit time.
            dm_url_id = (
                source_node_id.split("/")[0]
                if "/" in source_node_id
                else source_node_id
            )
            if not dm_url_id:
                self.report.warning(
                    title="Sigma data-model lineage node missing url-id prefix",
                    message="Sigma data-model lineage node missing url-id prefix",
                    context=(
                        f"node={source_node_id}, element={element.name}, "
                        f"workbook={workbook.name}"
                    ),
                )
                return
            try:
                # Uses the API-reported DM-side name. The edge-only
                # synthesis path below uses the workbook element's own
                # name instead; the two diverge only if the workbook
                # element was renamed after the DM link.
                upstream_sources[source_node_id] = DataModelElementUpstream(
                    name=source_node.get(Constant.NAME),
                    data_model_url_id=dm_url_id,
                )
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type in _PASS_THROUGH_NODE_TYPES:
            # A combining node holds no data of its own: its upstreams are what
            # feed it, so re-enqueue and keep walking. The BFS ``visited`` set
            # makes re-enqueueing safe.
            self.report.workbook_lineage_pass_through_nodes[str(source_type)] = (
                self.report.workbook_lineage_pass_through_nodes.get(str(source_type), 0)
                + 1
            )
            queue.append(source_node_id)
        elif source_type == "table":
            # nodeId format: "inode-{urlId}". Strip prefix; name is used for
            # name-based resolution in SigmaSource via wb_warehouse_table_index.
            if not isinstance(source_node_id, str) or not source_node_id.startswith(
                "inode-"
            ):
                self.report.chart_warehouse_table_node_skipped += 1
                logger.debug(
                    "Sigma chart BFS table node has unexpected nodeId format: "
                    "node=%s, element=%s, workbook=%s",
                    source_node_id,
                    element.name,
                    workbook.name,
                )
                return
            url_id = source_node_id[len("inode-") :]
            name = source_node.get(Constant.NAME)
            if not url_id or not isinstance(name, str) or not name:
                self.report.chart_warehouse_table_node_skipped += 1
                logger.debug(
                    "Sigma chart BFS table node missing url_id or name: "
                    "node=%s, name=%r, element=%s, workbook=%s",
                    source_node_id,
                    name,
                    element.name,
                    workbook.name,
                )
                return
            upstream_sources[source_node_id] = WarehouseTableUpstream(
                url_id=url_id,
                name=name,
            )
        elif source_type == "datasheet":
            # Shape confirmed on a live tenant (2026-09): ``{nodeId, type}`` --
            # no ``name`` and no sources of its own, so it is a leaf, not a
            # pass-through. Its nodeId comes in two shapes and the type field
            # does not say which, so each branch TESTS the shape rather than
            # assuming it.
            if source_node_id.startswith(_INODE_PREFIX):
                # A stored datasheet. Nothing here identifies a warehouse table
                # or carries a name, and DatasetUpstream needs a name to
                # SQL-correlate, so emitting one would only add a counted drop.
                self.report.workbook_lineage_datasheet_inode_unresolved += 1
                logger.debug(
                    "DATASHEET NODE element=%s workbook=%s: nodeId is an "
                    "inode with no name on the node, so there is nothing to "
                    "resolve it by. Resolving these needs the /files entry for "
                    "the inode, which this endpoint does not give.",
                    element.elementId,
                    workbook.workbookId,
                )
                return
            # Otherwise the nodeId is a bare element id. Emitting a SheetUpstream
            # is self-validating: the emit-time lookup drops it when no element
            # in this workbook has that id, so a wrong guess costs a debug line
            # rather than a fabricated edge.
            try:
                upstream_sources[source_node_id] = SheetUpstream(
                    name=source_node.get(Constant.NAME),
                    element_id=source_node_id,
                )
                self.report.workbook_lineage_datasheet_as_sheet += 1
            except ValidationError as e:
                self.report.warning(
                    title="Sigma lineage node parse failed",
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "datafile":
            # An uploaded file. Shape is ``{name, nodeId, type}`` with a UUID
            # nodeId, no sources and no warehouse counterpart -- there is no
            # dataset on any platform to point an upstream at. A true leaf, so
            # this is counted rather than warned about.
            self.report.workbook_lineage_datafile_leaf += 1
        elif source_type == "semantic-view":
            # Shape is ``{name, nodeId, semanticViewTable, type}``.
            # ``semanticViewTable`` looks like a warehouse path, but resolving
            # one needs the connection behind it to pick a platform, and this
            # node carries no connectionId. Logged with the part count only --
            # enough to settle whether it is db.schema.view without printing a
            # customer's warehouse path.
            self.report.workbook_lineage_semantic_view_unresolved += 1
            table_ref = source_node.get(_SEMANTIC_VIEW_TABLE)
            logger.debug(
                "SEMANTIC VIEW NODE element=%s workbook=%s: not resolved to a "
                "warehouse dataset. semanticViewTable has %d dot-separated "
                "part(s); the node carries connectionId=%s, which is what a "
                "resolver would need to choose a platform.",
                element.elementId,
                workbook.workbookId,
                len(str(table_ref).split(".")) if isinstance(table_ref, str) else -1,
                _CONNECTION_ID in source_node,
            )
        elif source_type == "customSQL":
            pass  # handled by _build_workbook_customsql_registry via the workbook-level lineage endpoint
        else:
            self._record_unknown_lineage_node(
                source_type=source_type,
                source_node=source_node,
                element=element,
                workbook=workbook,
            )

    def _record_unknown_lineage_node(
        self,
        *,
        source_type: Any,
        source_node: Dict,
        element: Element,
        workbook: Workbook,
    ) -> None:
        """A lineage node type this walk does not handle.

        Split out of ``_process_lineage_node`` so the dispatch there stays a
        flat list of node types.
        """
        warn_key = source_type if isinstance(source_type, str) else "<non-str>"
        # The warning fires once per type, so without this the report says a
        # type exists but never how much lineage it costs. A 'union' node
        # combines inputs the same way 'join' does, and every element behind one
        # loses its upstreams silently.
        self.report.workbook_lineage_node_types_unhandled[warn_key] = (
            self.report.workbook_lineage_node_types_unhandled.get(warn_key, 0) + 1
        )
        # Guarded: this fires per NODE, not per type, and ``key_skeleton``
        # walks the whole descriptor.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "UNKNOWN LINEAGE NODE type=%r element=%s workbook=%s: this "
                "node's upstreams are not walked, so anything behind it has no "
                "lineage at all. To handle it we need three things from this "
                "skeleton: whether the nodeId is an inode-<urlId>, whether it "
                "carries a name, and whether it has sources of its own (which "
                "would make it a pass-through like join/union rather than a "
                "leaf). Key skeleton (structure only, no values): %r",
                source_type,
                element.elementId,
                workbook.workbookId,
                # Recursive: a one-level view renders a nested descriptor as
                # just "list"/"dict" and hides the field that says what the
                # node points at.
                key_skeleton(source_node),
            )
        if warn_key not in self._unknown_lineage_node_types_warned:
            self._unknown_lineage_node_types_warned.add(warn_key)
            self.report.warning(
                title="Unknown Sigma lineage node type",
                message="Unknown Sigma lineage node type",
                context=(
                    f"type={source_type!r}, element={element.name}, "
                    f"workbook={workbook.name} (further occurrences of "
                    f"this type will be suppressed)"
                ),
            )

    def _get_element_upstream_sources(
        self, element: Element, workbook: Workbook
    ) -> Dict[str, ElementUpstream]:
        """Return upstream sources keyed by nodeId, admitting Sigma Dataset
        (``dataset``), intra-workbook element (``sheet``), data-model
        (``data-model``), and direct warehouse table (``table``) nodes. Walks
        through ``join`` pass-through nodes.

        BFS from the queried element's sheet node, following edges in
        reverse (target-to-source), so only reachable upstreams are
        captured (sibling edges in the payload do not leak in).
        """
        upstream_sources: Dict[str, ElementUpstream] = {}

        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/lineage/elements/{element.elementId}"
            )
            if response.status_code == 500:
                logger.debug(
                    f"Lineage metadata not present for element {element.name} of workbook '{workbook.name}'"
                )
                return upstream_sources
            if response.status_code == 403:
                logger.debug(
                    f"Lineage metadata not accessible for element {element.name} of workbook '{workbook.name}'"
                )
                return upstream_sources
            if response.status_code == 400:
                logger.debug(
                    f"Lineage not supported for element {element.name} of workbook '{workbook.name}' (400 Bad Request)"
                )
                return upstream_sources
            response.raise_for_status()
            response_dict = response.json()
        except requests.exceptions.RequestException as e:
            # Recorded HERE, not in the caller's try/except: this handler
            # returns normally, so nothing propagates for the caller to catch.
            # Counting only in the caller left the counter reading 0 on a
            # tenant where 200 elements failed with HTTP 409.
            self._record_element_fetch_failure(
                element_id=element.elementId,
                element_type=str(element.type),
                workbook_name=workbook.name,
                fetch="lineage",
                exc=e,
            )
            return {}

        try:
            dependencies = response_dict[Constant.DEPENDENCIES]

            # Reverse adjacency (target nodeId -> source nodeIds). A
            # malformed edge skips itself; others still populate.
            edges_by_target: Dict[str, List[str]] = {}
            for edge in response_dict[Constant.EDGES]:
                try:
                    edges_by_target.setdefault(edge[Constant.TARGET], []).append(
                        edge[Constant.SOURCE]
                    )
                except (KeyError, TypeError) as e:
                    self.report.warning(
                        message="Skipping malformed Sigma lineage edge",
                        context=f"edge={edge!r}, element={element.name}, workbook={workbook.name}",
                        exc=e,
                    )

            # Seeds: sheet nodes whose elementId matches the queried
            # element. Sigma typically returns one, but we BFS from all of
            # them defensively.
            seed_node_ids = [
                node_id
                for node_id, node_data in dependencies.items()
                if node_data.get(Constant.TYPE) == "sheet"
                and node_data.get(Constant.ELEMENTID) == element.elementId
            ]

            if not seed_node_ids:
                self.report.warning(
                    message="Could not find sheet node for element in lineage response",
                    context=f"element={element.name}, workbook={workbook.name}",
                )
                return {}

            if len(seed_node_ids) > 1:
                self.report.warning(
                    message="Multiple seed sheet nodes found for element in lineage response",
                    context=f"element={element.name}, workbook={workbook.name}, seed_count={len(seed_node_ids)}",
                )

            # BFS from all seeds, walking edges in reverse (target to source).
            visited: Set[str] = set(seed_node_ids)
            queue: Deque[str] = deque(seed_node_ids)

            while queue:
                current_id = queue.popleft()
                for source_node_id in edges_by_target.get(current_id, []):
                    if source_node_id in visited:
                        continue
                    visited.add(source_node_id)

                    # Workbook-to-DM-element reference: the node
                    # ``<dmUrlId>/<suffix>`` appears only as an edge source
                    # (not as a ``dependencies`` key). Synthesize the
                    # upstream from the edge. The edge carries no DM-side
                    # name, so we fall back to the workbook element's own
                    # name (Sigma's default mirrors the DM element name).
                    # A user rename degrades to
                    # ``element_dm_edge.name_unmatched_but_dm_known``.
                    if source_node_id not in dependencies and "/" in source_node_id:
                        dm_url_id, _, suffix = source_node_id.partition("/")
                        if dm_url_id and suffix:
                            try:
                                upstream_sources[source_node_id] = (
                                    DataModelElementUpstream(
                                        name=element.name,
                                        data_model_url_id=dm_url_id,
                                    )
                                )
                                self.report.element_dm_edge.synthesized_from_edge_only += 1
                            except ValidationError as e:
                                self.report.warning(
                                    title="Sigma DM upstream synthesis from edge-only node failed",
                                    message="Failed to synthesize Sigma DM upstream from edges-only node",
                                    context=(
                                        f"node={source_node_id}, element={element.name}, "
                                        f"workbook={workbook.name}"
                                    ),
                                    exc=e,
                                )
                            continue
                        # Malformed (empty prefix or suffix): fall through
                        # so the legacy dispatch surfaces a warning.

                    # Per-node isolation: a malformed node skips itself.
                    try:
                        source_node = dependencies[source_node_id]
                    except (KeyError, AttributeError, TypeError) as e:
                        self.report.warning(
                            title="Sigma lineage node parse failed",
                            message="Failed to parse Sigma lineage node",
                            context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                            exc=e,
                        )
                        continue

                    try:
                        self._process_lineage_node(
                            source_node_id,
                            source_node,
                            upstream_sources,
                            queue,
                            element,
                            workbook,
                        )
                    except (KeyError, AttributeError, TypeError, ValidationError) as e:
                        # Defence-in-depth; the helper already handles
                        # ValidationError internally.
                        self.report.warning(
                            title="Sigma lineage node parse failed",
                            message="Failed to parse Sigma lineage node",
                            context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                            exc=e,
                        )
        except (KeyError, AttributeError, TypeError, ValidationError) as e:
            # Structural errors in the setup phase (missing keys, malformed
            # edges, non-dict seed entries).
            self.report.warning(
                title="Sigma element lineage response parse failed",
                message="Failed to parse Sigma element lineage response",
                context=f"element={element.name}, workbook={workbook.name}",
                exc=e,
            )
            return {}

        return upstream_sources

    def _get_element_sql_query(
        self, element: Element, workbook: Workbook
    ) -> Optional[str]:
        try:
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/elements/{element.elementId}/query"
            )
            if response.status_code == 404:
                logger.debug(
                    f"Query not present for element {element.name} of workbook '{workbook.name}'"
                )
                return None
            response.raise_for_status()
            response_dict = response.json()
            if "sql" in response_dict:
                return response_dict["sql"]
        except Exception as e:
            self._record_element_fetch_failure(
                element_id=element.elementId,
                element_type=str(element.type),
                workbook_name=workbook.name,
                fetch="query",
                exc=e,
            )
            # report_warning=False: _record_element_fetch_failure above already
            # warned and counted this failure. _get_element_upstream_sources
            # does the same; the asymmetry here produced two report warnings
            # and two counters for one failed call.
            self._log_http_error(
                message=f"Unable to fetch sql query for element {element.name} of workbook '{workbook.name}'. Exception: {e}",
                report_warning=False,
            )
        return None

    def get_workbook_column_formulas(
        self, workbook_id: str
    ) -> Tuple[Dict[str, Dict[str, Optional[str]]], Dict[str, Dict[str, str]]]:
        """Fetch per-element column formulas from GET /workbooks/{id}/columns.

        Returns a tuple:
          - elementId -> {column_name: formula_or_None}
          - elementId -> {column_name: raw_columnId}

        The raw_columnId for warehouse-backed columns is "inode-{tableUrlId}/{NATIVE_NAME}";
        for DM-backed columns it is an opaque hash with no slash.

        The /columns endpoint is the authoritative source for column formulas
        in production; the page-elements endpoint returns columns as plain strings.
        If pagination aborts partway through, a report warning is emitted by
        _paginated_raw_entries and column_formulas_fetch_partial is incremented so
        the partial-data workbook is distinguishable from one with few formulas.
        """
        error_ctx = f"Unable to fetch column formulas for workbook {workbook_id}."
        aborts_before = self.report.pagination_aborted
        result: Dict[str, Dict[str, Optional[str]]] = {}
        col_ids: Dict[str, Dict[str, str]] = {}
        for col in self._paginated_raw_entries(
            f"{self.config.api_url}/workbooks/{workbook_id}/columns",
            error_ctx,
            silent_statuses=(404,),
        ):
            elem_id = col.get(Constant.ELEMENTID)
            name = col.get(Constant.NAME)
            formula: Optional[str] = col.get("formula") or None
            column_id: str = col.get("columnId") or ""
            if elem_id and name:
                result.setdefault(elem_id, {})[name] = formula
                if column_id:
                    col_ids.setdefault(elem_id, {})[name] = column_id
        if self.report.pagination_aborted > aborts_before:
            self.report.column_formulas_fetch_partial += 1
            # Recorded, not just counted. Without the id, a chart column from
            # this workbook is indistinguishable at emit time from one Sigma
            # genuinely reported no formula for, and the whole workbook lands
            # in ``chart_input_fields_self_ref_no_formula`` -- which reads as
            # "Sigma has nothing to give" when the truth is we never asked
            # successfully. On one tenant (2026-09) 12 workbooks aborted with
            # ZERO entries retrieved.
            self.column_formulas_incomplete_workbooks.add(workbook_id)
            logger.debug(
                "COLUMNS PARTIAL workbook %s: pagination aborted; %d element(s) "
                "carry formulas. Every chart column absent from this response "
                "falls back to a self-referential InputField, so this workbook's "
                "chart_input_fields_self_ref_* share is not evidence of a "
                "resolver defect.",
                workbook_id,
                len(result),
            )
        return result, col_ids

    def get_page_elements(
        self,
        workbook: Workbook,
        page: Page,
        column_formulas_by_element: Optional[
            Dict[str, Dict[str, Optional[str]]]
        ] = None,
        column_ids_by_element: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[Element]:
        try:
            elements: List[Element] = []
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages/{page.pageId}/elements"
            )
            response.raise_for_status()
            for i, element_dict in enumerate(response.json()[Constant.ENTRIES]):
                if element_dict.get("type") not in self.ingested_element_types:
                    # Skipped elements never enter the workbook element index, so
                    # any chart formula referencing one can never resolve and
                    # falls back to a self-reference. Log the elementId (always
                    # present) as well as the name, which is frequently absent
                    # here -- without it a self-ref miss cannot be matched
                    # against the element that caused it.
                    el_type = str(element_dict.get("type"))
                    self.report.workbook_elements_skipped_by_type[el_type] = (
                        self.report.workbook_elements_skipped_by_type.get(el_type, 0)
                        + 1
                    )
                    logger.debug(
                        "Skipping lineage and sql query extraction for element "
                        "name=%r elementId=%r of type %r of workbook %r",
                        element_dict.get("name"),
                        element_dict.get(Constant.ELEMENTID),
                        el_type,
                        workbook.name,
                    )
                    continue

                if not element_dict.get(Constant.NAME):
                    element_dict[Constant.NAME] = (
                        f"Element {i + 1} of Page '{page.name}'"
                    )
                element_dict[Constant.URL] = (
                    f"{workbook.url}?:nodeId={element_dict[Constant.ELEMENTID]}&:fullScreen=true"
                )
                element = Element.model_validate(element_dict)
                if column_formulas_by_element is not None:
                    element.column_formulas = column_formulas_by_element.get(
                        element.elementId, {}
                    )
                if column_ids_by_element is not None:
                    element.column_id_by_name = column_ids_by_element.get(
                        element.elementId, {}
                    )
                if (
                    self.config.extract_lineage
                    and self.config.workbook_lineage_pattern.allowed(workbook.name)
                ):
                    # Scoped to this element on purpose. These two calls are the
                    # only per-element network work here, and an escaping
                    # exception would be caught by the page-level handler below,
                    # which returns [] -- silently dropping EVERY element on the
                    # page, including the ones that fetched cleanly. Losing one
                    # element's lineage is the correct blast radius.
                    # Two separate blocks: a lineage failure must not also
                    # cost the SQL query, which is an independent call that may
                    # well have succeeded.
                    #
                    # Both are backstops. Each fetcher handles its own HTTP
                    # errors and returns empty, so in practice nothing reaches
                    # these handlers -- which is exactly why the failure counter
                    # is recorded inside the fetchers too.
                    el_type = str(element_dict.get("type"))
                    try:
                        element.upstream_sources = self._get_element_upstream_sources(
                            element, workbook
                        )
                    except Exception as e:
                        self._record_element_fetch_failure(
                            element_id=element.elementId,
                            element_type=el_type,
                            workbook_name=workbook.name,
                            fetch="lineage",
                            exc=e,
                        )
                    try:
                        element.query = self._get_element_sql_query(element, workbook)
                    except Exception as e:
                        self._record_element_fetch_failure(
                            element_id=element.elementId,
                            element_type=el_type,
                            workbook_name=workbook.name,
                            fetch="query",
                            exc=e,
                        )
                elements.append(element)
            return elements
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch elements of page '{page.name}', workbook '{workbook.name}'. Exception: {e}"
            )
            return []

    def get_workbook_pages(self, workbook: Workbook) -> List[Page]:
        try:
            pages: List[Page] = []
            column_formulas_by_element: Optional[
                Dict[str, Dict[str, Optional[str]]]
            ] = None
            column_ids_by_element: Optional[Dict[str, Dict[str, str]]] = None
            if (
                self.config.extract_lineage
                and self.config.workbook_lineage_pattern.allowed(workbook.name)
            ):
                column_formulas_by_element, column_ids_by_element = (
                    self.get_workbook_column_formulas(workbook.workbookId)
                )
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages"
            )
            response.raise_for_status()
            for page_dict in response.json()[Constant.ENTRIES]:
                page = Page.model_validate(page_dict)
                page.elements = self.get_page_elements(
                    workbook,
                    page,
                    column_formulas_by_element=column_formulas_by_element,
                    column_ids_by_element=column_ids_by_element,
                )
                pages.append(page)
            return pages
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch pages of workbook '{workbook.name}'. Exception: {e}"
            )
            return []

    def _paginated_raw_entries(
        self,
        base_url: str,
        error_ctx: str,
        silent_statuses: Tuple[int, ...] = (),
    ) -> List[Dict[str, Any]]:
        """Page through a Sigma list endpoint and return raw ``entries``
        dicts. Handles both pagination shapes (``nextPage`` and
        ``nextPageToken``) and guards against pathological proxies that
        return the same cursor twice in a row (which would otherwise loop
        forever and accumulate duplicates). HTTP/JSON errors abort
        pagination and surface a report warning; partial results
        collected before the failure are preserved. ``silent_statuses``
        lets callers treat expected "no data" statuses (e.g. 404 from
        Sigma's /lineage on empty DMs) as an empty list without emitting
        a warning -- applied only to the first page so later-page
        failures still surface.
        """
        # Use ``&`` as the separator if the base URL already has query
        # params (e.g. an ``api_url`` routed through a proxy), so
        # pagination does not collide with existing params.
        separator = "&" if "?" in base_url else "?"
        url = base_url
        raw_entries: List[Dict[str, Any]] = []
        # Cycle protection: a broken proxy (or caching layer) can echo the
        # same ``nextPage`` / ``nextPageToken`` back on every call. Track
        # (kind, value) tuples so a cycle that crosses cursor types is also
        # detected (e.g. page=1 → nextPageToken=1 → page=1 repeating).
        seen_cursors: Set[Tuple[str, str]] = set()
        first_page = True
        pages_read = 0
        # Sigma reports the full row count on each page. Comparing against it
        # is the only reliable truncation test: a caller guessing from round
        # numbers misses a 5,000 cap and false-positives on a tenant that
        # genuinely has exactly 10,000 rows.
        reported_total: Optional[int] = None
        try:
            while True:
                response = self._get_api_call(url)
                # Swallow expected "no data" statuses before raise_for_status.
                if first_page and response.status_code in silent_statuses:
                    logger.debug(
                        f"{error_ctx} Swallowed expected status "
                        f"{response.status_code} on first page."
                    )
                    return raw_entries
                first_page = False
                response.raise_for_status()
                pages_read += 1
                response_dict = response.json()
                for entry in response_dict.get(Constant.ENTRIES, []):
                    if isinstance(entry, dict):
                        raw_entries.append(entry)
                logger.debug(
                    "PAGE %s: +%d entries (running total %d) reported_total=%s",
                    error_ctx,
                    len(response_dict.get(Constant.ENTRIES, []) or []),
                    len(raw_entries),
                    response_dict.get("total"),
                )
                raw_total = response_dict.get("total")
                if isinstance(raw_total, int):
                    reported_total = raw_total
                next_page = response_dict.get(Constant.NEXTPAGE)
                next_token = response_dict.get(Constant.NEXTPAGETOKEN)
                if next_page:
                    cursor_key: Tuple[str, str] = ("page", str(next_page))
                    cursor = urlencode({"page": next_page})
                elif next_token:
                    cursor_key = ("nextPageToken", str(next_token))
                    cursor = urlencode({"nextPageToken": next_token})
                else:
                    break
                if cursor_key in seen_cursors:
                    self.report.pagination_aborted += 1
                    self.report.warning(
                        message="Pagination cursor repeated; aborting",
                        context=f"{error_ctx} url={base_url}, cursor={cursor}, "
                        f"entries_so_far={len(raw_entries)}",
                    )
                    break
                seen_cursors.add(cursor_key)
                url = f"{base_url}{separator}{cursor}"
            if reported_total is not None and len(raw_entries) < reported_total:
                self.report.pagination_short_of_reported_total[error_ctx] = (
                    reported_total - len(raw_entries)
                )
                self.report.warning(
                    title="Sigma paginated endpoint returned fewer rows than it reported",
                    message="The endpoint's own ``total`` exceeds the rows "
                    "pagination actually returned, so this listing is "
                    "incomplete and anything resolved from it may be missing "
                    "entries. See pagination_short_of_reported_total.",
                    context=(
                        f"endpoint={error_ctx}, returned={len(raw_entries)}, "
                        f"reported_total={reported_total}"
                    ),
                )
            return raw_entries
        except Exception as e:
            # Surface HTTP/JSON pagination failures so the operator sees
            # them in the ingestion report; ``_log_http_error`` alone is
            # debug-level and would leave the DM looking healthy while its
            # elements/columns are silently missing. Partial results
            # collected before the failure are preserved.
            # HTTP status goes into ``context`` (not ``title``) so LossyList
            # groups all pagination aborts under one stable key regardless
            # of status code.
            http_status: Optional[int] = (
                e.response.status_code
                if isinstance(e, requests.HTTPError) and e.response is not None
                else None
            )
            self.report.pagination_aborted += 1
            self.report.warning(
                title="Sigma paginated endpoint aborted",
                message="Pagination aborted; partial results preserved.",
                context=(
                    f"endpoint={error_ctx}, url={url}, "
                    f"partial_results={len(raw_entries)}, "
                    # Which page died separates "the endpoint refuses this
                    # object outright" from "it served rows and then stopped",
                    # which need different fixes.
                    f"pages_read={pages_read}"
                    + (f", http_status={http_status}" if http_status else "")
                    + (f", body={_error_body(getattr(e, 'response', None))}")
                ),
                exc=e,
            )
            self._log_http_error(
                message=f"{error_ctx} Exception: {e}", report_warning=False
            )
            return raw_entries

    # Cap per-endpoint malformed-entry warnings so a vendor regression
    # that breaks every row cannot flood the report with thousands of
    # identical warnings. Total dropped count is tracked via the
    # ``pagination_malformed_entries_dropped`` counter.
    _MAX_MALFORMED_WARNINGS_PER_ENDPOINT: int = 10

    def _paginated_entries(
        self,
        base_url: str,
        model_cls: Type[T],
        error_ctx: str,
        dedup_key: Optional[Callable[[T], Hashable]] = None,
    ) -> List[T]:
        """Page through a Sigma list endpoint, parsing each entry into
        ``model_cls``. Shares pagination / cycle-protection logic with
        :meth:`_paginated_raw_entries`. Per-entry ``ValidationError``
        drops only that entry (so one malformed row cannot empty the
        whole list). ``dedup_key`` lets callers collapse duplicates by
        a natural key so an echoed pagination cursor (or server-side
        overlap between pages) cannot leak duplicate aspects downstream
        -- matters because the same element/column emitted twice
        double-counts counters and re-upserts the same aspect for the
        same URN.
        """
        results: List[T] = []
        seen_keys: Set[Hashable] = set()
        malformed_warned = 0
        for entry in self._paginated_raw_entries(base_url, error_ctx):
            try:
                parsed = model_cls.model_validate(entry)
            except ValidationError as ve:
                entry_type = entry.get("type") if isinstance(entry, dict) else None
                if entry_type in _NON_DATA_ELEMENT_TYPES:
                    # Not malformed: Sigma returns layout elements with no name
                    # alongside real ones. Counting them as malformed buried the
                    # signal that a REAL entry failed to parse.
                    self.report.non_data_elements_skipped[str(entry_type)] = (
                        self.report.non_data_elements_skipped.get(str(entry_type), 0)
                        + 1
                    )
                    logger.debug(
                        "%s Skipping non-data element of type %r (no name; not a "
                        "parse failure).",
                        error_ctx,
                        entry_type,
                    )
                    continue
                self.report.pagination_malformed_entries_dropped += 1
                if malformed_warned < self._MAX_MALFORMED_WARNINGS_PER_ENDPOINT:
                    self.report.warning(
                        message="Dropped malformed entry",
                        context=f"{error_ctx} entry={entry!r}",
                        exc=ve,
                    )
                    malformed_warned += 1
                continue
            if dedup_key is not None:
                key = dedup_key(parsed)
                if key in seen_keys:
                    self.report.pagination_duplicate_entries_dropped += 1
                    continue
                seen_keys.add(key)
            results.append(parsed)
        return results

    def _get_data_model_elements(
        self, data_model_id: str
    ) -> List[SigmaDataModelElement]:
        logger.debug(f"Fetching elements for data model '{data_model_id}'.")
        return self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/elements",
            SigmaDataModelElement,
            f"Unable to fetch elements for data model '{data_model_id}'.",
            dedup_key=lambda element: element.elementId,
        )

    def _get_data_model_columns(self, data_model_id: str) -> List[SigmaDataModelColumn]:
        logger.debug(f"Fetching columns for data model '{data_model_id}'.")
        aborts_before = self.report.pagination_aborted
        columns = self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/columns",
            SigmaDataModelColumn,
            f"Unable to fetch columns for data model '{data_model_id}'.",
            # Dedup by (elementId, columnId): Sigma reuses warehouse-native
            # columnIds (e.g. CUSTOMER_ID) across customSQL elements that share
            # a warehouse passthrough column. Keying on columnId alone silently
            # drops all but the first occurrence, removing real passthrough
            # columns from consumer elements' schemaMetadata.
            dedup_key=lambda column: (column.elementId, column.columnId),
        )
        if self.report.pagination_aborted > aborts_before:
            self.report.data_model_columns_fetch_partial += 1
            logger.debug(
                "COLUMNS PARTIAL DM %s: pagination aborted with %d column(s) "
                "recovered. /columns is the ONLY source of formulas and "
                "columnIds, so every element in this Data Model loses column "
                "lineage it would otherwise have -- read this before treating "
                "the model's empty FGL as a resolver failure.",
                data_model_id,
                len(columns),
            )
        return columns

    def _get_data_model_lineage_entries(
        self, data_model_id: str
    ) -> List[Dict[str, Any]]:
        """Return raw entries from the DM /lineage endpoint. ``element``
        entries carry ``elementId`` + ``sourceIds`` (intra-DM elementIds
        or ``inode-<suffix>`` for external upstreams); ``dataset`` /
        ``table`` entries carry ``inodeId``. Paginated via
        :meth:`_paginated_raw_entries` so large DMs whose lineage spans
        multiple pages are not silently truncated. Sigma returns
        400/403/404 for DMs with no lineage graph (empty DMs or
        permission-scoped views); those are swallowed silently. 5xx
        responses are *not* in ``silent_statuses`` -- a globally
        degraded Sigma region would otherwise produce zero lineage
        aspects with zero warnings, which is worse than a loud
        warning per affected DM.

        Raw entries are deduped by a shape-aware natural key
        (``elementId`` for elements, ``inodeId`` for dataset/table,
        ``nodeId`` as a last-ditch fallback for any future shape) to
        defuse the same cursor-echo / pagination-overlap concern
        :meth:`_paginated_entries` guards against for typed models.
        Picking the *wrong* key silently collapses multiple real rows
        into one (e.g. keying elements on an absent ``nodeId`` would
        discard every element after the first), so this function is
        intentionally conservative: entries whose expected identifier
        is missing are preserved, not dropped.
        """
        logger.debug(f"Fetching lineage for data model '{data_model_id}'.")
        raw = self._paginated_raw_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/lineage",
            f"Unable to fetch lineage for data model '{data_model_id}'.",
            silent_statuses=(400, 403, 404),
        )
        deduped: List[Dict[str, Any]] = []
        # Map natural key -> index into ``deduped`` so we can merge on
        # collision rather than silently drop the second occurrence.
        # For ``element`` rows, merging means union of ``sourceIds``;
        # for ``dataset`` / ``table`` rows it's a no-op (there is no
        # payload field we'd want to accumulate across duplicates).
        # The union-on-collision shape handles:
        #   (a) a proxy that echoes the same page cursor -- the two
        #       rows are identical so the union collapses to the same
        #       set.
        #   (b) a future Sigma-side API change that splits one
        #       element's lineage across multiple rows (e.g. versioned
        #       upstreams) -- we accumulate rather than losing the
        #       trailing rows' ``sourceIds``.
        seen_index: Dict[Tuple[str, str], int] = {}
        for entry in raw:
            entry_type = str(entry.get(Constant.TYPE, ""))
            if entry_type == "element":
                identifier = str(entry.get(Constant.ELEMENTID, ""))
            elif entry_type in ("dataset", "table"):
                identifier = str(entry.get("inodeId", ""))
            else:
                identifier = str(entry.get("nodeId", ""))
            # Preserve entries whose natural identifier is absent rather
            # than collapsing them under the same empty-string key --
            # the point of dedup is cursor echo, not data loss.
            if not entry_type or not identifier:
                deduped.append(entry)
                continue
            key = (entry_type, identifier)
            existing_idx = seen_index.get(key)
            if existing_idx is None:
                seen_index[key] = len(deduped)
                deduped.append(entry)
                continue
            self.report.pagination_duplicate_entries_dropped += 1
            if entry_type == "element":
                existing = deduped[existing_idx]
                existing_sources = existing.get("sourceIds") or []
                new_sources = entry.get("sourceIds") or []
                if not isinstance(existing_sources, list) or not isinstance(
                    new_sources, list
                ):
                    # Vendor shape drift: don't attempt a merge on a
                    # non-list sourceIds -- the enclosing
                    # ``_assemble_data_model`` defensively validates
                    # per-element and will simply see the already-stored
                    # row.
                    continue
                # Preserve first-seen order while unioning the second
                # row's additions. Stringify-and-dedupe matches the
                # invariant enforced downstream by
                # ``_assemble_data_model`` (non-string sourceIds are
                # filtered out there anyway).
                seen_sources: Set[str] = {
                    s for s in existing_sources if isinstance(s, str)
                }
                merged = list(existing_sources)
                for s in new_sources:
                    if isinstance(s, str) and s not in seen_sources:
                        merged.append(s)
                        seen_sources.add(s)
                existing["sourceIds"] = merged
        return deduped

    def get_workbook_lineage_entries(self, workbook_id: str) -> List[Dict[str, Any]]:
        """Return raw entries from GET /v2/workbooks/{id}/lineage.

        ``customSQL`` entries carry the SQL definition; ``element`` entries
        carry ``elementId`` + ``sourceIds`` pointing at customSQL names.

        Sigma returns 400 (not 404) for workbooks that have no lineage graph at
        all (empirically observed — workbooks whose only sources are non-SQL
        warehouse tables never have a /lineage endpoint and return 400).
        403/404 cover permission-scoped views and deleted workbooks.  5xx is
        intentionally *not* silenced: a degraded Sigma API would otherwise
        produce zero lineage aspects with zero warnings.
        """
        logger.debug(f"Fetching lineage for workbook '{workbook_id}'.")
        return self._paginated_raw_entries(
            f"{self.config.api_url}/workbooks/{workbook_id}/lineage",
            f"Unable to fetch lineage for workbook '{workbook_id}'.",
            silent_statuses=(400, 403, 404),
        )

    def _assemble_data_model(
        self,
        data_model: SigmaDataModel,
        file_meta: Optional[File],
        resolved_workspace_id: Optional[str] = None,
    ) -> None:
        """Fetch and attach elements, per-element columns, and per-element
        sourceIds. If ``resolved_workspace_id`` is provided by the caller,
        it overrides whatever the ``/dataModels`` payload or ``/files``
        row reported, ensuring filtering (done by the caller) and
        rendering (done below) agree on a single workspace per DM.
        """
        if resolved_workspace_id is not None:
            # Overwrite unconditionally: ``get_data_models`` already
            # resolved ``(file_meta.workspaceId, data_model.workspaceId)``
            # into a single "authoritative" workspace with /files
            # preferred. Leaving the original ``data_model.workspaceId``
            # intact would produce DMs filtered under workspace B but
            # rendered / counted under workspace A when the two
            # disagree (e.g. a DM moved across workspaces, or a
            # Sigma-side inconsistency between the two endpoints).
            data_model.workspaceId = resolved_workspace_id
        if file_meta is not None:
            # These secondary fields are /files-authoritative (the folder
            # tree); fall back to them only when the /dataModels payload
            # did not carry a value.
            if data_model.path is None:
                data_model.path = file_meta.path
            if data_model.badge is None:
                data_model.badge = file_meta.badge
            if data_model.urlId is None and file_meta.urlId:
                data_model.urlId = file_meta.urlId

        elements = self._get_data_model_elements(data_model.dataModelId)
        columns = self._get_data_model_columns(data_model.dataModelId)
        # ``extract_lineage=False`` is a historical opt-out for the
        # privileged ``/workbooks/{id}/lineage`` surface; users who set
        # it don't expect the connector to call *any* ``/lineage``
        # endpoint. Honor that here so ``ingest_data_models=True +
        # extract_lineage=False`` emits DM Containers + element Datasets
        # + ``SchemaMetadata`` (the "catalog without upstreams" shape),
        # rather than silently reaching for the same lineage surface
        # under a different flag.
        if self.config.extract_lineage:
            lineage_entries = self._get_data_model_lineage_entries(
                data_model.dataModelId
            )
        else:
            lineage_entries = []

        columns_by_element: Dict[str, List[SigmaDataModelColumn]] = {}
        for column in columns:
            if column.elementId is None:
                # DM-global calculations: no element to attach to.
                self.report.data_model_columns_without_element_dropped += 1
                continue
            columns_by_element.setdefault(column.elementId, []).append(column)

        # Reset before populating so repeated _assemble_data_model calls
        # (e.g. during alias discovery) cannot accumulate stale entries.
        data_model.source_dm_element_names = {}
        data_model.warehouse_inodes_by_inode_id = {}
        data_model.custom_sql_by_name = {}
        source_ids_by_element: Dict[str, List[str]] = {}
        for entry in lineage_entries:
            entry_type = entry.get(Constant.TYPE)
            if entry_type == "element":
                element_id = entry.get(Constant.ELEMENTID)
                source_ids = entry.get("sourceIds") or []
                if element_id and isinstance(source_ids, list):
                    source_ids_by_element[element_id] = [
                        s for s in source_ids if isinstance(s, str)
                    ]
            elif entry_type == "data-model":
                # Each ``data-model`` entry names the specific element consumed
                # from a source DM.  Stash by source dataModelId so
                # ``_resolve_dm_element_cross_dm_upstream`` can look up the
                # correct source element name without relying on the consuming
                # element sharing that name.
                src_dm_id = entry.get("dataModelId")
                src_name = entry.get("name")
                if (
                    isinstance(src_dm_id, str)
                    and src_dm_id
                    and isinstance(src_name, str)
                    and src_name.strip()
                ):
                    data_model.source_dm_element_names.setdefault(src_dm_id, []).append(
                        src_name.strip()
                    )
            elif entry_type == "table":
                # Stash raw warehouse-table nodes keyed by inodeId so
                # SigmaSource can call /files/{inodeId} for the urlId + path
                # needed to construct fully-qualified warehouse Dataset URNs.
                inode_id = str(entry.get("inodeId") or "")
                conn_id = str(entry.get("connectionId") or "")
                if not (inode_id and conn_id):
                    self.report.dm_element_warehouse_table_entry_incomplete += 1
                    missing = [
                        f
                        for f, v in (
                            ("inodeId", inode_id),
                            ("connectionId", conn_id),
                        )
                        if not v
                    ]
                    self.report.warning(
                        title="Sigma type=table lineage entry is incomplete",
                        message=(
                            "A type=table lineage entry is missing inodeId or "
                            "connectionId. Warehouse upstream skipped."
                        ),
                        context=(
                            f"dm={data_model.dataModelId}, missing_fields={missing}"
                        ),
                    )
                    continue
                raw: WarehouseInodeRaw = {"connectionId": conn_id}
                data_model.warehouse_inodes_by_inode_id[inode_id] = raw
            elif entry_type in ("customSQL", "customSql"):
                name = entry.get("name")
                if isinstance(name, str) and name:
                    # Sigma's API normally guarantees unique entry names within
                    # a DM.  A collision here means a payload anomaly; last
                    # entry wins so downstream elements still resolve (with a
                    # warning so operators can investigate).
                    if name in data_model.custom_sql_by_name:
                        self.report.warning(
                            title="Sigma DM customSQL duplicate entry name",
                            message="Two customSQL lineage entries share the same name; later entry overwrites earlier — elements sourcing the earlier entry will get the wrong SQL.",
                            context=f"dataModelId={data_model.dataModelId!r}, name={name!r}",
                        )
                    data_model.custom_sql_by_name[name] = CustomSqlEntry.model_validate(
                        entry
                    )
                else:
                    self.report.warning(
                        title="Sigma DM customSQL entry missing name",
                        message="A customSQL lineage entry has a missing or non-string name field; it will be skipped and any elements referencing it will have no warehouse lineage.",
                        context=f"dataModelId={data_model.dataModelId!r}, entry_keys={sorted(entry.keys())!r}",
                    )
            # ``type: dataset`` entries (CSV uploads) are terminal.

        self._log_dm_lineage_shape(data_model, lineage_entries)

        for element in elements:
            element.columns = columns_by_element.get(element.elementId, [])
            element.source_ids = source_ids_by_element.get(element.elementId, [])
            logger.debug(
                "DM ELEMENT ASSEMBLED %s/%s %r: type=%r columns=%d source_ids=%r",
                data_model.dataModelId,
                element.elementId,
                element.name,
                element.type,
                len(element.columns),
                element.source_ids,
            )

        data_model.elements = elements

    @staticmethod
    def _log_dm_lineage_shape(
        data_model: SigmaDataModel, lineage_entries: List[Dict[str, Any]]
    ) -> None:
        """Classify a Data Model's /lineage payload by entry type.

        Decisive for "why does this element have no warehouse column lineage":
        the warehouse url_id map is built ONLY from type=table rows, so a Data
        Model reporting none can never resolve an inode-shaped columnId no
        matter what its columns say.
        """
        entry_types: Dict[str, int] = {}
        for entry in lineage_entries:
            key = str(entry.get(Constant.TYPE))
            entry_types[key] = entry_types.get(key, 0) + 1
        logger.debug(
            "DM LINEAGE %s: %d entries by type=%r; table inodes stashed=%d; "
            "customSQL names=%d; source-DM names=%d",
            data_model.dataModelId,
            len(lineage_entries),
            entry_types,
            len(data_model.warehouse_inodes_by_inode_id),
            len(data_model.custom_sql_by_name),
            len(data_model.source_dm_element_names),
        )

    def list_warehouse_table_files(self) -> List[Dict[str, Any]]:
        """List every ``type=table`` file, for the by-NAME warehouse index.

        Used only by the name-based fallback: a formula can reference a
        warehouse table by name that neither the element's ``source_ids`` nor
        its Data Model's ``/lineage`` ever mentions, and a name is the only
        signal left to resolve it by.

        Deliberately NOT used for url_id resolution. Measured on a live tenant (2026-09),
        this listing costs ~41 paged calls and answered none of the 37
        unresolved url_ids; ``get_file_metadata_by_url_id`` answers those in one
        call each and distinguishes "absent from the Data Model's lineage" from
        "deleted from Sigma". Each entry carries ``id``, ``urlId``, ``name`` and
        ``path`` together, so no per-inode follow-up call is needed.
        """
        entries = self._paginated_raw_entries(
            f"{self.config.api_url}/files?typeFilters=table&limit=1000",
            "Unable to list warehouse table files.",
        )
        logger.debug(
            "FILES LISTING: /v2/files?typeFilters=table returned %d entries; "
            "%d carry a urlId, %d carry a path",
            len(entries),
            sum(1 for e in entries if e.get("urlId")),
            sum(1 for e in entries if e.get("path")),
        )
        return entries

    def get_file_metadata_by_url_id(self, url_id: str) -> Optional[Dict[str, Any]]:
        """Fetch ``/v2/files/{urlId}``, or None when it cannot be resolved.

        ``/v2/files/{id}`` accepts either an inodeId (UUID) or a urlId and
        returns the same document for both. That matters because a Data Model
        element's ``inode-<suffix>`` carries the urlId, not the UUID, so this is
        the only way to ask about a table the Data Model's own ``/lineage``
        never described -- and it resolves tables the tenant-wide table listing
        omits.

        A 404 returns None WITHOUT a warning. It means the token cannot resolve
        that url_id, which may be a deleted file or one outside the credential's
        visibility; the two are indistinguishable from here, so the caller only
        counts it.
        """
        url = f"{self.config.api_url}/files/{quote(url_id, safe='')}"
        try:
            response = self._get_api_call(url)
            if response.status_code == 200:
                return response.json()
            if response.status_code != 404:
                self.report.warning(
                    title="Sigma /files lookup by urlId returned non-200",
                    message=(
                        "Could not resolve a warehouse table referenced by a "
                        "Data Model element. Column lineage to that table is "
                        "skipped."
                    ),
                    context=f"url_id={url_id}, http_status={response.status_code}",
                )
            return None
        except Exception as e:
            self.report.warning(
                title="Sigma /files lookup by urlId failed",
                message="Exception resolving a warehouse table by urlId.",
                context=f"url_id={url_id}",
                exc=e,
            )
            return None

    def get_data_model_spec(self, data_model_id: str) -> Optional[Dict[str, Any]]:
        """Fetch ``/v2/dataModels/{id}/spec``, the Data Model's authoring document.

        This is the only endpoint that describes a JOIN's predicate or a
        UNION's branch pairing. Neither ``/elements`` nor ``/columns`` nor
        ``/lineage`` carries either, so a join's output column can only be
        linked to the side its formula names and a union's only to one branch --
        every other upstream is invisible without this call.

        Response shape, confirmed on a live tenant (2026-09)::

            {"kind": "data-model", "pages": [{"elements": [
                {"id": ..., "kind": "table", "order": [columnId, ...],
                 "columns": [{"id": ..., "formula": ...}],
                 "source": {...}}]}]}

        ``source.kind`` observed as ``warehouse-table``, ``table``, ``join``,
        ``data-model``, ``union`` and ``sql``. :func:`parse_data_model_spec`
        holds the per-kind shapes it reads and logs a structural skeleton for
        any it does not.

        Returns the raw document, or None on non-200 / exception -- a Data Model
        whose spec is unavailable simply gets no join-key or union lineage.
        """
        logger.debug("Fetching spec for data model '%s'.", data_model_id)
        url = f"{self.config.api_url}/dataModels/{quote(data_model_id, safe='')}/spec"
        try:
            response = self._get_api_call(url)
            if response.status_code == 200:
                return response.json()
            self.report.data_model_spec_fetch_failed += 1
            self._warn_spec_unavailable(
                data_model_id=data_model_id,
                detail=f"http_status={response.status_code}",
            )
            return None
        except Exception as e:
            self.report.data_model_spec_fetch_failed += 1
            self._warn_spec_unavailable(
                data_model_id=data_model_id, detail=f"error={type(e).__name__}"
            )
            return None

    def _record_element_fetch_failure(
        self,
        *,
        element_id: str,
        element_type: str,
        workbook_name: str,
        fetch: str,
        exc: Exception,
    ) -> None:
        """Count and report one per-element fetch failure.

        Must be called from the handler that actually catches the error.
        ``_get_element_upstream_sources`` and ``_get_element_sql_query`` both
        swallow HTTP failures and return empty, so an increment placed only in
        their caller's ``except`` never runs: on one tenant (2026-09) 200 elements failed
        with HTTP 409 while this counter read 0, making the report claim every
        element had been fetched cleanly.
        """
        self.report.workbook_element_lineage_fetch_failed += 1
        self._warn_element_fetch_failed(
            element_id=element_id,
            element_type=element_type,
            workbook_name=workbook_name,
            fetch=fetch,
            exc=exc,
        )

    def _warn_element_fetch_failed(
        self,
        *,
        element_id: str,
        element_type: str,
        workbook_name: str,
        fetch: str,
        exc: Exception,
    ) -> None:
        """Report a per-element fetch failure once per run.

        A cause that affects one element rarely affects only one -- an expired
        token fails every remaining element on the tenant -- so an
        un-deduplicated warning would bury the report under thousands of copies
        while the counter already carries the true magnitude.
        """
        if self._element_fetch_failed_warned:
            logger.debug(
                "Element %s (%s) %s fetch failed: %s. Warning already reported "
                "once this run; see workbook_element_lineage_fetch_failed.",
                element_id,
                element_type,
                fetch,
                exc,
            )
            return
        self._element_fetch_failed_warned = True
        self.report.warning(
            title="Sigma element lineage fetch failed",
            message=(
                "Lineage or SQL query could not be fetched for a workbook "
                "element. Affected elements are still emitted, without their "
                "upstream edges; other elements on the page are unaffected. "
                "See workbook_element_lineage_fetch_failed for how many."
            ),
            context=(
                f"first_failure: element={element_id}, type={element_type!r}, "
                f"fetch={fetch}, workbook={workbook_name}"
            ),
            exc=exc,
        )

    def _warn_spec_unavailable(self, *, data_model_id: str, detail: str) -> None:
        """Report a /spec failure once per run, not once per Data Model.

        A token without the data model read scope fails for EVERY model, so an
        un-deduplicated warning would bury the report under hundreds of copies
        of the same fact. The counter keeps the true magnitude.
        """
        if self._spec_unavailable_warned:
            logger.debug(
                "Data model spec unavailable for '%s' (%s); warning already "
                "reported once this run.",
                data_model_id,
                detail,
            )
            return
        self._spec_unavailable_warned = True
        self.report.warning(
            title="Sigma data model spec unavailable",
            message=(
                "Could not fetch the Data Model authoring spec, which is the "
                "only source of JOIN key columns. Column lineage for affected "
                "models will link only to the side each formula names. A 403 "
                "usually means the API token lacks the data model read scope. "
                "See data_model_spec_fetch_failed for how many models this hit."
            ),
            context=f"first_failure: data_model_id={data_model_id}, {detail}",
        )

    def get_file_metadata(self, inode_id: str) -> Optional[Dict[str, Any]]:
        """Fetch /files/{inodeId} and return the raw JSON dict, or None on
        non-200 or exception.  Resolves a warehouse-table lineage ``inodeId``
        (UUID) to its ``urlId`` (alphanumeric slug) and file-system ``path``
        (``Connection Root/<DB>/<SCHEMA>`` for Snowflake; shape for other
        platforms is unverified — see TODO in _build_dm_warehouse_url_id_map).

        Callers are responsible for caching; this method always makes a live
        HTTP call so the instance-level cache on ``SigmaSource`` can be shared
        across multiple callers without duplicating retry/error logic here.

        Error handling mirrors ``get_data_model_by_url_id``: 429 gets a
        dedicated counter + warning; other non-200 statuses and exceptions
        emit a rate-limited structured warning so operators can distinguish
        rate-limiting from missing-scope (403/404) from server errors (5xx).
        """
        logger.debug("Fetching file metadata for inode '%s'.", inode_id)
        url = f"{self.config.api_url}/files/{quote(inode_id, safe='')}"
        try:
            response = self._get_api_call(url)
            if response.status_code == 200:
                return response.json()
            status = response.status_code
            if status == 429:
                self.report.dm_element_warehouse_table_lookup_rate_limited += 1
                self.report.warning(
                    title="Sigma API rate-limited on /files lookup",
                    message=(
                        "Retry budget exhausted on a 429 response for a /files inode lookup. "
                        "Warehouse upstream will be missing for this inode. "
                        "Re-run the ingestion to recover."
                    ),
                    context=f"inode_id={inode_id}, http_status={status}",
                )
            else:
                self.report.warning(
                    title="Sigma /files lookup returned non-200",
                    message=(
                        "Unable to resolve warehouse table metadata for an inode. "
                        "Warehouse upstream will be missing."
                    ),
                    context=f"inode_id={inode_id}, http_status={status}",
                )
            return None
        except Exception as e:
            self.report.warning(
                title="Sigma /files lookup failed",
                message="Exception while fetching file metadata for an inode; warehouse upstream skipped.",
                context=f"inode_id={inode_id}",
                exc=e,
            )
            return None

    def get_workbook_lineage(
        self, workbook_id: str
    ) -> Optional[List[WorkbookLineageTableEntry]]:
        """Fetch /v2/workbooks/{workbook_id}/lineage and return parsed type=table
        entries, or None on non-200/exception.

        Non-table entries (type=dataset/customSQL/element) are silently skipped.
        Table entries missing required fields emit a structured warning and are
        skipped; they do not cause the whole call to fail.

        Error handling: 404 is treated as a silent None (workbook deleted
        since listing). 429 and other non-200 statuses emit a structured
        warning; failures return None so the caller can increment
        chart_input_fields_warehouse_index_lookup_failed. Paginated to
        handle workbooks with large lineage graphs.
        """
        logger.debug("Fetching workbook lineage for workbook '%s'.", workbook_id)
        base_url = (
            f"{self.config.api_url}/workbooks/{quote(workbook_id, safe='')}/lineage"
        )
        all_entries: List[WorkbookLineageTableEntry] = []
        url = base_url
        try:
            while True:
                response = self._get_api_call(url)
                if response.status_code == 200:
                    data = response.json()
                    for raw in data.get("entries") or []:
                        if raw.get("type") != "table":
                            logger.debug(
                                "Workbook %s: skipping lineage entry with type %r.",
                                workbook_id,
                                raw.get("type"),
                            )
                            continue
                        try:
                            all_entries.append(
                                WorkbookLineageTableEntry.model_validate(raw)
                            )
                        except ValidationError:
                            self.report.warning(
                                title="Sigma workbook lineage type=table entry missing required fields",
                                message=(
                                    "A type=table lineage entry is missing one or more "
                                    "required fields (name, connectionId, inodeId). "
                                    "Warehouse table index entry skipped."
                                ),
                                context=f"workbook_id={workbook_id}, entry={raw}",
                            )
                    next_page = data.get("nextPage")
                    if not next_page:
                        return all_entries
                    sep = "&" if "?" in base_url else "?"
                    url = f"{base_url}{sep}page={next_page}"
                    continue
                status = response.status_code
                if status == 404:
                    # Workbook may have been deleted between listing and lineage fetch.
                    return None
                if status == 429:
                    self.report.warning(
                        title="Sigma API rate-limited on /workbooks/{id}/lineage",
                        message=(
                            "Retry budget exhausted on a 429 response for workbook "
                            "lineage lookup. Chart formula warehouse resolution will "
                            "be incomplete for this workbook. Re-run the ingestion "
                            "to recover."
                        ),
                        context=f"workbook_id={workbook_id}, http_status={status}",
                    )
                else:
                    # LossyList caps the warning list, so the distribution of
                    # statuses would not survive a run with many of these.
                    self.report.workbook_lineage_non_200_by_status[str(status)] = (
                        self.report.workbook_lineage_non_200_by_status.get(
                            str(status), 0
                        )
                        + 1
                    )
                    self.report.warning(
                        title="Sigma /workbooks/{id}/lineage returned non-200",
                        message=(
                            "Unable to fetch workbook lineage for warehouse table "
                            "index. Chart formula warehouse resolution may be "
                            "incomplete."
                        ),
                        context=f"workbook_id={workbook_id}, http_status={status}",
                    )
                return None
        except Exception as e:
            self.report.warning(
                title="Sigma /workbooks/{id}/lineage lookup failed",
                message=(
                    "Exception while fetching workbook lineage; warehouse table "
                    "index skipped for this workbook."
                ),
                context=f"workbook_id={workbook_id}",
                exc=e,
            )
            return None

    def get_data_model_by_url_id(self, url_id: str) -> Optional[SigmaDataModel]:
        """Fetch a DM by its urlId (not UUID). Used to resolve personal-space
        or otherwise unlisted DMs referenced from another DM's /lineage.

        Returns None on non-200 so the caller can count and continue.
        The HTTP status code is surfaced in the report warning so operators
        can distinguish 429 (rate-limited, re-run the pipeline) from 403 /
        404 (genuinely forbidden / deleted). 429s (after the urllib3 retry
        budget has been exhausted) additionally bump a dedicated
        ``data_model_external_reference_rate_limited`` counter.
        """
        logger.debug(f"Fetching data model by url_id '{url_id}'.")
        url = f"{self.config.api_url}/dataModels/{url_id}"
        try:
            response = self._get_api_call(url)
            if response.status_code != 200:
                status = response.status_code
                if status == 429:
                    self.report.data_model_external_reference_rate_limited += 1
                    self.report.warning(
                        title="Sigma API rate-limited while fetching orphan Data Model",
                        message=(
                            "Retry budget exhausted on 429; this DM will be "
                            "reported as unresolved for the rest of the run. "
                            "Re-run the ingestion to pick up the cross-DM "
                            "edge, or investigate the Sigma API rate limit."
                        ),
                        context=f"url_id={url_id}, http_status={status}",
                    )
                else:
                    # 401 / 403 / 404 / 5xx (after retries) land here. Emit
                    # a low-severity structured entry so operators can
                    # triage without stdout tailing; the warning is
                    # rate-limited by LossyList on the report side.
                    self.report.warning(
                        title="Sigma orphan Data Model fetch returned non-200",
                        message=(
                            "Cross-DM reference could not be resolved; "
                            "treating as ``dm_unknown`` for the rest of "
                            "the run. Common causes: DM deleted, admin "
                            "scope revoked, personal space not shared with "
                            "the ingest principal."
                        ),
                        context=f"url_id={url_id}, http_status={status}",
                    )
                return None
            data = response.json()
            # By-urlId responses return ``dataModelUrlId`` and a null
            # ``urlId``; by-UUID responses use ``urlId``. Normalize.
            if "dataModelUrlId" in data and not data.get("urlId"):
                data["urlId"] = data["dataModelUrlId"]
            dm = SigmaDataModel.model_validate(data)
            # No file_meta: these DMs are not in /files.
            self._assemble_data_model(dm, file_meta=None)
            return dm
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch data model by url_id '{url_id}'. Exception: {e}"
            )
            self.report.warning(
                title="Sigma orphan Data Model fetch raised exception",
                message=(
                    "An unexpected exception occurred while fetching or "
                    "assembling the cross-DM reference; treating as "
                    "``dm_unknown`` for the rest of the run. Common causes: "
                    "Pydantic validation failure on a malformed 200 payload, "
                    "network error inside element/column/lineage assembly."
                ),
                context=f"url_id={url_id}, exception={type(e).__name__}: {e}",
            )
            return None

    def get_data_models(self) -> List[SigmaDataModel]:
        logger.debug("Fetching all accessible data models metadata.")
        data_model_files_metadata = self._get_files_metadata(
            file_type=Constant.DATA_MODEL
        )
        # Pagination and per-entry parse errors are handled by
        # ``_paginated_entries``; this method only has to apply the
        # workspace / DM-pattern filters and assemble each DM.
        raw_data_models = self._paginated_entries(
            f"{self.config.api_url}/dataModels",
            SigmaDataModel,
            "Unable to fetch sigma data models.",
            dedup_key=lambda dm: dm.dataModelId,
        )
        data_models: List[SigmaDataModel] = []
        for data_model in raw_data_models:
            try:
                file_meta = data_model_files_metadata.get(data_model.dataModelId)

                # DM-pattern filter runs before workspace lookup to
                # short-circuit three extra HTTP calls per filtered DM.
                # (get_sigma_workbooks/datasets check workspace first
                # because their payload is already complete.)
                if not self.config.data_model_pattern.allowed(data_model.name):
                    self.report.data_models.dropped(
                        f"{data_model.name} ({data_model.dataModelId})"
                    )
                    continue

                workspace = None
                # Prefer ``/files`` workspaceId (authoritative for the folder
                # tree) and fall back to the ``/dataModels`` payload so a DM
                # whose ``/files`` row is missing workspace (admin-perm /
                # legacy-tenant edge case, same shape as workbook L833-L839
                # below) but whose payload names an allowed workspace still
                # routes through the normal workspace-pattern branch instead
                # of being dropped / gated behind ``ingest_shared_entities``.
                candidate_workspace_id = (
                    file_meta.workspaceId if file_meta else None
                ) or data_model.workspaceId
                if candidate_workspace_id:
                    workspace = self.get_workspace(candidate_workspace_id)

                if workspace:
                    if self.config.workspace_pattern.allowed(workspace.name):
                        self.report.data_models.processed(
                            f"{data_model.name} ({data_model.dataModelId}) in {workspace.name}"
                        )
                        self._assemble_data_model(
                            data_model,
                            file_meta,
                            resolved_workspace_id=candidate_workspace_id,
                        )
                        data_models.append(data_model)
                    else:
                        self.report.data_models.dropped(
                            f"{data_model.name} ({data_model.dataModelId}) in {workspace.name}"
                        )
                elif self.config.ingest_shared_entities:
                    self.report.data_models_without_workspace += 1
                    self.report.data_models.processed(
                        f"{data_model.name} ({data_model.dataModelId}) (no workspace)"
                    )
                    self._assemble_data_model(
                        data_model,
                        file_meta,
                        resolved_workspace_id=candidate_workspace_id,
                    )
                    data_models.append(data_model)
                else:
                    self.report.data_models.dropped(
                        f"{data_model.name} ({data_model.dataModelId}) (no workspace, ingest_shared_entities=False)"
                    )
            except Exception as e:
                # Per-DM isolation: an unexpected exception during
                # assembly (pydantic remodel, network transient that
                # escapes the inner silent-status gate, etc.) must not
                # abort the whole DM feed. Mirrors ``get_sigma_workbooks``
                # which swallows per-workbook failures for the same
                # reason. The warning carries enough identifiers to
                # locate the offender, and the outer loop continues
                # with the next DM.
                self.report.warning(
                    title="Failed to assemble Sigma Data Model",
                    message="Skipping this DM; other DMs will still be "
                    "assembled. The DM Container, its elements, and any "
                    "lineage derived from it will be absent from this "
                    "ingestion run.",
                    context=(
                        f"dataModelId={data_model.dataModelId}, "
                        f"name={data_model.name!r}"
                    ),
                    exc=e,
                )
                self.report.data_models.dropped(
                    f"{data_model.name} ({data_model.dataModelId}) "
                    "(assembly failed -- see warning)"
                )
        return data_models

    def get_sigma_workbooks(self) -> List[Workbook]:
        logger.debug("Fetching all accessible workbooks metadata.")
        workbook_url = url = f"{self.config.api_url}/workbooks"
        workbook_files_metadata = self._get_files_metadata(file_type=Constant.WORKBOOK)
        try:
            workbooks: List[Workbook] = []
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for workbook_dict in response_dict[Constant.ENTRIES]:
                    workbook = Workbook.model_validate(workbook_dict)

                    # Skip workbook if workbook name filtered out by config
                    if not self.config.workbook_pattern.allowed(workbook.name):
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId})"
                        )
                        continue

                    if workbook.workbookId not in workbook_files_metadata:
                        # Due to a bug in the Sigma API, it seems like the /files endpoint does not
                        # return file metadata when the user has access via admin permissions. In
                        # those cases, the user associated with the token needs to be manually added
                        # to the workspace.
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId}) (missing file metadata; path: {workbook.path}; likely need to manually add user to workspace)"
                        )
                        continue

                    workbook.workspaceId = workbook_files_metadata[
                        workbook.workbookId
                    ].workspaceId

                    workbook.badge = workbook_files_metadata[workbook.workbookId].badge

                    workspace = None
                    if workbook.workspaceId:
                        workspace = self.get_workspace(workbook.workspaceId)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.workbooks.processed(
                                f"{workbook.name} ({workbook.workbookId}) in {workspace.name}"
                            )
                            workbook.pages = self.get_workbook_pages(workbook)
                            workbooks.append(workbook)
                        else:
                            self.report.workbooks.dropped(
                                f"{workbook.name} ({workbook.workbookId}) in {workspace.name}"
                            )
                    elif self.config.ingest_shared_entities:
                        # If no workspace for workbook we can consider it as shared entity
                        self.report.workbooks_without_workspace += 1
                        self.report.workbooks.processed(
                            f"{workbook.name} ({workbook.workbookId}) in workspace id {workbook.workspaceId or 'unknown'}"
                        )
                        workbook.pages = self.get_workbook_pages(workbook)
                        workbooks.append(workbook)
                    else:
                        self.report.workbooks.dropped(
                            f"{workbook.name} ({workbook.workbookId}) in workspace id {workbook.workspaceId or 'unknown'}"
                        )

                if response_dict[Constant.NEXTPAGE]:
                    url = f"{workbook_url}?page={response_dict[Constant.NEXTPAGE]}"
                else:
                    break
            return workbooks
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma workbooks. Exception: {e}"
            )
            return []
