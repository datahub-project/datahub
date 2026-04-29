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
    Workbook,
    Workspace,
)

# Logger instance
logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


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

    def _log_http_error(self, message: str) -> Any:
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
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
        """Make an API call with automatic retry on 429/503 and token refresh on 401."""
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
            logger.error(
                f"Unable to find workspace id using file path '{path}'. Exception: {e}"
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
        elif source_type == "join":
            queue.append(source_node_id)  # pass-through
        elif source_type == "table":
            pass  # terminal; warehouse lineage comes from SQL parsing
        else:
            # Warn once per unknown source_type to avoid log spam.
            warn_key = source_type if isinstance(source_type, str) else "<non-str>"
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
        (``dataset``) and intra-workbook element (``sheet``) nodes. Walks
        through ``join`` pass-through nodes. Warehouse tables (``table``)
        come from the SQL-parse path and are skipped here.

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
            self.report.warning(
                message="Failed to fetch Sigma element lineage",
                context=f"element={element.name}, workbook={workbook.name}",
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
            self._log_http_error(
                message=f"Unable to fetch sql query for element {element.name} of workbook '{workbook.name}'. Exception: {e}"
            )
        return None

    def get_page_elements(self, workbook: Workbook, page: Page) -> List[Element]:
        try:
            elements: List[Element] = []
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages/{page.pageId}/elements"
            )
            response.raise_for_status()
            for i, element_dict in enumerate(response.json()[Constant.ENTRIES]):
                # only element of table and visualization type have lineage and sql query supported
                if element_dict.get("type") not in ["table", "visualization"]:
                    logger.debug(
                        f"Skipping lineage and sql query extraction for element {element_dict.get('name')} of type {element_dict.get('type')} of workbook '{workbook.name}'"
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
                if (
                    self.config.extract_lineage
                    and self.config.workbook_lineage_pattern.allowed(workbook.name)
                ):
                    element.upstream_sources = self._get_element_upstream_sources(
                        element, workbook
                    )
                    element.query = self._get_element_sql_query(element, workbook)
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
            response = self._get_api_call(
                f"{self.config.api_url}/workbooks/{workbook.workbookId}/pages"
            )
            response.raise_for_status()
            for page_dict in response.json()[Constant.ENTRIES]:
                page = Page.model_validate(page_dict)
                page.elements = self.get_page_elements(workbook, page)
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
        try:
            while True:
                response = self._get_api_call(url)
                if first_page and response.status_code in silent_statuses:
                    logger.debug(
                        f"{error_ctx} Swallowed expected status "
                        f"{response.status_code} on first page."
                    )
                    return raw_entries
                first_page = False
                response.raise_for_status()
                response_dict = response.json()
                for entry in response_dict.get(Constant.ENTRIES, []):
                    if isinstance(entry, dict):
                        raw_entries.append(entry)
                next_page = response_dict.get(Constant.NEXTPAGE)
                next_token = response_dict.get(Constant.NEXTPAGETOKEN)
                if next_page:
                    cursor_key: Tuple[str, str] = ("page", str(next_page))
                    cursor = f"page={next_page}"
                elif next_token:
                    cursor_key = ("nextPageToken", str(next_token))
                    cursor = f"nextPageToken={next_token}"
                else:
                    break
                if cursor_key in seen_cursors:
                    self.report.warning(
                        message=f"{error_ctx} Pagination cursor repeated; aborting.",
                        context=f"url={base_url}, cursor={cursor}, "
                        f"entries_so_far={len(raw_entries)}",
                    )
                    break
                seen_cursors.add(cursor_key)
                url = f"{base_url}{separator}{cursor}"
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
            self.report.warning(
                title="Sigma paginated endpoint aborted",
                message="Pagination aborted; partial results preserved.",
                context=(
                    f"endpoint={error_ctx}, url={url}, "
                    f"partial_results={len(raw_entries)}"
                    + (f", http_status={http_status}" if http_status else "")
                ),
                exc=e,
            )
            self._log_http_error(message=f"{error_ctx} Exception: {e}")
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
                self.report.pagination_malformed_entries_dropped += 1
                if malformed_warned < self._MAX_MALFORMED_WARNINGS_PER_ENDPOINT:
                    self.report.warning(
                        message=f"{error_ctx} Dropped malformed entry.",
                        context=f"entry={entry!r}",
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
        return self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/columns",
            SigmaDataModelColumn,
            f"Unable to fetch columns for data model '{data_model_id}'.",
            dedup_key=lambda column: column.columnId,
        )

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
            # ``type: dataset`` / ``type: table`` entries are resolved
            # on the fly from their ``inode-<id>`` source_ids; no DM-side
            # stash is needed.

        for element in elements:
            element.columns = columns_by_element.get(element.elementId, [])
            element.source_ids = source_ids_by_element.get(element.elementId, [])

        data_model.elements = elements

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
