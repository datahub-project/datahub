import functools
import logging
import sys
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set, Type, TypeVar

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
        self.session = requests.Session()

        # Configure retry strategy for 429/503 with exponential backoff
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
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "sheet":
            element_id = source_node.get(Constant.ELEMENTID)
            if element_id is None:
                self.report.warning(
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
                    message="Failed to parse Sigma lineage node",
                    context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                    exc=e,
                )
        elif source_type == "join":
            # Pass-through node: enqueue for continued BFS traversal.
            queue.append(source_node_id)
        elif source_type == "table":
            # Warehouse table: handled by SQL-parse path; terminal for BFS.
            pass
        else:
            self.report.warning(
                message="Unknown Sigma lineage node type",
                context=f"type={source_type!r}, element={element.name}, workbook={workbook.name}",
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

                    # Per-node isolation: a malformed node skips itself.
                    try:
                        source_node = dependencies[source_node_id]
                    except (KeyError, AttributeError, TypeError) as e:
                        self.report.warning(
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
                            message="Failed to parse Sigma lineage node",
                            context=f"node={source_node_id}, element={element.name}, workbook={workbook.name}",
                            exc=e,
                        )
        except (KeyError, AttributeError, TypeError, ValidationError) as e:
            # Structural errors in the setup phase (missing keys, malformed
            # edges, non-dict seed entries).
            self.report.warning(
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

    def _paginated_entries(
        self, base_url: str, model_cls: Type[T], error_ctx: str
    ) -> List[T]:
        """Page through a Sigma list endpoint, parsing each entry into
        ``model_cls``. Handles both pagination shapes (``nextPage`` and
        ``nextPageToken``). Swallows HTTP/JSON errors so a broken page
        does not abort the containing ingestion loop.
        """
        # Use ``&`` as the separator if the base URL already has query
        # params (e.g. an ``api_url`` routed through a proxy), so
        # pagination does not collide with existing params.
        separator = "&" if "?" in base_url else "?"
        url = base_url
        results: List[T] = []
        try:
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for entry in response_dict.get(Constant.ENTRIES, []):
                    results.append(model_cls.model_validate(entry))
                next_page = response_dict.get(Constant.NEXTPAGE)
                next_token = response_dict.get(Constant.NEXTPAGETOKEN)
                if next_page:
                    url = f"{base_url}{separator}page={next_page}"
                elif next_token:
                    url = f"{base_url}{separator}nextPageToken={next_token}"
                else:
                    break
            return results
        except Exception as e:
            self._log_http_error(message=f"{error_ctx} Exception: {e}")
            return []

    def _get_data_model_elements(
        self, data_model_id: str
    ) -> List[SigmaDataModelElement]:
        logger.debug(f"Fetching elements for data model '{data_model_id}'.")
        return self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/elements",
            SigmaDataModelElement,
            f"Unable to fetch elements for data model '{data_model_id}'.",
        )

    def _get_data_model_columns(self, data_model_id: str) -> List[SigmaDataModelColumn]:
        logger.debug(f"Fetching columns for data model '{data_model_id}'.")
        return self._paginated_entries(
            f"{self.config.api_url}/dataModels/{data_model_id}/columns",
            SigmaDataModelColumn,
            f"Unable to fetch columns for data model '{data_model_id}'.",
        )

    def _get_data_model_lineage_entries(
        self, data_model_id: str
    ) -> List[Dict[str, Any]]:
        """Return raw entries from the DM /lineage endpoint. ``element``
        entries have ``sourceIds`` holding either an intra-DM elementId
        or an ``inode-<suffix>`` for external upstreams.
        """
        logger.debug(f"Fetching lineage for data model '{data_model_id}'.")
        url = f"{self.config.api_url}/dataModels/{data_model_id}/lineage"
        try:
            response = self._get_api_call(url)
            if response.status_code in (400, 403, 404, 500):
                logger.debug(
                    f"Lineage not available for data model '{data_model_id}' "
                    f"(status {response.status_code})."
                )
                return []
            response.raise_for_status()
            response_dict = response.json()
            entries = response_dict.get(Constant.ENTRIES, [])
            return [entry for entry in entries if isinstance(entry, dict)]
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch lineage for data model '{data_model_id}'. Exception: {e}"
            )
            return []

    def _assemble_data_model(
        self, data_model: SigmaDataModel, file_meta: Optional[File]
    ) -> None:
        """Fetch and attach elements, per-element columns, and per-element sourceIds."""
        if file_meta is not None:
            # Fill only when the DM payload did not carry the field, so a
            # future vendor change that populates these on ``/dataModels``
            # directly cannot be silently overridden by ``/files``.
            if data_model.workspaceId is None:
                data_model.workspaceId = file_meta.workspaceId
            if data_model.path is None:
                data_model.path = file_meta.path
            if data_model.badge is None:
                data_model.badge = file_meta.badge
            if data_model.urlId is None and file_meta.urlId:
                data_model.urlId = file_meta.urlId

        elements = self._get_data_model_elements(data_model.dataModelId)
        columns = self._get_data_model_columns(data_model.dataModelId)
        lineage_entries = self._get_data_model_lineage_entries(data_model.dataModelId)

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

    def get_data_models(self) -> List[SigmaDataModel]:
        logger.debug("Fetching all accessible data models metadata.")
        base_url = url = f"{self.config.api_url}/dataModels"
        # Match ``_paginated_entries``: handle a proxied ``api_url`` that
        # already carries a query string so pagination does not collide.
        separator = "&" if "?" in base_url else "?"
        data_model_files_metadata = self._get_files_metadata(
            file_type=Constant.DATA_MODEL
        )
        try:
            data_models: List[SigmaDataModel] = []
            while True:
                response = self._get_api_call(url)
                response.raise_for_status()
                response_dict = response.json()
                for dm_dict in response_dict.get(Constant.ENTRIES, []):
                    try:
                        data_model = SigmaDataModel.model_validate(dm_dict)
                    except ValidationError as e:
                        self.report.warning(
                            message="Failed to parse Sigma Data Model payload",
                            context=f"entry={dm_dict!r}",
                            exc=e,
                        )
                        continue

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
                    candidate_workspace_id = (
                        file_meta.workspaceId if file_meta else None
                    )
                    if candidate_workspace_id:
                        workspace = self.get_workspace(candidate_workspace_id)

                    if workspace:
                        if self.config.workspace_pattern.allowed(workspace.name):
                            self.report.data_models.processed(
                                f"{data_model.name} ({data_model.dataModelId}) in {workspace.name}"
                            )
                            self._assemble_data_model(data_model, file_meta)
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
                        self._assemble_data_model(data_model, file_meta)
                        data_models.append(data_model)
                    else:
                        self.report.data_models.dropped(
                            f"{data_model.name} ({data_model.dataModelId}) (no workspace, ingest_shared_entities=False)"
                        )

                next_page = response_dict.get(Constant.NEXTPAGE)
                next_token = response_dict.get(Constant.NEXTPAGETOKEN)
                if next_page:
                    url = f"{base_url}{separator}page={next_page}"
                elif next_token:
                    url = f"{base_url}{separator}nextPageToken={next_token}"
                else:
                    break
            return data_models
        except Exception as e:
            self._log_http_error(
                message=f"Unable to fetch sigma data models. Exception: {e}"
            )
            return []

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
