import logging
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

import requests
from requests.adapters import DEFAULT_POOLSIZE, HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.qlik_sense.config import Constant, QlikSourceConfig
from datahub.ingestion.source.qlik_sense.data_classes import (
    DATABASE_QRI_PREFIX,
    PERSONAL_SPACE_DICT,
    App,
    Item,
    LineageGraph,
    LineageGraphLevel,
    LineageTable,
    LineageUpstreams,
    ListedDataset,
    QlikDataset,
    QlikTable,
    ReloadStatement,
    Space,
)
from datahub.ingestion.source.qlik_sense.file_lineage import (
    file_connector_platform,
    file_storage_of_connection,
)
from datahub.ingestion.source.qlik_sense.qlik_base_api import (
    UNREAD_DATA_CONNECTIONS,
    UNREAD_SPACES,
    IsUnchanged,
    QlikBaseAPI,
    ReadErrors,
    log_read_error,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    SPACE_SEPARATOR,
    DataConnectionInfo,
    IncludeRead,
    inline_includes,
    merged_variables,
)
from datahub.ingestion.source.qlik_sense.websocket_connection import (
    EngineError,
    WebsocketConnection,
)

# Logger instance
logger = logging.getLogger(__name__)


def _database_labels(graph: LineageGraph) -> Dict[str, str]:
    return {
        qri: node.label
        for qri, node in graph.nodes.items()
        if qri.startswith(DATABASE_QRI_PREFIX) and node.label
    }


class QlikAPI(QlikBaseAPI):
    def __init__(self, config: QlikSourceConfig) -> None:
        super().__init__(config)
        if not config.tenant_hostname or config.api_key is None:
            raise ValueError("Qlik Cloud requires tenant_hostname and api_key")
        self.tenant_hostname = config.tenant_hostname
        self.api_key = config.api_key.get_secret_value()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )
        # Qlik Cloud rate-limits the tenant, and parallel app reads make 429s
        # likely. It also drops connections mid-pagination now and then. Every
        # call only reads, including the lineage overview POST. Connection
        # errors are not retried, so an unreachable tenant still fails fast.
        retry = Retry(
            total=5,
            connect=0,
            read=2,
            other=0,
            status=5,
            backoff_factor=1,
            status_forcelist=[429],
            allowed_methods=None,
            raise_on_status=False,
        )
        # Every worker can hold a connection at once.
        pool_size = max(DEFAULT_POOLSIZE, config.resolved_max_workers())
        self.session.mount(
            "https://",
            HTTPAdapter(max_retries=retry, pool_maxsize=pool_size),
        )
        self.rest_api_url = f"https://{self.tenant_hostname}/api/v1"
        # Included scripts by the space (or personal app) reading them and the
        # path, None when unreadable. Many apps include the same file, so each
        # is read once a run per space, as a path without a space resolves in
        # the reading app's space.
        self._include_contents: Dict[str, IncludeRead] = {}
        # Test connection by fetching list of api keys
        logger.info(f"Trying to connect to {self.rest_api_url}")
        self.session.get(f"{self.rest_api_url}/api-keys").raise_for_status()

    def _paginate(self, url: str) -> Iterable[Dict]:
        next_url: Optional[str] = url
        while next_url:
            response = self.session.get(next_url)
            response.raise_for_status()
            page = response.json()
            yield from page.get(Constant.DATA) or []
            next_url = (page.get(Constant.LINKS, {}).get(Constant.NEXT) or {}).get(
                Constant.HREF
            )

    def get_spaces(self) -> List[Space]:
        spaces: List[Space] = []
        try:
            for space_dict in self._paginate(f"{self.rest_api_url}/spaces"):
                space = Space.model_validate(space_dict)
                spaces.append(space)
                self.spaces[space.id] = space.name
            # Add personal space entity
            spaces.append(Space.model_validate(PERSONAL_SPACE_DICT))
            self.spaces[Constant.PERSONAL_SPACE_ID] = Constant.PERSONAL_SPACE_NAME
        except Exception as e:
            self._record_unread(
                UNREAD_SPACES, f"Unable to fetch spaces. Exception: {e}", e
            )
        return spaces

    def _get_dataset(self, dataset_id: str, item_id: str) -> QlikDataset:
        response = self.session.get(f"{self.rest_api_url}/data-sets/{dataset_id}")
        response.raise_for_status()
        response_dict = response.json()
        response_dict[Constant.ITEMID] = item_id
        dataset = QlikDataset.model_validate(response_dict)
        try:
            tables = self.get_table_lineage(dataset.secureQri)
            # No graph (404) leaves the storing apps unknown, while an empty
            # graph means none store into it any more.
            if tables is not None:
                dataset.stored_from_qris = [
                    qri for table in tables for qri in table.source_qris
                ]
                dataset.stored_field_lineage_failed = any(
                    table.field_lineage_failed for table in tables
                )
                for table in tables:
                    for name, sources in table.field_source_qris.items():
                        dataset.stored_field_source_qris.setdefault(name, []).extend(
                            sources
                        )
        except Exception as e:
            log_read_error(
                f"Unable to fetch table lineage of dataset {dataset_id}. Exception: {e}"
            )
            self.lineage_read_failures.append(f"{dataset_id}: {e}")
        return dataset

    def get_table_lineage(self, qri: str) -> Optional[List[LineageTable]]:
        """The resource's tables with the tables they are loaded from, or None
        when Qlik has no lineage graph for it (404)."""
        encoded_qri = quote(qri, safe="")
        response = self.session.get(
            f"{self.rest_api_url}/lineage-graphs/nodes/{encoded_qri}/actions/expand"
            f"?node={encoded_qri}&level={LineageGraphLevel.TABLE}"
        )
        # Qlik has no lineage for a resource it has not loaded or stored yet, or
        # has not computed it since.
        if response.status_code == 404:
            return None
        graph = self._graph_of(response, f"Lineage graph of {qri}")
        # Only the resource's own tables: neighbouring nodes would otherwise be
        # read as its tables, and their sources as its sources.
        tables = {
            table_qri: LineageTable(qri=table_qri, label=node.label or "")
            for table_qri, node in graph.nodes.items()
            if table_qri.startswith(f"{qri}#")
        }
        for edge in graph.edges:
            table = tables.get(edge.target or "")
            if table and edge.source:
                table.source_qris.append(edge.source)
        if self.config.include_column_lineage:
            for table in tables.values():
                # The table graph is complete without its field level, which
                # only adds column lineage.
                try:
                    self._add_field_lineage(qri, table)
                except Exception as e:
                    log_read_error(
                        f"Unable to fetch field lineage of {table.qri}. Exception: {e}"
                    )
                    table.field_lineage_failed = True
                    self.field_lineage_failures.append(f"{table.qri}: {e}")
        return list(tables.values())

    @staticmethod
    def _graph_of(response: requests.Response, what: str) -> LineageGraph:
        response.raise_for_status()
        graph = response.json().get(Constant.GRAPH)
        if graph is None:
            raise ValueError(f"{what} response has no graph")
        return LineageGraph.model_validate(graph)

    def _upstream_graph(
        self, qri: str, level: LineageGraphLevel, up: int
    ) -> LineageGraph:
        return self._graph_of(
            self.session.get(
                f"{self.rest_api_url}/lineage-graphs/nodes/{quote(qri, safe='')}"
                f"?level={level}&up={up}"
            ),
            f"Upstream lineage of {qri}",
        )

    def get_lineage_upstreams(self, qri: str) -> LineageUpstreams:
        """Labels of the resources upstream of the app, and the app tables two
        levels up that store into its source tables."""
        resources = self._upstream_graph(qri, LineageGraphLevel.RESOURCE, 1)
        upstreams = LineageUpstreams(
            resource_labels={
                resource_qri: node.label
                for resource_qri, node in resources.nodes.items()
                if node.label
            }
        )
        tables = self._upstream_graph(qri, LineageGraphLevel.TABLE, 2)
        upstreams.database_labels = _database_labels(tables)
        for edge in tables.edges:
            if (
                edge.source
                and edge.target
                and (edge.relation or "").lower() == Constant.STORE
            ):
                upstreams.stored_from.setdefault(edge.target, []).append(edge.source)
        return upstreams

    def _add_field_lineage(self, qri: str, table: LineageTable) -> None:
        """Expanding a table node at field level gives its field nodes, named,
        and the field nodes each is loaded or stored from."""
        response = self.session.get(
            f"{self.rest_api_url}/lineage-graphs/nodes/{quote(qri, safe='')}"
            f"/actions/expand?node={quote(table.qri, safe='')}"
            f"&level={LineageGraphLevel.FIELD}"
        )
        graph = self._graph_of(response, f"Field lineage of {table.qri}")
        names = {
            field_qri: node.label
            for field_qri, node in graph.nodes.items()
            if field_qri.startswith(f"{table.qri}#") and node.label
        }
        table.fields = list(names.values())
        for edge in graph.edges:
            name = names.get(edge.target or "")
            if name and edge.source:
                table.field_source_qris.setdefault(name, []).append(edge.source)

    def list_datasets(self) -> Optional[List[ListedDataset]]:
        datasets: List[ListedDataset] = []
        try:
            for item in self._paginate(
                f"{self.rest_api_url}/items?resourceType={Constant.DATASET}"
            ):
                attributes = item.get(Constant.RESOURCEATTRIBUTES) or {}
                datasets.append(
                    ListedDataset(
                        space_id=item.get(Constant.SPACEID)
                        or Constant.PERSONAL_SPACE_ID,
                        name=item[Constant.NAME],
                        secure_qri=attributes.get(Constant.SECUREQRI) or None,
                        type=attributes.get(Constant.TYPE) or "",
                    )
                )
        except Exception as e:
            # Only lineage needs the list, and it falls back to adding edges,
            # so this is not a failed read of the run's entities.
            log_read_error(f"Unable to list datasets. Exception: {e}")
            self.dataset_listing_error = str(e)
            return None
        return datasets

    def get_user_name(self, user_id: str) -> Optional[str]:
        try:
            if user_id in self.users:
                # To avoid fetching same user details again
                return self.users[user_id]
            elif user_id in self.failed_user_lookups:
                return None
            else:
                response = self.session.get(f"{self.rest_api_url}/users/{user_id}")
                response.raise_for_status()
                user_name = response.json()[Constant.NAME]
                self.users[user_id] = user_name
                return user_name
        except Exception as e:
            self._record_failed_user_lookup(user_id, e)
        return None

    def _open_engine(self, app_id: str) -> WebsocketConnection:
        return WebsocketConnection.for_cloud(
            self.tenant_hostname,
            self.api_key,
            app_id,
            timeout=self.config.timeout_seconds,
        )

    def _sheet_meta(self, meta: Dict, sheet_id: str) -> Optional[Dict]:
        # A sheet without an owner is a private sheet.
        return meta if Constant.OWNERID in meta else None

    @staticmethod
    def _table_label_key(label: str) -> str:
        return label.replace('"', "").split(".")[-1]

    @classmethod
    def _own_table_nodes_by_label(
        cls, table_nodes: Dict[str, Dict], app_id: str
    ) -> Dict[str, str]:
        """The app's own table nodes by label, leaving out labels more than one
        node has, as those can't be told apart."""
        by_label: Dict[str, List[str]] = {}
        for qri, node in table_nodes.items():
            if qri.startswith(f"qri:app:sense://{app_id}#") and node.get(
                Constant.LABEL
            ):
                by_label.setdefault(
                    cls._table_label_key(node[Constant.LABEL]), []
                ).append(qri)
        return {label: qris[0] for label, qris in by_label.items() if len(qris) == 1}

    def _add_qri_of_tables(
        self, tables: List[QlikTable], app_id: str, errors: ReadErrors
    ) -> None:
        table_qri_dict: Dict[str, str] = {}
        app_qri = quote(f"qri:app:sense://{app_id}", safe="")
        try:
            response = self.session.get(
                f"{self.rest_api_url}/lineage-graphs/nodes/{app_qri}/actions/expand?node={app_qri}&level=TABLE"
            )
            if response.status_code == 404:
                logger.debug(f"App {app_id} has no lineage graph")
                errors.lineage_graph_missing = True
                return
            response.raise_for_status()
            table_nodes = response.json()[Constant.GRAPH][Constant.NODES]
            graph_qris = self._own_table_nodes_by_label(table_nodes, app_id)
            for table_node_qri in table_nodes:
                table_node_qri = quote(table_node_qri, safe="")
                response = self.session.get(
                    f"{self.rest_api_url}/lineage-graphs/nodes/{app_qri}/actions/expand?node={table_node_qri}&level=FIELD"
                )
                response.raise_for_status()
                field_nodes_qris = list(
                    response.json()[Constant.GRAPH][Constant.NODES].keys()
                )
                for field_node_qri in field_nodes_qris:
                    response = self.session.post(
                        f"{self.rest_api_url}/lineage-graphs/nodes/{app_qri}/overview",
                        json=[field_node_qri],
                    )
                    response.raise_for_status()
                    # Some fields might not have lineage overview, in that case status code is 207
                    if response.status_code == 200:
                        for each_lineage in response.json()[Constant.RESOURCES][0][
                            Constant.LINEAGE
                        ]:
                            table_name = self._table_label_key(
                                each_lineage[Constant.TABLELABEL]
                            )
                            table_qri_dict[table_name] = each_lineage[Constant.TABLEQRI]
                        break
            for table in tables:
                if table.tableName in table_qri_dict:
                    table.tableQri = table_qri_dict[table.tableName]
                table.graphQri = graph_qris.get(table.tableName)
            matched = {table.graphQri for table in tables if table.tableQri}
            errors.unmatched_graph_tables = sorted(
                {
                    node.get(Constant.LABEL) or qri.rpartition("#")[2]
                    for qri, node in table_nodes.items()
                    if qri.startswith(f"qri:app:sense://{app_id}#")
                    and qri not in matched
                }
            )
        except Exception as e:
            errors.record(
                f"Unable to add QRI for tables of app {app_id}. Exception: {e}"
            )

    def _get_app_used_tables(
        self,
        websocket_connection: WebsocketConnection,
        app_id: str,
        errors: ReadErrors,
    ) -> List[QlikTable]:
        tables: List[QlikTable] = []
        # The app handle must be on top again afterwards, as GetScript runs on it.
        with websocket_connection.restore_handle():
            try:
                response = websocket_connection.websocket_send_request(
                    method="GetObject",
                    params=["LoadModel"],
                )
                if not response[Constant.QRETURN][Constant.QTYPE]:
                    logger.debug(f"App {app_id} has no LoadModel object")
                    return []
                response = websocket_connection.websocket_send_request(
                    method="GetLayout"
                )
                for table_dict in response[Constant.QLAYOUT][Constant.TABLES]:
                    tables.append(QlikTable.model_validate(table_dict))
                self._add_qri_of_tables(tables, app_id, errors)
                logger.debug(
                    f"App {app_id} LoadModel tables: "
                    f"{[(t.tableName, t.type.value, t.dataconnectorPlatform, t.tableQri) for t in tables]}"
                )
            except Exception as e:
                errors.record(
                    f"Unable to fetch tables used by app {app_id}. Exception: {e}"
                )
        return tables

    def get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        # Scripts name a connection as `<space>:<connection>`, or by its bare name
        # when it is in the app's own space. Several spaces can hold a connection
        # with the same name, so a bare name is only kept when it is unique.
        connections: Dict[str, DataConnectionInfo] = {}
        by_name: Dict[str, List[DataConnectionInfo]] = {}
        try:
            for connection in self._paginate(f"{self.rest_api_url}/data-connections"):
                name = connection.get(Constant.QNAME)
                if not name:
                    continue
                info = self._infer_connection(
                    name,
                    connection,
                    (
                        Constant.DATASOURCEID,
                        Constant.QCONNECTSTATEMENT,
                        Constant.QTYPE,
                    ),
                )
                info.file_storage = file_storage_of_connection(
                    connection.get(Constant.QCONNECTSTATEMENT) or ""
                )
                info.file_platform = file_connector_platform(
                    connection.get(Constant.QCONNECTSTATEMENT) or ""
                )
                # A personal connection has no space, like a personal app.
                space_name = self.spaces.get(
                    connection.get(Constant.SPACE) or Constant.PERSONAL_SPACE_ID
                )
                if space_name:
                    connections[f"{space_name}{SPACE_SEPARATOR}{name}"] = info
                by_name.setdefault(name, []).append(info)
        except Exception as e:
            self._record_unread(
                UNREAD_DATA_CONNECTIONS,
                f"Unable to fetch data connections. Exception: {e}",
                e,
            )
        for name, infos in by_name.items():
            if len(infos) == 1:
                connections.setdefault(name, infos[0])
        logger.debug(f"Qlik Cloud data connections: {connections}")
        return connections

    def _read_include(
        self,
        websocket_connection: WebsocketConnection,
        reader: str,
        path: str,
        transient: Dict[str, IncludeRead],
    ) -> IncludeRead:
        """The script of a file an app includes, read by the Engine as a reload
        reads it. `transient` holds failures outside the Engine for the app
        being read, so a hung file is not waited on again for that app."""
        key = f"{reader}|{path}"
        if key in self._include_contents:
            return self._include_contents[key]
        if path in transient:
            return transient[path]
        try:
            content = websocket_connection.websocket_send_request(
                method="GetIncludeFileContent", params={"qPath": path}
            ).get("qContent")
        except EngineError as e:
            result = IncludeRead(error=str(e))
        except Exception as e:
            # Not cached for the run, as a dropped websocket may read it for
            # the next app.
            transient[path] = IncludeRead(error=str(e))
            return transient[path]
        else:
            result = (
                IncludeRead(content=content)
                if isinstance(content, str)
                else IncludeRead(error="no script text returned")
            )
        self._include_contents[key] = result
        return result

    def _inline_includes(
        self,
        websocket_connection: WebsocketConnection,
        app_id: str,
        app: App,
        script: str,
    ) -> str:
        # A personal app's unqualified paths resolve in its owner's personal
        # space, so its includes are cached apart.
        personal = app.spaceId == Constant.PERSONAL_SPACE_ID
        reader = app_id if personal else app.spaceId
        space = (
            Constant.PERSONAL_SPACE_NAME
            if personal
            else self.spaces.get(app.spaceId, app.spaceId)
        )
        unread: List[str] = []
        transient: Dict[str, IncludeRead] = {}

        def report(path: str, reason: str) -> None:
            unread.append(f"{path} (app {app_id} in space {space}): {reason}")

        try:
            inlined = inline_includes(
                script,
                merged_variables(self.config.script_variables, app.variables),
                lambda path: self._read_include(
                    websocket_connection, reader, path, transient
                ),
                report,
            )
        except Exception as e:
            # The app's own script still holds its includes, so it still
            # counts as including scripts that are not read.
            report("included scripts", f"Unable to inline. Exception: {e}")
            inlined = script
        self.include_read_failures.extend(unread)
        return inlined

    def get_reload_statements(self, app_id: str) -> List[ReloadStatement]:
        response = self.session.get(f"{self.rest_api_url}/apps/{app_id}/data/lineage")
        response.raise_for_status()
        return [ReloadStatement.model_validate(entry) for entry in response.json()]

    def _read_reload_statements(self, app: App) -> None:
        """The statements of the app's last reload, which may name the database
        tables its lineage graph tables load from."""
        if not any(
            qri.startswith(DATABASE_QRI_PREFIX)
            for table in app.lineage_tables
            for qri in table.source_qris
        ):
            return
        try:
            app.reload_statements = self.get_reload_statements(app.id)
        except Exception as e:
            log_read_error(
                f"Unable to fetch reload statements of app {app.id}. Exception: {e}"
            )
            app.reload_statements_read_failed = True
            self.reload_statement_read_failures.append(f"{app.id}: {e}")

    def get_database_labels(self, qri: str) -> Dict[str, str]:
        """Labels of the database tables the app's lineage graph tables load
        from, which only the upstream graph gives."""
        return _database_labels(self._upstream_graph(qri, LineageGraphLevel.TABLE, 1))

    def _get_app(self, app_id: str, script_app: bool = False) -> App:
        errors = ReadErrors()
        websocket_connection = self._open_engine(app_id)
        try:
            websocket_connection.websocket_send_request(
                method="OpenDoc",
                params={"qDocName": app_id},
            )
            response = websocket_connection.websocket_send_request(
                method="GetAppLayout"
            )
            app = App.model_validate(response[Constant.QLAYOUT])
            if script_app:
                app.qri = f"qri:app:dataprep://{app_id}"
            app.sheets = self._get_app_sheets(websocket_connection, app_id, errors)
            variables = self._read_variables(websocket_connection, app_id)
            app.variables = variables or {}
            app.variables_read_failed = variables is None
            read_errors = len(errors.messages)
            app.tables = self._get_app_used_tables(websocket_connection, app_id, errors)
            if not app.tables and len(errors.messages) == read_errors:
                # Script-built apps have no data model, but Qlik still traces
                # their tables, including QVDs they load.
                try:
                    lineage_tables = self.get_table_lineage(app.qri)
                    app.lineage_graph_missing = lineage_tables is None
                    app.lineage_tables = lineage_tables or []
                except Exception as e:
                    errors.record(
                        f"Unable to fetch table lineage of app {app_id}. Exception: {e}"
                    )
                self._read_reload_statements(app)
            app.lineage_graph_missing |= errors.lineage_graph_missing
            app.unmatched_graph_tables = errors.unmatched_graph_tables
            app.tables_read_failed = len(errors.messages) > read_errors
            if not app.tables or not all(table.tableQri for table in app.tables):
                try:
                    script = websocket_connection.get_script()
                except Exception as e:
                    if isinstance(e, EngineError) and e.access_denied:
                        # Cloud only lets Owner, Can manage and Can operate read
                        # the load script, so a denied read leaves the rest of
                        # the app complete. The denial lasts, so the data model
                        # tables are all the tables charts will get.
                        log_read_error(
                            f"Access denied to load script of app {app_id}: {e}"
                        )
                        app.script_read_failed = True
                    else:
                        app.tables_read_failed = True
                        errors.record(
                            f"Unable to fetch load script of app {app_id}. Exception: {e}"
                        )
                else:
                    app.script = self._inline_includes(
                        websocket_connection, app_id, app, script
                    )
            if not app.tables or not all(table.tableQri for table in app.tables):
                # Graph and script tables carry no fields of their own; the
                # data model has them as the last reload left them.
                model_errors = len(errors.messages)
                app.data_model_tables = self._read_data_model(
                    websocket_connection, app_id, errors
                )
                if len(errors.messages) > model_errors:
                    # Unlabelled script tables are named from the data model,
                    # so without it they may not match their graph tables.
                    app.tables_read_failed = True
            app.read_errors = errors.messages
            app.sheets_complete = not errors.sheets_unread
            app.read_fallbacks = errors.fallbacks
            return app
        finally:
            self._close_engine(websocket_connection, app_id)

    def _list_items(self, space_id: str) -> Iterable[Dict]:
        try:
            for item in self._paginate(f"{self.rest_api_url}/items?spaceId={space_id}"):
                # spaceId none indicates item present in personal space
                if not item.get(Constant.SPACEID):
                    item[Constant.SPACEID] = Constant.PERSONAL_SPACE_ID
                if self.config.space_pattern.allowed(
                    self.spaces[item[Constant.SPACEID]]
                ):
                    yield item
        except Exception as e:
            self._record_unread(
                f"items of space {space_id}",
                f"Unable to fetch items. Exception: {e}",
                e,
            )

    def _load_item(
        self, item: Dict, is_unchanged: Optional[IsUnchanged]
    ) -> Optional[Item]:
        resource_type = item[Constant.RESOURCETYPE]
        resource_id = item[Constant.RESOURCEID]
        try:
            if resource_type == Constant.APP:
                return self._get_app(
                    resource_id,
                    # A script app's node in the lineage graph is a dataprep
                    # resource; an analytics app's is a sense resource.
                    script_app=item.get("resourceSubType") == "script",
                )
            if resource_type == Constant.DATASET:
                return self._get_dataset(resource_id, item[Constant.ID])
        except Exception as e:
            self._record_unread(
                resource_id,
                f"Unable to fetch {resource_type} with id {resource_id}. Exception: {e}",
                e,
            )
        return None
