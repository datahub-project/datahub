import logging
from typing import Dict, Iterable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.qlik_sense.app_state import (
    AppCheckpoint,
    UnmappedFileStore,
)
from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.constants import (
    APP_PARTLY_READ_TITLE,
    CHECKPOINT_LOAD_FAILED_TITLE,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    Item,
    QlikDataset,
    UnchangedApp,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
    app_table_identifier,
)
from datahub.ingestion.source.qlik_sense.source.onprem_binary import OnPremBinaryMixin
from datahub.ingestion.source.qlik_sense.source_models import (
    _PartialRead,
    _QvdsUnknownReason,
    _StorageFile,
)
from datahub.ingestion.source.qlik_sense.source_utils import (
    _app_field_names,
    _own_table_urns,
)
from datahub.ingestion.source.state.entity_removal_state import (
    STATEFUL_INGESTION_IGNORED_ENTITY_TYPES,
)
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


class ItemsMixin(OnPremBinaryMixin):
    def _keep_previous_read(self, app_id: str) -> Optional[AppCheckpoint]:
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            # Reporting a failure stops stale entity removal from soft-deleting
            # what the app's last read produced.
            self.reporter.failure(
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="Entities from the app's last read could not be kept, so "
                "stale metadata removal is skipped for this run.",
                context=app_id,
                exc=e,
            )
            self._apps_tables_unknown.add(app_id)
            self._apps_qvds_unknown[app_id] = _QvdsUnknownReason.CHECKPOINT_UNREAD
            return None
        if previous is None:
            self._apps_tables_unknown.add(app_id)
            self._apps_qvds_unknown[app_id] = _QvdsUnknownReason.NO_CHECKPOINT
            return None
        self._kept_app_ids.add(app_id)
        if app_id in self._binary_source_apps:
            self._app_urns.setdefault(app_id, set()).update(previous.urns)
        if previous.binary_tables:
            self._apps_with_binary_tables.add(app_id)
        if previous.data_model_fields is not None:
            self._app_model_fields[app_id] = previous.data_model_fields
        if previous.qvds is None:
            self._apps_qvds_unknown[app_id] = _QvdsUnknownReason.RECORDED_BEFORE_QVDS
        else:
            self._app_qvds[app_id] = previous.qvds
            if not previous.qvds.writes_complete:
                self._apps_qvds_unknown[app_id] = _QvdsUnknownReason.UNRESOLVED_STORE
        if previous.field_names:
            self._learn_field_names(previous.field_names)
        unmapped_stores: Dict[str, UnmappedFileStore] = {}
        stored_file_urns = list(previous.stored_file_urns)
        for file_urn, store in previous.unmapped_file_stores.items():
            storage_file = self._storage_of_unmapped_store(store)
            if storage_file is None:
                unmapped_stores[file_urn] = store
                self._add_unmapped_file(file_urn, store.path)
                urns = self._unmapped_file_stores[file_urn].setdefault(app_id, [])
                urns.extend(urn for urn in store.table_urns if urn not in urns)
                continue
            # Its storage is known now, so the file is the storage file a full
            # read links, not the Qlik dataset named by its connection.
            if storage_file.urn is not None:
                tables = self._kept_storage_file_stores.setdefault(storage_file.urn, [])
                tables.extend(urn for urn in store.table_urns if urn not in tables)
                if storage_file.urn not in stored_file_urns:
                    stored_file_urns.append(storage_file.urn)
        dropped = (
            set(previous.unmapped_file_stores)
            - set(unmapped_stores)
            - set(previous.read_unmapped_file_urns)
        )
        previous = previous.model_copy(
            update={
                "urns": [urn for urn in previous.urns if urn not in dropped],
                "unmapped_file_stores": unmapped_stores,
                "stored_file_urns": sorted(stored_file_urns),
            }
        )
        if previous.table_urns_by_qri is not None:
            kept = _own_table_urns(app_id, previous.table_urns_by_qri)
            self._app_table_urns_by_qri.update(kept)
            self._table_urns_by_app[app_id] = set(kept.values())
        # None was recorded before table URNs were kept in the state.
        if previous.table_urns_by_qri is None or previous.tables_unknown:
            self._apps_tables_unknown.add(app_id)
        else:
            self._apps_tables_known.add(app_id)
            if previous.graph_tables_unresolved:
                self._apps_graph_tables_unresolved.add(app_id)
        assert self.stale_entity_removal_handler is not None
        for urn in previous.urns:
            self._keep_in_stale_state(urn)
        return previous

    def _storage_of_unmapped_store(
        self, store: UnmappedFileStore
    ) -> Optional[_StorageFile]:
        """The storage file a kept app's store into a file on a connection of
        unknown storage now resolves to, or None while that storage is still
        unknown, as when the data connections or Qlik datasets could not be
        listed."""
        lib_path = LIB_PATH_RE.match(store.path)
        if lib_path is None:
            return None
        # The recorded connection is space-qualified, so no app space applies.
        storage, detail = self._file_storage(lib_path.group("connection"), "")
        if storage is None:
            return None
        path = lib_path.group("path")
        catalogued = self._catalogued_as_qlik_dataset(storage, path)
        # A full read keeps last run's edges when the datasets can't be listed.
        if catalogued is None:
            return None
        # A file Qlik catalogues is not linked on storage.
        if catalogued:
            return _StorageFile(urn=None)
        return _StorageFile(urn=self._file_urn(storage, detail, path))

    def _keep_unchanged_app(self, app: UnchangedApp) -> None:
        previous = self._keep_previous_read(app.id)
        # Without the kept entities, which _keep_previous_read reports as a
        # failure, nothing is recorded, so the next run reads the app again.
        if previous is not None and previous.urns:
            self.app_state.record(
                app.id,
                AppCheckpoint(
                    fingerprint=self._salted_fingerprint(app.fingerprint),
                    urns=previous.urns,
                    table_urns_by_qri=previous.table_urns_by_qri,
                    tables_unknown=previous.tables_unknown,
                    graph_tables_unresolved=previous.graph_tables_unresolved,
                    qvds=self._app_qvds.get(app.id),
                    data_model_fields=self._app_model_fields.get(app.id),
                    binary_tables=previous.binary_tables,
                    stored_file_urns=previous.stored_file_urns,
                    unmapped_file_stores=previous.unmapped_file_stores,
                    read_unmapped_file_urns=previous.read_unmapped_file_urns,
                    read_graph_named_urns=previous.read_graph_named_urns,
                    field_names=previous.field_names,
                ),
            )
        self.reporter.apps_skipped_unchanged += 1

    def _keep_partly_read_app(self, app: App) -> _PartialRead:
        """Keeps what the app's last read produced instead of emitting a partial
        read, which would overwrite its relationships, for up to
        max_partial_reads_kept consecutive runs."""
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        for failure in failures:
            self.reporter.warning(
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="The partly read app could not keep its last read.",
                context=failure,
            )
        if previous is None:
            return _PartialRead.NOT_KEPT
        if previous.partial_reads >= self.config.max_partial_reads_kept:
            return _PartialRead.LIMIT_REACHED
        kept = self._keep_previous_read(app.id)
        if kept is None or not kept.urns:
            return _PartialRead.NOT_KEPT
        # Its QVD writes are from its last full read and may have changed, so
        # they protect its QVD edges without replacing anyone else's.
        self._apps_qvds_unknown.setdefault(app.id, _QvdsUnknownReason.PARTLY_READ)
        # No fingerprint, so the next run reads the app again.
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=None,
                urns=kept.urns,
                partial_reads=previous.partial_reads + 1,
                table_urns_by_qri=previous.table_urns_by_qri,
                tables_unknown=previous.tables_unknown,
                graph_tables_unresolved=previous.graph_tables_unresolved,
                qvds=previous.qvds,
                data_model_fields=previous.data_model_fields,
                binary_tables=previous.binary_tables,
                stored_file_urns=kept.stored_file_urns,
                unmapped_file_stores=kept.unmapped_file_stores,
                read_unmapped_file_urns=previous.read_unmapped_file_urns,
                read_graph_named_urns=previous.read_graph_named_urns,
                field_names=previous.field_names,
            ),
        )
        return _PartialRead.KEPT

    def _gen_recorded_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        fingerprint = (
            self._salted_fingerprint(app.fingerprint)
            if app.fingerprint
            and app.read_complete
            and not app.data_load_failed
            # Its connection paths were unresolved, so it is read again next run.
            and not self._data_connections_unread()
            else None
        )
        urns: Dict[str, None] = {}
        for wu in self._gen_app_workunit(app, fingerprint):
            urn = wu.get_urn()
            if guess_entity_type(urn) not in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES:
                urns[urn] = None
            yield wu
        if not app.read_complete:
            # Only reached without a usable last read, and reported as a failure.
            # Recording the partial entity list as the app's last read would let
            # a later partial read keep only those entities.
            return
        # A complete read records only what it produced, so entities removed in
        # Qlik are soft-deleted even when the change check keeps failing. No
        # fingerprint never matches, so such an app is read again next run.
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=fingerprint,
                urns=list(urns),
                table_urns_by_qri=self._app_table_urns(app),
                # So a later run that keeps the app can't take its tables as
                # complete, nor a 404 as the app having no tables.
                tables_unknown=self._tables_unknown(app),
                graph_tables_unresolved=self._graph_tables_unresolved(app),
                qvds=self._app_qvds.get(app.id),
                data_model_fields=self._app_model_fields.get(app.id),
                binary_tables=app.id in self._apps_with_binary_tables,
                stored_file_urns=self._app_stored_files.get(app.id, []),
                unmapped_file_stores={
                    file_urn: UnmappedFileStore(
                        path=self._unmapped_files[file_urn], table_urns=stores[app.id]
                    )
                    for file_urn, stores in self._unmapped_file_stores.items()
                    if app.id in stores
                },
                field_names=_app_field_names(app),
            ),
        )

    def _gen_app_workunit(
        self, app: App, fingerprint: Optional[str] = None
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik App to Datahub container
        """
        owner_username = self._get_owner_name(app.ownerId)
        self._data_model_fields = {
            app_table_identifier(app.id, table.name): table.fields
            for table in app.data_model_tables
        }
        self._learn_field_names(_app_field_names(app))
        if self._on_cloud and not app.tables:
            self.reporter.apps_without_load_model.append(app.id)
        parsed = self._parsed_script(app)
        self._file_sources_by_app[app.id] = self._file_source_resolver(app, parsed)
        if parsed.has_includes:
            self._apps_with_includes.add(app.id)
        if app.variables_read_failed:
            self._apps_variables_unread.add(app.id)
            self.reporter.warning(
                title="Unable to read Qlik app variables",
                message="Load script paths and chart expressions that use these "
                "apps' variables are only expanded from `script_variables`. "
                "Lineage to the files they load is only added to this run, and "
                "chart fields read through other variables are left out.",
                context=app.id,
            )
        self._app_space_ids[app.id] = app.spaceId
        script_tables = self._build_script_tables(app, parsed=parsed)
        self._name_unknown_tables(app, script_tables)
        qvd_lineage = (
            self._app_qvd_lineage(app, parsed, script_tables)
            if self.config.on_prem is not None
            else None
        )
        binary_load = None
        if qvd_lineage is not None:
            script_tables = self._merge_tables(script_tables, qvd_lineage.tables)
            # Read, even with no tables, so a BINARY load from it isn't
            # reported as from an app the run didn't read.
            if app.id in self._binary_source_apps:
                self._app_urns.setdefault(app.id, set())
            self._record_model_fields(app)
            binary_load = self._binary_load(app, parsed)
            if binary_load.tables:
                self._apps_with_binary_tables.add(app.id)
            script_tables = self._merge_tables(script_tables, binary_load.tables)
        self._use_data_model_field_names(script_tables)
        matched_script_tables = self._match_script_tables(
            app.lineage_tables, script_tables
        )
        matched_ids = {id(table) for table in matched_script_tables.values()}
        script_tables = [
            table for table in script_tables if id(table) not in matched_ids
        ]
        table_fields = self._app_table_fields(app.tables, script_tables)
        script_table_urns = {
            self._gen_qlik_dataset_urn(table.identifier) for table in script_tables
        }
        self._register_app_tables(app)
        for table in app.lineage_tables:
            matched = matched_script_tables.get(table.qri)
            urn = self._gen_qlik_dataset_urn(
                self._lineage_table_identifier(app.id, table)
            )
            table_fields[urn] = self._table_schema(
                self._lineage_table_identifier(app.id, table),
                self._lineage_table_fields(table, matched),
            )
            if matched:
                script_table_urns.add(urn)
        input_tables_urns = list(table_fields)
        container_workunits = list(
            gen_containers(
                container_key=self._gen_app_key(app.id),
                name=app.qTitle,
                description=app.description,
                # "Qlik App" first, as the UI labels a container by its first
                # subtype; "Workbook" makes the UI treat the app as its charts'
                # workbook, as it does for Tableau.
                sub_types=[
                    BIContainerSubTypes.QLIK_APP,
                    BIContainerSubTypes.WORKBOOK,
                ],
                parent_container_key=self._gen_space_key(app.spaceId),
                extra_properties={
                    Constant.QRI: app.qri,
                    **({Constant.USAGE: app.qUsage} if app.qUsage else {}),
                },
                owner_urn=(
                    builder.make_user_urn(owner_username)
                    if self.config.ingest_owner and owner_username
                    else None
                ),
                external_url=f"https://{self.ui_host}/sense/app/{app.id}/overview",
                created=int(app.createdAt.timestamp() * 1000),
                last_modified=int(app.updatedAt.timestamp() * 1000),
            )
        )
        yield from container_workunits
        app_fields = self._index_app_fields(table_fields, script_table_urns)
        has_unknown_schema = any(not fields for fields in table_fields.values())
        chart_inputs = (
            None
            if app.tables_read_failed
            else self._chart_inputs(
                (chart for sheet in app.sheets for chart in sheet.charts),
                self._app_variables(app),
                app_fields,
                input_tables_urns,
                has_unknown_schema,
            )
        )
        yield from self._gen_app_dashboard_workunit(
            app,
            owner_username,
            None
            if chart_inputs is None
            else self._tables_without_chart_path(app, chart_inputs, input_tables_urns),
        )
        yield from self._gen_sheets_workunit(app, chart_inputs)
        yield from self._gen_app_tables_workunit(app.tables, app.id)
        yield from self._gen_lineage_tables_workunit(app, matched_script_tables)
        yield from self._gen_script_tables_workunit(
            script_tables,
            app.id,
            # A table the lineage graph could not be read for may be one an
            # earlier run merged with a graph table, whose other edges stay.
            replace_lineage=not app.tables_read_failed,
            binary_links=binary_load.links if binary_load is not None else {},
        )
        if qvd_lineage is not None:
            yield from self._gen_file_entities(
                qvd_lineage.paths, DatasetSubTypes.QLIK_QVD
            )
        if binary_load is not None:
            yield from self._gen_file_entities(
                binary_load.paths, DatasetSubTypes.QLIK_APP_FILE
            )
        if qvd_lineage is None:
            self._queue_file_store_lineage(app, parsed, table_fields)
        if fingerprint:
            # Skipping an unchanged app trusts this fingerprint, so it is written
            # only after everything else the app produced.
            fingerprint_wu = self._gen_fingerprint_workunit(
                container_workunits, fingerprint
            )
            if any(p.app_id == app.id for p in self._pending_table_lineage) or any(
                p.app_id == app.id for p in self._pending_stored_files
            ):
                self._deferred_fingerprints[app.id] = fingerprint_wu
            else:
                yield fingerprint_wu

    def _check_missing_lineage_graph(self, app: App) -> None:
        # Qlik also answers 404 for lineage it has not computed yet, so a 404
        # only means the app has no tables when its last read had none.
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        if failures:
            # Without the last read, the app's tables may still exist.
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, and its last read "
                "could not be loaded"
            )
            app.tables_read_failed = True
        elif previous is not None and (
            previous.table_urns_by_qri or previous.graph_tables_unresolved
        ):
            # Unresolved graph tables are tables the last read saw too.
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, whose last read "
                "had tables"
            )
            app.tables_read_failed = True
        else:
            self.reporter.apps_without_lineage_graph.append(app.id)
            # A last read whose tables are unknown can't tell either.
            unknown = (
                previous is None
                or previous.table_urns_by_qri is None
                or previous.tables_unknown
            )
            if unknown and self.app_state.is_checkpointing_enabled():
                # With no last read to compare with, the graph may only be
                # missing because Qlik has not computed it yet, such as right
                # after a reload, so the app's lineage is only added to. Without
                # stateful ingestion there is never a last read, so the app is
                # taken as having no tables, as before.
                app.tables_read_failed = True
                self.reporter.warning(
                    title="Qlik Cloud has no lineage graph for app",
                    message="Qlik may not have computed it yet, so the lineage "
                    "of the app's tables is only added to this run.",
                    context=app.id,
                )

    def _gen_item_workunits(
        self,
        item: Union[Item, UnchangedApp],
        space_name: str,
        record_state: bool,
    ) -> Iterable[MetadataWorkUnit]:
        if isinstance(item, UnchangedApp):
            self._keep_unchanged_app(item)
        elif isinstance(item, App):
            for fallback in item.read_fallbacks:
                self.reporter.info(
                    title="Qlik Engine batch read unavailable",
                    message="Fell back to one Engine request per chart or master "
                    "measure, which is slower on large apps.",
                    context=f"{item.id}: {fallback}",
                )
            self.reporter.apps_read += 1
            if item.lineage_graph_missing:
                self._check_missing_lineage_graph(item)
            if self.config.on_prem is not None and record_state:
                self._check_binary_read(item)
            logger.info(
                f"Processed app {item.qTitle} in space {space_name} "
                f"({self.reporter.apps_read} apps read from the Engine)"
            )
            partial_read = (
                self._keep_partly_read_app(item)
                if not item.read_complete and record_state
                else _PartialRead.NOT_KEPT
            )
            if partial_read == _PartialRead.KEPT:
                self.reporter.warning(
                    title=APP_PARTLY_READ_TITLE,
                    message="Nothing from this read of the app is emitted. Its "
                    "metadata stays as of its last complete read until a read of "
                    "it completes.",
                    context=self._read_errors_context(item),
                )
                return
            if partial_read == _PartialRead.LIMIT_REACHED:
                self.reporter.failure(
                    title="Qlik app has been only partly read for too many runs",
                    message="Its last complete read is no longer kept, so this "
                    "partial read is ingested. Stale metadata removal is skipped "
                    "for this run.",
                    context=self._read_errors_context(item),
                )
            elif not item.read_complete:
                # Reporting a failure also stops stale entity removal from
                # soft-deleting what this app's last full read produced.
                self.reporter.failure(
                    title=APP_PARTLY_READ_TITLE,
                    message="Some sheets, charts, tables or load script lineage "
                    "of this app could not be read from Qlik.",
                    context=self._read_errors_context(item),
                )
            if item.script_read_failed:
                self.reporter.warning(
                    title="Unable to read Qlik app load script",
                    message="Load script lineage of this app is skipped. Reading "
                    "load scripts needs the Owner, Can manage or Can operate role "
                    "in managed spaces.",
                    context=item.id,
                )
            yield from (
                self._gen_recorded_app_workunit(item)
                if record_state
                else self._gen_app_workunit(item)
            )
        elif isinstance(item, QlikDataset):
            yield from self._gen_dataset_workunit(item)

    @staticmethod
    def _read_errors_context(app: App) -> str:
        first, *rest = app.read_errors
        more = f" (and {len(rest)} more)" if rest else ""
        return f"{app.id}: {first}{more}"
