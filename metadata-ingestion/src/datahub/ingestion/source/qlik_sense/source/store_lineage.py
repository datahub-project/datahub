from typing import Dict, Iterable, List, Optional, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.file_lineage import FilePlatform
from datahub.ingestion.source.qlik_sense.lib_path import is_data_files
from datahub.ingestion.source.qlik_sense.lineage_utils import stale_field_lineage
from datahub.ingestion.source.qlik_sense.qvd_lineage import QvdWrite, stored_fields
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ParsedScript,
    QvdStore,
    app_table_identifier,
    stores_in_competing_branches,
)
from datahub.ingestion.source.qlik_sense.source.onprem_qvd import OnPremQvdMixin
from datahub.ingestion.source.qlik_sense.source_models import PendingStoredFile
from datahub.ingestion.source.qlik_sense.source_utils import (
    patch_workunits,
    qlik_field_id,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
)
from datahub.specific.dataset import DatasetPatchBuilder


class StoreLineageMixin(OnPremQvdMixin):
    def _queue_file_store_lineage(
        self,
        app: App,
        parsed: ParsedScript,
        table_fields: Dict[str, List[QlikDatasetSchemaField]],
    ) -> None:
        """Lineage into the QVD, text and Parquet files an app's load script
        stores on external storage, such as Azure Storage. Other apps may store
        into the same file, so only this app's tables' edges are replaced, and
        only when every file it stores into is known."""
        stores = [*parsed.qvd_stores, *parsed.file_stores]
        # Whether the files the app stores into are all known this run.
        complete = (
            app.read_complete
            and not parsed.has_unresolved_store
            and not parsed.has_unresolved_file_store
            and app.id not in self._apps_with_includes
            and not app.variables_read_failed
            # An unread script hides its STOREs.
            and app.script is not None
            and not app.script_read_failed
            and not self._data_connections_unread()
            and self._dataset_urns_by_secure_qri is not None
        )
        file_urns: Dict[int, str] = {}
        unmapped_indexes: Set[int] = set()
        for index, store in enumerate(stores):
            if store.file.connection is None:
                continue
            match = self._file_storage(store.file.connection, app.spaceId)
            storage, detail = match.storage, match.detail
            if storage is None:
                if is_data_files(store.file.connection):
                    continue
                info = self._find_data_connection(store.file.connection, app.spaceId)
                # A visible connection of another connector, such as SFTP, has
                # no storage to map; its file is named by connection and path,
                # as its readers' lineage graphs name it. So is a visible Data
                # Gateway connection, until it is mapped to its share.
                unmapped_share = (
                    info is not None
                    and info.file_platform == FilePlatform.WINDOWS_SHARE
                )
                listed_without_storage = info is not None and (
                    info.file_platform is None or unmapped_share
                )
                if not listed_without_storage:
                    # Its file may be a storage file the app stored into before.
                    complete = False
                unmapped_urn = self._unmapped_storage_file_urn(
                    store.file.connection,
                    store.file.path,
                    app.spaceId,
                    report=not listed_without_storage or unmapped_share,
                )
                if unmapped_urn is not None:
                    file_urns[index] = unmapped_urn
                    unmapped_indexes.add(index)
                continue
            catalogued = self._catalogued_as_qlik_dataset(storage, store.file.path)
            if catalogued is None:
                complete = False
            # A file Qlik catalogues is the Qlik dataset that loads of it and
            # its own lineage graph name.
            if catalogued is False:
                file_urns[index] = self._file_urn(storage, detail, store.file.path)
        table_urns = set(table_fields)
        urns_by_store = {id(store): file_urns.get(i) for i, store in enumerate(stores)}

        def same_file(store: QvdStore, other: QvdStore) -> bool:
            # Two names of one connection, such as with and without its space,
            # store into the same file.
            urn = urns_by_store[id(store)]
            return store.file.key == other.file.key or (
                urn is not None and urn == urns_by_store[id(other)]
            )

        competing = stores_in_competing_branches(
            stores,
            lambda store: (
                self._stored_table_urn(app, store.label, table_urns)
                or store.label.lower()
            ),
            same_file,
        )
        if competing:
            complete = False
            self._report_competing_stores(app, competing)
        writes: Dict[str, List[QvdWrite]] = {}
        # A STORE without a format writes a QVD, whatever the file is named.
        qvd_file_urns = {
            file_urn
            for index, file_urn in file_urns.items()
            if index < len(parsed.qvd_stores)
        }
        for index, store in enumerate(stores):
            file_urn = file_urns.get(index)
            if file_urn is None or store in competing:
                continue
            table_urn = self._stored_table_urn(app, store.label, table_urns)
            # A table the app doesn't ingest, such as one a script variable
            # names, has no dataset to link, and may be one the file's
            # existing edges come from.
            if table_urn is None:
                complete = False
                continue
            if index in unmapped_indexes:
                # Linked at the end of the run, once an app that reads the
                # file is known.
                urns = self._unmapped_file_stores[file_urn].setdefault(app.id, [])
                if table_urn not in urns:
                    urns.append(table_urn)
                continue
            write = QvdWrite(
                table_urn=table_urn,
                fields=stored_fields(
                    store, [field.name for field in table_fields.get(table_urn, [])]
                ),
            )
            if write not in writes.setdefault(file_urn, []):
                writes[file_urn].append(write)
                self.reporter.file_stores_linked += 1
                self._note_storage_qvd(file_urn)
        previous = self._usable_checkpoint(app.id, [])
        previous_files = previous.stored_file_urns if previous else []
        currents = {
            file_urn: self._current_upstreams(file_urn) if complete else None
            for file_urn in sorted({*writes, *previous_files})
        }
        # Files the app may still have edges into, so a later run that knows
        # all its stores and can read their lineage removes them.
        self._app_stored_files[app.id] = sorted(
            file_urn
            for file_urn, current in currents.items()
            if file_urn in writes or current is None
        )
        app_table_urns = {
            *table_urns,
            *self._app_table_urns(app).values(),
            *((previous.table_urns_by_qri or {}).values() if previous else []),
        }
        for file_urn, current in currents.items():
            if file_urn not in writes and current is None:
                continue
            self._pending_stored_files.append(
                PendingStoredFile(
                    app_id=app.id,
                    file_urn=file_urn,
                    writes=writes.get(file_urn, []),
                    current=current,
                    app_table_urns=app_table_urns,
                    has_field_schema=file_urn not in qvd_file_urns,
                )
            )

    def _gen_pending_stored_files(self) -> Iterable[MetadataWorkUnit]:
        shared_by_app: Dict[str, Optional[Set[str]]] = {}
        for pending in self._pending_stored_files:
            if pending.app_id not in shared_by_app:
                shared_by_app[pending.app_id] = self._table_urns_of_other_apps(
                    pending.app_id
                )
            shared = shared_by_app[pending.app_id]
            # A table another app also has, such as a QVD both load, may be
            # what that app stores into the file, so its edge is never this
            # app's to remove.
            owned = set() if shared is None else pending.app_table_urns - shared
            if shared is None:
                # Nothing is removed this run, so a later run must still know
                # the app may have edges into the file.
                self._keep_stored_file(pending.app_id, pending.file_urn)
            yield from self._gen_qvd_upstreams(
                pending.file_urn,
                pending.writes,
                pending.current,
                owned.__contains__,
                has_field_schema=pending.has_field_schema,
            )
        self._pending_stored_files = []

    def _keep_stored_file(self, app_id: str, file_urn: str) -> None:
        files = self._app_stored_files.setdefault(app_id, [])
        if file_urn not in files:
            files.append(file_urn)
            files.sort()
        self.app_state.add_stored_files(app_id, [file_urn])

    def _table_urns_of_other_apps(self, app_id: str) -> Optional[Set[str]]:
        """The tables of the other apps read or kept this run, or recorded by
        the last run; None when another app's tables may be missing from them,
        as when part of the run or the last run's checkpoints were not read."""
        if self.qlik_api.unread_items or any(
            other != app_id for other in self._apps_tables_unknown
        ):
            return None
        urns = {
            urn
            for other, table_urns in self._table_urns_by_app.items()
            if other != app_id
            for urn in table_urns
        }
        try:
            last_apps = self.app_state.previous_apps()
        except Exception as e:
            self.reporter.warning(
                title="Unable to load the last checkpoints of Qlik apps",
                message="Which tables other apps share with the app is unknown, "
                "so its edges into the files it stores into are only added to.",
                context=app_id,
                exc=e,
            )
            return None
        if last_apps is None:
            return None
        for other, checkpoint in last_apps.items():
            if other != app_id:
                urns.update((checkpoint.table_urns_by_qri or {}).values())
        return urns

    def _gen_stored_file_lineage(
        self, file_urn: str, table_urns: Iterable[str]
    ) -> Iterable[MetadataWorkUnit]:
        patch = DatasetPatchBuilder(file_urn)
        for urn in table_urns:
            patch.add_upstream_lineage(
                Upstream(dataset=urn, type=DatasetLineageType.COPY)
            )
        # Only added to: other apps, read or not, may store into the same file.
        yield from patch_workunits(patch, is_primary_source=False)

    def _report_competing_stores(self, app: App, stores: List[QvdStore]) -> None:
        stored = [
            f"{store.label} INTO lib://{store.file.connection}/{store.file.path}"
            if store.file.connection
            else f"{store.label} INTO {store.file.path}"
            for store in stores
        ]
        self.reporter.qvd_stores_in_competing_branches.extend(
            f"{app.id}: {store}" for store in stored
        )
        self.reporter.warning(
            title="Qlik STOREs in competing branches are not linked",
            message="The app stores a table into different files from different "
            "branches of an IF or SWITCH, so which file it wrote can't be told "
            "and none of them is linked.",
            context=f"app={app.id}, stores={stored}",
        )

    def _stored_table_urn(
        self, app: App, label: str, table_urns: Set[str]
    ) -> Optional[str]:
        urn = self._gen_qlik_dataset_urn(app_table_identifier(app.id, label))
        if urn in table_urns:
            return urn
        # A load-model table is ingested under its QRI, not the app and label,
        # and the script may name it by its alias.
        urns = {
            self._gen_qlik_dataset_urn(table.tableQri.lower())
            for table in app.tables
            if table.tableQri
            and label.lower() in (table.tableName.lower(), table.tableAlias.lower())
        } & table_urns
        return next(iter(urns)) if len(urns) == 1 else None

    def _gen_stored_table_types(self) -> Iterable[MetadataWorkUnit]:
        """Types for the fields of untyped graph tables, from the datasets they
        are stored into: a STORE writes a field's values unchanged, so a field
        stored under its own name has the stored field's type. A field stored
        into datasets that disagree on its type keeps none."""
        types: Dict[str, Dict[str, Set[Optional[str]]]] = {}
        for dataset_urn, field_sources in self._dataset_store_field_sources.items():
            dataset_types = self._dataset_field_types.get(dataset_urn, {})
            for field, source_qris in field_sources.items():
                for field_qri in source_qris:
                    table_qri, _, field_id = field_qri.rpartition("#")
                    table_urn = self._app_table_urns_by_qri.get(table_qri)
                    if (
                        table_urn in self._untyped_tables
                        and field_id == qlik_field_id(field)
                        and dataset_types.get(field)
                    ):
                        types.setdefault(table_urn, {}).setdefault(field, set()).add(
                            dataset_types[field]
                        )
        for table_urn, field_types in types.items():
            untyped = self._untyped_tables[table_urn]
            identifier, schema = untyped.identifier, untyped.schema_fields
            # Columns traced from a load script may have their case changed by
            # the SQL parser, so a field matches a stored one of another case
            # when no other stored field folds to the same name.
            folded: Dict[str, List[str]] = {}
            for name in field_types:
                folded.setdefault(name.lower(), []).append(name)
            typed = []
            for schema_field in schema:
                matches = folded.get(schema_field.name.lower(), [])
                stored_name: Optional[str] = (
                    schema_field.name
                    if schema_field.name in field_types
                    else (matches[0] if len(matches) == 1 else None)
                )
                stored_types = (
                    field_types.get(stored_name, set()) if stored_name else set()
                )
                typed.append(
                    schema_field.model_copy(
                        update={"dataType": next(iter(stored_types))}
                    )
                    if len(stored_types) == 1
                    else schema_field
                )
            yield self._gen_schema_metadata(identifier, typed)

    def _gen_dataset_store_lineage(self) -> Iterable[MetadataWorkUnit]:
        datasets = self._dataset_urns_by_secure_qri
        qlik_datasets = set(datasets.values()) if datasets is not None else None
        owned = self._known_app_table_urns()
        for dataset_urn, source_qris in self._dataset_store_sources.items():
            resolved = self._resolve_lineage_sources(
                source_qris, dataset_urn, own_app_id=None
            )
            stored_fields = self._graph_column_lineage(
                dataset_urn,
                self._dataset_store_field_sources.get(dataset_urn, {}),
                own_app_id=None,
            )
            if not resolved.complete:
                self.reporter.warning(
                    title="Lineage of Qlik dataset only added to",
                    message="An app table that stores into the dataset could not "
                    "be resolved this run, such as one of an app that could not be "
                    "read, so its storing tables are added to the dataset's "
                    "lineage and none are removed.",
                    context=dataset_urn,
                )
            # Added as a patch, so lineage from elsewhere, such as added in
            # DataHub, stays.
            patch = DatasetPatchBuilder(dataset_urn)
            for urn in resolved.urns:
                patch.add_upstream_lineage(
                    Upstream(dataset=urn, type=DatasetLineageType.COPY)
                )
            for lineage in stored_fields:
                patch.add_fine_grained_lineage(lineage)
            # Edges this connector owns are those from the tables of apps read
            # in full or kept this run. Those that no longer store into the
            # dataset are removed one by one; while a storing app is unread
            # this run, nothing is.
            current = (
                self._current_upstreams(dataset_urn)
                if resolved.complete and qlik_datasets is not None
                else None
            )
            if current is not None:
                for upstream in current.upstreams:
                    if upstream.dataset in owned and upstream.dataset not in (
                        resolved.urns
                    ):
                        patch.remove_upstream_lineage(upstream.dataset)
                if (
                    self.config.include_column_lineage
                    and dataset_urn not in self._dataset_store_fields_unknown
                    and dataset_urn not in self._unnamed_field_lineage
                ):
                    for lineage in stale_field_lineage(
                        current.fineGrainedLineages or [], stored_fields, owned
                    ):
                        patch.remove_fine_grained_lineage(lineage)
            yield from patch_workunits(patch, is_primary_source=True)
