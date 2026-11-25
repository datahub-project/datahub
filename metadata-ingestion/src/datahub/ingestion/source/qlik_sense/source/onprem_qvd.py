from typing import Callable, Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.constants import (
    CHECKPOINT_LOAD_FAILED_TITLE,
    UNRESOLVED_STORE,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.qvd_lineage import (
    CANONICAL_ABSOLUTE_PATH_RE,
    AppQvdLineage,
    QvdPathResolver,
    QvdWrite,
    app_qvd_lineage,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ParsedScript,
    ScriptTableLineage,
)
from datahub.ingestion.source.qlik_sense.source.reload_sql import ReloadSqlMixin
from datahub.ingestion.source.qlik_sense.source_utils import (
    _bounded_list,
    _patch_workunits,
    _qvd_fields,
    _schema_field_parent,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL, SchemaMetadata
from datahub.metadata.urns import DatasetUrn
from datahub.specific.dataset import DatasetPatchBuilder


class OnPremQvdMixin(ReloadSqlMixin):
    def _app_qvd_lineage(
        self, app: App, parsed: ParsedScript, script_tables: List[ScriptTableLineage]
    ) -> AppQvdLineage:
        resolver = self._qvd_path_resolver(app)
        lineage = app_qvd_lineage(
            app.id,
            parsed,
            resolver,
            self._gen_qlik_dataset_urn,
            table_fields={
                **{
                    table.identifier: list(table.column_upstreams)
                    for table in script_tables
                    if not table.mapping
                },
                **{
                    identifier: [field.name for field in fields]
                    for identifier, fields in self._data_model_fields.items()
                },
            },
        )
        for path in [*parsed.unresolved_qvd_paths, *resolver.unresolved]:
            self.reporter.unresolved_qvd_paths.append(f"{app.id}: {path}")
            self._unresolved_qvd_loads += 1
        for path in parsed.unlabelled_qvd_loads:
            self.reporter.unlabelled_qvd_loads.append(f"{app.id}: {path}")
            self._unlabelled_qvd_loads += 1
        self.reporter.qvds_found += len(lineage.paths)
        self.reporter.storage_qvds_resolved += len(lineage.storage_urns)
        if app.script is None or app.script_read_failed:
            # QVDs the script writes can't be known, so their lineage is kept.
            self._apps_qvds_unknown[app.id] = "its load script could not be read"
            return lineage
        if self._data_connections_unread():
            # Without the folder connections, the lib:// QVDs it writes are
            # unknown.
            unknown_reason: Optional[str] = "data connections could not be listed"
        elif parsed.has_unresolved_store or lineage.has_unresolved_store:
            # Such as a loop STORE, or a path built from a variable that failed
            # to read.
            unknown_reason = UNRESOLVED_STORE
        else:
            unknown_reason = None
        if unknown_reason:
            # The writes that resolved, such as to a mapped storage connection,
            # are still added, and the app's edges into QVDs are kept.
            lineage.qvds.writes_complete = False
            self._apps_qvds_unknown[app.id] = unknown_reason
        self._app_qvds[app.id] = lineage.qvds
        return lineage

    def _qvd_path_resolver(self, app: App) -> QvdPathResolver:
        on_prem = self.config.on_prem
        assert on_prem is not None
        return QvdPathResolver(
            folders={
                name: info.folder_path
                for name, info in self._get_data_connections().items()
                if info.folder_path
            },
            ignored_connections=on_prem.qvd_ignored_connections,
            folder_aliases=on_prem.qvd_folder_aliases,
            variables_known=not app.variables_read_failed,
            storage_urns=self._qvd_storage_urns,
        )

    def _gen_qvd_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Schema and lineage of on-prem QVDs, from every app that writes or
        reads them this run or was kept from an earlier one."""
        self._report_unresolved_qvds()
        writes: Dict[str, List[QvdWrite]] = {}
        reads: Dict[str, List[str]] = {}
        for qvds in self._app_qvds.values():
            for urn, app_writes in qvds.writes.items():
                writes.setdefault(urn, []).extend(app_writes)
            for urn, read_fields in qvds.reads.items():
                known = reads.setdefault(urn, [])
                known.extend(field for field in read_fields if field not in known)
        # Storage files are never removed as stale, so the edges of apps that
        # stopped storing into them are only removed here.
        stopped = self._storage_qvds_written_before()
        if not (writes or reads or stopped):
            return
        unknown = dict(self._apps_qvds_unknown)
        if self.qlik_api.unread_items:
            unknown["apps that could not be listed"] = "they were not read"
        if unknown:
            # An app whose QVD writes are unknown may write any QVD, so its
            # edges are kept and a QVD only it writes keeps its last schema.
            self.reporter.warning(
                title="Lineage of Qlik QVDs only added to",
                message="The QVDs some apps write are not known this run, so "
                "their edges into QVDs are kept, and only QVDs written by an app "
                "read this run get a new schema.",
                context=_bounded_list(
                    [f"{app}: {reason}" for app, reason in sorted(unknown.items())]
                ),
            )
        qvd_urns = sorted({*writes, *reads, *stopped})
        # Read in batches up front rather than one request per QVD. Removal is
        # limited to edges from apps whose writes are known, so it runs even
        # while other apps are unknown.
        current_lineage = self._current_upstreams_batch(qvd_urns)
        for urn in qvd_urns:
            # The fields apps read are a subset, so a QVD whose writer is
            # unknown or not read this run keeps its schema.
            fields = (
                _qvd_fields(writes.get(urn, []), reads.get(urn, []))
                if urn in writes
                or not (
                    unknown or self._has_unread_qvd_writer(current_lineage.get(urn))
                )
                else None
            )
            if fields is not None and self._is_own_dataset(urn):
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=SchemaMetadata(
                        schemaName=urn,
                        platform=builder.make_data_platform_urn(self.platform),
                        version=0,
                        hash="",
                        platformSchema=MySqlDDL(tableSchema=""),
                        fields=self._gen_schema_fields(
                            [QlikDatasetSchemaField(name=field) for field in fields]
                        ),
                    ),
                ).as_workunit()
            # A QVD no app writes any more still loses its old writers' edges.
            yield from self._gen_qvd_upstreams(
                urn,
                writes.get(urn, []),
                current_lineage.get(urn),
                self._owned_qvd_writer,
                # The storage sources don't read QVDs, so a QVD on storage has
                # no fields in DataHub.
                has_field_schema=self._is_own_dataset(urn),
            )

    def _storage_qvds_written_before(self) -> Set[str]:
        """Storage QVDs that apps whose writes are known this run stored into
        on their last read."""
        failures: List[str] = []
        urns: Set[str] = set()
        for app_id in self._app_qvds:
            if app_id in self._apps_qvds_unknown:
                continue
            previous = self._usable_checkpoint(app_id, failures)
            if previous is not None and previous.qvds is not None:
                urns.update(
                    urn for urn in previous.qvds.writes if not self._is_own_dataset(urn)
                )
        if failures:
            self.reporter.warning(
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="Edges from these apps into storage QVDs they no longer "
                "write are kept.",
                context=_bounded_list(failures),
            )
        return urns

    def _report_unresolved_qvds(self) -> None:
        if not (self._unresolved_qvd_loads or self._unlabelled_qvd_loads):
            return
        self.reporter.warning(
            title="Some Qlik QVD loads have no lineage",
            message="QVD paths that name no single file the connector can "
            "resolve, such as a wildcard, an unexpanded variable, a relative path "
            "or an unknown data connection, and QVDs loaded without a table label, "
            "are not linked. See unresolved_qvd_paths and unlabelled_qvd_loads.",
            context=f"{self._unresolved_qvd_loads} unresolved QVD paths, "
            f"{self._unlabelled_qvd_loads} unlabelled QVD loads",
        )

    def _qvd_writer_app_id(self, urn: str) -> Optional[str]:
        """The app id of an on-prem app table. QVDs are named by their file
        path, and app tables by their app id."""
        try:
            dataset = DatasetUrn.from_string(urn)
        except Exception:
            return None
        if dataset.platform != builder.make_data_platform_urn(self.platform):
            return None
        name = dataset.name
        instance = self.config.platform_instance
        if instance and name.startswith(f"{instance}."):
            name = name[len(instance) + 1 :]
        if CANONICAL_ABSOLUTE_PATH_RE.match(name):
            return None
        return name.split(".", 1)[0]

    def _owned_qvd_writer(self, urn: str) -> bool:
        """An app table of an app whose QVD writes are known this run, so its
        edges into QVDs are this connector's to remove."""
        app_id = self._qvd_writer_app_id(urn)
        return (
            app_id is not None
            and app_id in self._app_qvds
            and app_id not in self._apps_qvds_unknown
        )

    def _has_unread_qvd_writer(self, current: Optional[UpstreamLineage]) -> bool:
        """Whether an app this run did not read, such as one outside the
        recipe's streams, wrote the QVD, so its schema is that app's."""
        return current is not None and any(
            self._qvd_writer_app_id(upstream.dataset) is not None
            and not self._owned_qvd_writer(upstream.dataset)
            for upstream in current.upstreams
        )

    def _gen_qvd_upstreams(
        self,
        qvd_urn: str,
        writes: List[QvdWrite],
        current: Optional[UpstreamLineage],
        is_owned: Callable[[str], bool],
        has_field_schema: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """`is_owned` tells the tables whose edges into the file this run knows
        all of, so that those no longer written are removed."""
        table_urns = list(dict.fromkeys(write.table_urn for write in writes))
        fine_grained = [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    builder.make_schema_field_urn(write.table_urn, field.source)
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[builder.make_schema_field_urn(qvd_urn, field.name)],
            )
            for write in writes
            for field in write.fields or []
            if field.source and has_field_schema
        ]
        # Added as a patch, so lineage from elsewhere stays.
        patch = DatasetPatchBuilder(qvd_urn)
        for table_urn in table_urns:
            patch.add_upstream_lineage(
                Upstream(dataset=table_urn, type=DatasetLineageType.COPY)
            )
        if self.config.include_column_lineage:
            for lineage in fine_grained:
                patch.add_fine_grained_lineage(lineage)
        # Edges from tables of apps whose writes are known this run, that no
        # longer store into the QVD, are removed one by one.
        if current is not None:
            seen = {
                *(u.dataset for u in current.upstreams),
                *(
                    parent
                    for lineage in current.fineGrainedLineages or []
                    for u in lineage.upstreams or []
                    if (parent := _schema_field_parent(u))
                ),
            }
            owned = {urn for urn in seen if is_owned(urn)}
            for upstream in current.upstreams:
                if upstream.dataset in owned and upstream.dataset not in table_urns:
                    patch.remove_upstream_lineage(upstream.dataset)
            if self.config.include_column_lineage:
                # A write whose fields are unknown, such as a STORE of a LOAD *
                # table, may still write the columns an earlier run saw.
                fields_unknown = {
                    write.table_urn for write in writes if write.fields is None
                }
                for lineage in self._stale_field_lineage(
                    current.fineGrainedLineages or [],
                    fine_grained,
                    owned - fields_unknown,
                ):
                    patch.remove_fine_grained_lineage(lineage)
        # A file on storage is not this source's to remove when stale.
        yield from _patch_workunits(
            patch, is_primary_source=self._is_own_dataset(qvd_urn)
        )

    def _is_own_dataset(self, urn: str) -> bool:
        return DatasetUrn.from_string(urn).platform == builder.make_data_platform_urn(
            self.platform
        )
