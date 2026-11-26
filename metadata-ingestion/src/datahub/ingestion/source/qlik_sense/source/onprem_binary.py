from typing import Iterable, List, Optional, Set, Tuple

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.constants import (
    BINARY_TABLES_UNKNOWN,
    CHECKPOINT_LOAD_FAILED_TITLE,
    DUPLICATE_TABLE_NAME_RE,
)
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.qvd_lineage import canonical_path
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ParsedScript,
    QvdReference,
    ScriptTableLineage,
    app_table_identifier,
)
from datahub.ingestion.source.qlik_sense.source.table_lineage import TableLineageMixin
from datahub.ingestion.source.qlik_sense.source_models import _BinaryLink, _BinaryLoad
from datahub.ingestion.source.qlik_sense.source_utils import _bounded_list
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


class OnPremBinaryMixin(TableLineageMixin):
    def _record_model_fields(self, app: App) -> None:
        # Only for the apps BINARY loads come from, as the checkpoint keeps them.
        if app.id not in self._binary_source_apps:
            return
        if app.data_model_tables:
            self._app_model_fields[app.id] = {
                table.name.lower(): [field.name for field in table.fields]
                for table in app.data_model_tables
            }
            return
        if not app.data_load_failed:
            return
        # Its fields from its last read, so BINARY loads from it keep their
        # column lineage while its data model can't be read.
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        for failure in failures:
            self.reporter.warning(
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="The data model fields of an app BINARY loads come from "
                "are not known, so lineage to its tables is only added to.",
                context=failure,
            )
        if previous is not None and previous.data_model_fields is not None:
            self._app_model_fields[app.id] = previous.data_model_fields

    def _binary_source_app(
        self, file: QvdReference, resolved: Optional[str]
    ) -> Optional[str]:
        """The app `binary_file_to_app` says the file was exported from, matched
        by the path as the script names it or the file it resolves to."""
        on_prem = self.config.on_prem
        assert on_prem is not None
        apps = on_prem.binary_file_to_app
        return apps.get(canonical_path(file.as_written)) or (
            apps.get(resolved) if resolved else None
        )

    def _check_binary_read(self, app: App) -> None:
        """Counts an app as partly read when the tables its BINARY statement
        loads can't be found for a reason the next run may not have, and its
        last read ingested such tables, so stale removal doesn't delete them.
        After max_partial_reads_kept runs they are let go, as the cause, such
        as section access denying the app's data, may never go away."""
        on_prem = self.config.on_prem
        assert on_prem is not None
        if not on_prem.read_data_model:
            return
        binary = self._parsed_script(app).binary
        if binary is None:
            return
        if app.data_load_failed:
            reason = "its data model could not be loaded"
        elif binary.uses_variables and app.variables_read_failed:
            reason = "its variables, which the BINARY path uses, could not be read"
        else:
            return
        tables_unknown = f"{BINARY_TABLES_UNKNOWN}{reason}"
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        if failures:
            # Its last read may hold BINARY tables.
            app.read_errors.append(tables_unknown)
            return
        if previous is None or not previous.binary_tables:
            return
        if previous.partial_reads >= self.config.max_partial_reads_kept:
            self.reporter.warning(
                title="Qlik BINARY tables are no longer kept",
                message="The tables these apps load with BINARY could not be "
                "found for more than max_partial_reads_kept runs, so they are "
                "no longer kept from the app's last read.",
                context=f"{app.id}: {reason}",
            )
            return
        app.read_errors.append(tables_unknown)

    def _binary_load(self, app: App, parsed: ParsedScript) -> _BinaryLoad:
        """The app's tables a BINARY statement loads: the data model's tables
        the script doesn't create itself."""
        binary = _BinaryLoad()
        statement = parsed.binary
        if statement is None:
            return binary
        self.reporter.binary_loads_found += 1

        def without_lineage(reason: str) -> _BinaryLoad:
            self.reporter.binary_loads_without_lineage.append(
                f"{app.id}: {statement.written}: {reason}"
            )
            return binary

        file = statement.file
        if file is None:
            return without_lineage("the path names no single file")
        if not app.data_model_tables:
            return without_lineage(
                "the app's data model was not read, see on_prem.read_data_model"
            )
        resolver = self._qvd_path_resolver(app)
        resolved = resolver.resolve(file)
        source_app_id = self._binary_source_app(file, resolved)
        if source_app_id == app.id:
            return without_lineage("binary_file_to_app maps it to the app itself")
        if source_app_id is not None and parsed.drops_unnamed_tables:
            # Any data model table could be one the script dropped and loaded again.
            return without_lineage(
                "the script drops, replaces or renames to tables it doesn't "
                "name, so the tables the file brings in are not known"
            )
        labels = {label.lower() for label in parsed.table_labels}
        created = set(labels)
        if source_app_id is not None:
            # A load under a BINARY table's name adds to it, or Qlik renames
            # the new table, so only a table the script drops and creates again,
            # or renames a table to, is not the file's. Tables the source app
            # lacks are left out later.
            created &= {
                label.lower()
                for label in [*parsed.dropped_tables, *parsed.renamed_tables]
            }
        upstream_urns: List[str] = []
        if source_app_id is not None and parsed.has_includes:
            self.reporter.warning(
                title="Qlik BINARY tables may be changed by an included script",
                message="A script included with $(Include) or $(Must_Include) "
                "that was not read, or a SUB the script calls but does not "
                "define, could drop or reload the tables a mapped BINARY load "
                "brings in, so their lineage is only added to.",
                context=app.id,
            )
        if source_app_id is None:
            if parsed.has_unnamed_tables or parsed.has_includes:
                # Any data model table could be one of those.
                return without_lineage(
                    "the script creates tables it doesn't name, so the tables "
                    "the file brings in are not known; map the file in "
                    "binary_file_to_app"
                )
            urn = resolver.storage_urn(file)
            if urn is None:
                if resolved is None:
                    return without_lineage(
                        "its data connection is not a known folder connection"
                    )
                urn = self._gen_qlik_dataset_urn(resolved)
                binary.paths[urn] = resolved
            upstream_urns = [urn]
        for table in app.data_model_tables:
            if table.name.lower() in created:
                continue
            suffixed = DUPLICATE_TABLE_NAME_RE.match(table.name)
            # Qlik names a new table `Orders-1` when Orders already exists, but
            # a real name such as `Orders-2024` looks the same.
            maybe_renamed = bool(suffixed and suffixed.group("name").lower() in labels)
            if maybe_renamed and source_app_id is None:
                without_lineage(
                    f"table {table.name} may be a script table Qlik renamed; "
                    "map the file in binary_file_to_app"
                )
                continue
            identifier = app_table_identifier(app.id, table.name)
            fields = [field.name for field in table.fields]
            binary.tables[identifier] = ScriptTableLineage(
                label=table.name,
                identifier=identifier,
                upstream_urns=upstream_urns,
                column_upstreams={field: [] for field in fields},
            )
            if source_app_id is not None:
                dataset_urn = self._gen_qlik_dataset_urn(identifier)
                binary.links[dataset_urn] = _BinaryLink(
                    app_id=app.id,
                    source_app_id=source_app_id,
                    table=table.name,
                    dataset_urn=dataset_urn,
                    fields=fields,
                    additive=parsed.has_includes,
                    maybe_renamed_script_table=maybe_renamed,
                )
        return binary

    def _binary_link_source(
        self, link: _BinaryLink, unread: Set[Tuple[str, str]]
    ) -> Optional[str]:
        """The source app's table a BINARY table is linked to, if that app was
        read or kept this run and ingests it, so the edge never points at a
        dataset that doesn't exist."""
        source_urns = self._app_urns.get(link.source_app_id)
        if source_urns is None:
            unread.add((link.app_id, link.source_app_id))
            return None
        source_urn = self._gen_qlik_dataset_urn(
            app_table_identifier(link.source_app_id, link.table)
        )
        if source_urn not in source_urns:
            source_model = self._app_model_fields.get(link.source_app_id)
            if source_model is None:
                self.reporter.binary_loads_without_lineage.append(
                    f"{link.app_id}: app {link.source_app_id} ingests no table "
                    f"{link.table}"
                )
            elif link.table.lower() in source_model:
                self.reporter.binary_loads_without_lineage.append(
                    f"{link.app_id}: app {link.source_app_id} doesn't ingest table "
                    f"{link.table}, as it isn't loaded with SQL, from a QVD or "
                    "with BINARY there, such as a RESIDENT or INLINE table"
                )
            # Otherwise the source app's data model lacks it: the script's own.
            return None
        return source_urn

    def _binary_upstream_lineage(
        self,
        link: _BinaryLink,
        source_urn: Optional[str],
        fine_grained: List[FineGrainedLineage],
    ) -> UpstreamLineage:
        upstreams = [
            Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
            for urn in link.upstream_urns
        ]
        if source_urn is not None:
            upstreams.append(Upstream(dataset=source_urn, type=DatasetLineageType.COPY))
        return UpstreamLineage(
            upstreams=upstreams, fineGrainedLineages=self._column_lineage(fine_grained)
        )

    def _patch_binary_upstreams(
        self,
        link: _BinaryLink,
        source_urn: Optional[str],
        fine_grained: List[FineGrainedLineage],
    ) -> Iterable[MetadataWorkUnit]:
        lineage = self._binary_upstream_lineage(link, source_urn, fine_grained)
        if not lineage.upstreams and not lineage.fineGrainedLineages:
            return
        yield convert_upstream_lineage_to_patch(link.dataset_urn, lineage, None)

    def _gen_binary_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Lineage of the tables an app BINARY loads from another app, written
        once every app has been read. Without the source app's table or its
        fields, the table's own lineage is only added to, so the edges an
        earlier run wrote stay."""
        unread: Set[Tuple[str, str]] = set()
        for link in self._binary_links:
            source_model = self._app_model_fields.get(link.source_app_id)
            in_source_model = (
                source_model is not None and link.table.lower() in source_model
            )
            if link.maybe_renamed_script_table and not in_source_model:
                if link.source_app_id not in self._app_urns:
                    unread.add((link.app_id, link.source_app_id))
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                self.reporter.binary_loads_without_lineage.append(
                    f"{link.app_id}: table {link.table} is not in app "
                    f"{link.source_app_id}'s data model, so it is taken as a "
                    "script table Qlik renamed"
                    if source_model is not None
                    else f"{link.app_id}: table {link.table} may be a script "
                    f"table Qlik renamed, and app {link.source_app_id}'s data "
                    "model is not known"
                )
                # The script's own edges stay, only added to if it may be the file's.
                if source_model is None:
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                # It is the script's own table, so its script lineage is
                # written whole, which also drops a COPY edge an earlier run
                # wrote from the source app.
                if link.additive:
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                yield MetadataChangeProposalWrapper(
                    entityUrn=link.dataset_urn,
                    aspect=self._binary_upstream_lineage(
                        link, None, link.script_fields
                    ),
                ).as_workunit()
                continue
            source_urn = self._binary_link_source(link, unread)
            if source_urn is None and source_model is not None and not in_source_model:
                if link.additive:
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                # Not from the file, so its script lineage is written whole.
                yield MetadataChangeProposalWrapper(
                    entityUrn=link.dataset_urn,
                    aspect=self._binary_upstream_lineage(
                        link, None, link.script_fields
                    ),
                ).as_workunit()
                continue
            if source_urn is None or source_model is None:
                # Without the source app's fields, the column edges an earlier
                # run wrote stay.
                yield from self._patch_binary_upstreams(
                    link, source_urn, link.script_fields
                )
                continue
            source_fields = set(source_model.get(link.table.lower(), []))
            fine_grained = [
                *link.script_fields,
                *(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=[builder.make_schema_field_urn(source_urn, field)],
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[
                            builder.make_schema_field_urn(link.dataset_urn, field)
                        ],
                    )
                    for field in link.fields
                    if field in source_fields
                ),
            ]
            if link.additive:
                yield from self._patch_binary_upstreams(link, source_urn, fine_grained)
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=link.dataset_urn,
                aspect=self._binary_upstream_lineage(link, source_urn, fine_grained),
            ).as_workunit()
        for app_id, source_app_id in sorted(unread):
            self.reporter.binary_loads_without_lineage.append(
                f"{app_id}: app {source_app_id} was not read or kept this run"
            )
        if self.reporter.binary_loads_without_lineage:
            self.reporter.warning(
                title="Some Qlik BINARY loads have no lineage",
                message="The tables these apps load with BINARY are not linked "
                "to their source. See binary_loads_without_lineage.",
                context=_bounded_list(list(self.reporter.binary_loads_without_lineage)),
            )
