import logging
from typing import Collection, Dict, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.qlik_sense.constants import CHECKPOINT_LOAD_FAILED_TITLE
from datahub.ingestion.source.qlik_sense.data_classes import (
    QLIK_DATASET_QRI_PREFIX,
    UPSTREAM_RESOURCE_QRI_PREFIXES,
    LineageUpstreams,
)
from datahub.ingestion.source.qlik_sense.file_lineage import FILE_QRI_RE
from datahub.ingestion.source.qlik_sense.source.file_identity import FileIdentityMixin
from datahub.ingestion.source.qlik_sense.source_models import (
    IncompleteLineageReason,
    ResolvedSource,
    ResolvedSources,
)
from datahub.ingestion.source.qlik_sense.source_utils import (
    describe_error,
    qlik_app_id,
    qlik_field_id,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)

logger = logging.getLogger(__name__)


class SourceResolutionMixin(FileIdentityMixin):
    """Resolves a source node of Qlik's lineage graph to a dataset URN, in
    this order:

    1. A table of another app resolves only to a table of an app this run
       read or kept.
    2. Any other node resolves to the Qlik dataset listed with its secure QRI.
    3. A file node that isn't listed resolves through the reading app's load
       script, to the file on known storage or to an unmapped `lib://` path.
    4. Failing that, a node only the app's upstream lineage graph names
       becomes a graph-named dataset, unless the file is on known storage.

    A source this run can't tell, as when the datasets, the data connections
    or the upstream graph could not be read, comes with the reason, so the
    table's lineage is only added to."""

    def _resolve_lineage_sources(
        self, source_qris: List[str], dataset_urn: str, own_app_id: Optional[str]
    ) -> ResolvedSources:
        """URNs of the sources this run ingests. A source that can't be resolved
        this run, because the datasets or its app could not be read, makes the
        result incomplete. `own_app_id` is the app whose table it is."""
        resolved = ResolvedSources()
        for source_qri in source_qris:
            source = self._resolve_source(source_qri, dataset_urn, own_app_id)
            urn, unknown = source.urn, source.unknown_reason
            if unknown and unknown not in resolved.unknown_reasons:
                resolved.unknown_reasons.append(unknown)
            if urn and urn not in resolved.urns:
                resolved.urns.append(urn)
        return resolved

    def _resolve_source(
        self,
        table_qri: str,
        dataset_urn: str,
        own_app_id: Optional[str],
        name_from_graph: bool = True,
    ) -> ResolvedSource:
        """The URN of a source table node, if this run ingests it, and why the
        source could not be told this run, if so. An unknown source may still
        have a URN, which is only added."""
        app_id = qlik_app_id(table_qri)
        if app_id is not None:
            # Such as a BINARY load of another app; a table of an app the run
            # does not ingest has no URN and is left out.
            urn = self._app_table_urns_by_qri.get(table_qri)
            if not self._app_tables_known(app_id):
                # A table the graph names this run is still added, but the
                # app's other tables are unknown.
                return ResolvedSource(
                    urn=urn if urn != dataset_urn else None,
                    unknown_reason=f"the tables of source app {app_id} are not known this run",
                )
            if urn is None and app_id in self._apps_graph_tables_unresolved:
                return ResolvedSource(
                    urn=None,
                    unknown_reason=f"source app {app_id} has lineage graph tables matched to "
                    "none of its tables",
                )
        else:
            # Data files, and files Qlik catalogued on a data connection such
            # as ADLS, are datasets. Other sources, such as database tables,
            # have opaque QRIs; the load script parser traces SQL loads instead.
            datasets = self._dataset_urns_by_secure_qri
            urn = (
                datasets.get(table_qri.rpartition("#")[0]) or datasets.get(table_qri)
                if datasets
                else None
            )
            graph_app_id = own_app_id if name_from_graph else None
            on_known_storage = False
            if urn is None and own_app_id is not None and FILE_QRI_RE.match(table_qri):
                # Files on a storage connection resolve from the load script,
                # without the list of datasets.
                file_source = self._file_source_resolution(own_app_id, table_qri)
                urn, on_known_storage = file_source.urn, file_source.on_known_storage
                inputs_unknown = (
                    self._file_inputs_unknown(own_app_id) if urn is None else None
                )
                if inputs_unknown:
                    # The file may be one the script reads through a path,
                    # connection or included script this run could not read,
                    # so the last run's edge to it must stay. Without the
                    # listing, it may be a listed file the graph can't name,
                    # and without the connections, one on known storage.
                    if on_known_storage:
                        return ResolvedSource(urn=None, unknown_reason=inputs_unknown)
                    if datasets is None or self._data_connections_unread():
                        self._keep_graph_named_sources(own_app_id, table_qri)
                        return ResolvedSource(urn=None, unknown_reason=inputs_unknown)
                    return ResolvedSource(
                        urn=self._graph_named_source(graph_app_id, table_qri),
                        unknown_reason=inputs_unknown,
                    )
            if urn is None and datasets is None:
                self._keep_graph_named_sources(own_app_id, table_qri)
                return ResolvedSource(
                    urn=None, unknown_reason=IncompleteLineageReason.DATASETS_UNLISTED
                )
            if (
                urn is None
                and table_qri.startswith(QLIK_DATASET_QRI_PREFIX)
                and (
                    self._datasets_without_qri
                    or (
                        self._excluded_datasets_without_qri
                        and not self._is_listed_dataset(table_qri)
                    )
                )
            ):
                # It may be a listed dataset the listing gave no QRI for, so
                # it is not named from the graph either.
                self._keep_graph_named_sources(own_app_id, table_qri)
                return ResolvedSource(
                    urn=None,
                    unknown_reason=IncompleteLineageReason.DATASET_WITHOUT_QRI,
                )
            # A file on known storage that could not be told which is left out
            # rather than named apart from its storage dataset.
            if urn is None and graph_app_id is not None and not on_known_storage:
                urn = self._graph_named_source(graph_app_id, table_qri)
                if (
                    urn is None
                    and self._graph_could_name(table_qri)
                    and self._upstream_graph_unread(graph_app_id)
                ):
                    # The upstream graph that may name it could not be read.
                    return ResolvedSource(
                        urn=None,
                        unknown_reason=IncompleteLineageReason.UPSTREAM_GRAPH_UNREAD,
                    )
        return ResolvedSource(
            urn=urn if urn != dataset_urn else None, unknown_reason=None
        )

    def _graph_could_name(self, table_qri: str) -> bool:
        # A dataset listed in a space the recipe leaves out stays out. When one
        # has no QRI, any dataset QRI the graph names may be it.
        if (
            table_qri.startswith(QLIK_DATASET_QRI_PREFIX)
            and self._excluded_datasets_without_qri
        ):
            return False
        return table_qri.startswith(
            UPSTREAM_RESOURCE_QRI_PREFIXES
        ) and not self._is_listed_dataset(table_qri)

    def _is_listed_dataset(self, table_qri: str) -> bool:
        return any(
            qri in self._listed_dataset_qris
            for qri in (table_qri.rpartition("#")[0], table_qri)
        )

    def _graph_named_source(
        self, app_id: Optional[str], table_qri: str
    ) -> Optional[str]:
        """A dataset or file only the app's upstream graph names, such as a QVD
        in a space the API key can't see, under a URN from its QRI. Its writer
        apps' tables become its upstreams at the end of the run."""
        if app_id is None or not self._graph_could_name(table_qri):
            return None
        upstreams = self._app_lineage_upstreams(app_id)
        if upstreams is None:
            return None
        # The graph may name the resource itself rather than one of its tables.
        candidates = (table_qri.rpartition("#")[0], table_qri)
        resource_qri = next(
            (qri for qri in candidates if qri in upstreams.resource_labels), None
        )
        if resource_qri is None:
            return None
        label = upstreams.resource_labels[resource_qri]
        # A file another app's script resolves keeps that URN there, so the two
        # are separate datasets until its storage is mapped.
        urn = self._gen_qlik_dataset_urn(resource_qri.lower())
        self._graph_named_resources[urn] = label
        self._graph_resource_readers.setdefault(urn, set()).add(app_id)
        self._app_end_of_run_urns[app_id].add(urn)
        self._app_read_graph_named[app_id].add(urn)
        # The dataset is the whole resource, so a store into any of its tables
        # is an upstream, as for listed datasets.
        writers = self._graph_resource_writers.setdefault(urn, set())
        for stored_qri, writer_qris in upstreams.stored_from.items():
            if resource_qri in (stored_qri, stored_qri.rpartition("#")[0]):
                writers.update(writer_qris)
        return urn

    def _app_lineage_upstreams(self, app_id: str) -> Optional[LineageUpstreams]:
        """The app's upstream graph, read when a source only it can name is
        first met, or None when it could not be read."""
        if app_id not in self._lineage_upstreams:
            try:
                upstreams: Optional[LineageUpstreams] = (
                    self.qlik_api.get_lineage_upstreams(self._app_qris[app_id])
                )
            except Exception as e:
                logger.debug(
                    f"Unable to read upstream lineage of app {app_id}", exc_info=True
                )
                upstreams = None
                self._upstream_graph_read_failures.append(
                    f"{app_id}: {describe_error(e)}"
                )
                self._keep_graph_named_urns(app_id)
            self._lineage_upstreams[app_id] = upstreams
        return self._lineage_upstreams[app_id]

    def _keep_graph_named_sources(self, app_id: Optional[str], table_qri: str) -> None:
        """Keeps the datasets the app's last read named from its upstream graph
        when this run can't tell whether a source it may name is one of them."""
        if app_id is not None and table_qri.startswith(UPSTREAM_RESOURCE_QRI_PREFIXES):
            self._keep_graph_named_urns(app_id)

    def _keep_graph_named_urns(self, app_id: str) -> None:
        """Keeps the datasets the app's last read named from its upstream
        graph, as this run could not name them again."""
        if app_id in self._apps_graph_named_kept:
            return
        self._apps_graph_named_kept.add(app_id)
        failures: List[str] = []
        previous = self._usable_checkpoint(app_id, failures)
        for failure in failures:
            # A failure stops stale entity removal from soft-deleting them.
            self.reporter.failure(
                title=CHECKPOINT_LOAD_FAILED_TITLE,
                message="Datasets the app's last read named from Qlik's lineage "
                "graph could not be kept, so stale metadata removal is skipped "
                "for this run.",
                context=failure,
            )
        if previous is None:
            return
        # The URN up to its env, as `_graph_named_source` builds it.
        prefixes = tuple(
            self._gen_qlik_dataset_urn(prefix).rpartition(",")[0]
            for prefix in UPSTREAM_RESOURCE_QRI_PREFIXES
        )
        for urn in previous.urns:
            if urn.startswith(prefixes):
                self._app_end_of_run_urns[app_id].add(urn)
                self._keep_in_stale_state(urn)
        # Datasets a run left out are in no URNs, but the app still reads them.
        self._app_read_graph_named[app_id].update(previous.read_graph_named_urns)

    def _upstream_graph_unread(self, app_id: str) -> bool:
        return (
            app_id in self._lineage_upstreams
            and self._lineage_upstreams[app_id] is None
        )

    def _file_inputs_unknown(self, app_id: str) -> Optional[IncompleteLineageReason]:
        """Why the files the app's load script reads are not all known, if so."""
        if app_id in self._apps_with_includes:
            if app_id not in self._reported_include_apps:
                self._reported_include_apps.add(app_id)
                self.reporter.warning(
                    title="Qlik app loads files from an included script",
                    message="The app includes a script with $(Include) or "
                    "$(Must_Include) that was not read, or calls a SUB its script "
                    "does not define. An include is not read on-prem, nor when its "
                    "path varies as the script runs or does not expand to a "
                    "lib:// path, it is nested more than 10 levels deep or in a "
                    "cycle, or the file could not be read. A file in the app's "
                    "lineage graph that its own script does not load only adds to "
                    "the table's lineage.",
                    context=app_id,
                )
            return IncompleteLineageReason.INCLUDED_FILE
        if app_id in self._apps_variables_unread:
            return IncompleteLineageReason.VARIABLES_UNREAD
        if app_id in self._apps_file_paths_unknown:
            return IncompleteLineageReason.FILE_PATHS_UNKNOWN
        if self._data_connections_unread():
            return IncompleteLineageReason.DATA_CONNECTIONS_UNLISTED
        return None

    def _graph_column_lineage(
        self,
        dataset_urn: str,
        field_source_qris: Dict[str, List[str]],
        own_app_id: Optional[str],
        table_source_qris: Collection[str] = (),
    ) -> List[FineGrainedLineage]:
        """Column lineage from the field level of Qlik's lineage graph. A source
        field is named only when its id is the hash of a field name the run
        has seen, such as the field it is loaded into."""
        if not self.config.include_column_lineage:
            # An unnamed source field would otherwise keep the table's lineage
            # from being written whole, for column lineage that isn't emitted.
            return []
        # A resource only named from the graph at field level would have no
        # edge to the table, so it is only named when a table source.
        table_sources = {
            *table_source_qris,
            *(qri.rpartition("#")[0] for qri in table_source_qris),
        }
        lineages = []
        for field, source_qris in field_source_qris.items():
            upstreams: List[str] = []
            for field_qri in source_qris:
                table_qri, _, field_id = field_qri.rpartition("#")
                urn = self._resolve_source(
                    table_qri,
                    dataset_urn,
                    own_app_id,
                    name_from_graph=bool(
                        {table_qri, table_qri.rpartition("#")[0]} & table_sources
                    ),
                ).urn
                name = (
                    field
                    if qlik_field_id(field) == field_id
                    else self._field_names_by_id.get(field_id)
                )
                if urn and not name and urn not in self._graph_named_resources:
                    # The source field is real but no field of that name has
                    # been read yet, such as a renamed load from a QVD read
                    # later in the run, so the table's column lineage is not
                    # all known. A source only the upstream graph names is
                    # never read, so its field names never become known.
                    self._unnamed_field_lineage.add(dataset_urn)
                if urn and name:
                    upstream = builder.make_schema_field_urn(urn, name)
                    if upstream not in upstreams:
                        upstreams.append(upstream)
            if upstreams:
                lineages.append(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=upstreams,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[builder.make_schema_field_urn(dataset_urn, field)],
                    )
                )
        return lineages
