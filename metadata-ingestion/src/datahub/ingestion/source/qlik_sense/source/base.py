from collections import Counter
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.qlik_sense.app_state import QlikAppStateHandler
from datahub.ingestion.source.qlik_sense.config import (
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    LineageUpstreams,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.file_lineage import FileSourceResolver
from datahub.ingestion.source.qlik_sense.qlik_base_api import QlikBaseAPI
from datahub.ingestion.source.qlik_sense.qvd_lineage import AppQvds
from datahub.ingestion.source.qlik_sense.reload_sql import _ReloadFile
from datahub.ingestion.source.qlik_sense.script_lineage import DataConnectionInfo
from datahub.ingestion.source.qlik_sense.source_models import (
    PendingStoredFile,
    PendingTableLineage,
    _BinaryLink,
    _OnlyAddedTablesKey,
    _ParsedApp,
    _QvdsUnknownReason,
    _UnseenSpace,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)


class QlikSourceBase:
    """The state QlikSenseSource's mixins share. Every attribute is set in
    QlikSenseSource.__init__; declaring them here lets each mixin use them."""

    ctx: PipelineContext
    config: QlikSourceConfig
    reporter: QlikSourceReport
    platform: str
    ui_host: str
    qlik_api: QlikBaseAPI
    app_state: QlikAppStateHandler
    # Owned here rather than by the automatic processor so that entities of
    # apps skipped as unchanged or only partly read can be kept in the state.
    stale_entity_removal_handler: Optional[StaleEntityRemovalHandler]

    _data_connections: Optional[Dict[str, DataConnectionInfo]]
    # The files each app's load script reads, to resolve the files in its
    # lineage graph with.
    _file_sources_by_app: Dict[str, FileSourceResolver]
    _app_space_ids: Dict[str, str]
    _apps_variables_unread: Set[str]
    # The tables whose lineage is only added to, by app and reason, reported
    # once per app at the end of the run.
    _only_added_tables: Dict[_OnlyAddedTablesKey, List[str]]
    # Apps that load files by a wildcard or a path the connector can't
    # expand, so the files in their lineage graph can't all be named.
    _apps_file_paths_unknown: Set[str]
    _apps_with_includes: Set[str]
    _reported_include_apps: Set[str]
    _resolved_file_qris: Set[str]
    # Files on connections of unknown storage, by URN, as `lib://` paths,
    # and those that lineage links to, which get a dataset.
    _unmapped_files: Dict[str, str]
    # How load scripts spell each `lib://` path prefix, by its lowercase
    # key, and the paths it was built from.
    _script_spellings: Dict[str, Counter[str]]
    _script_spellings_built_from: Optional[List[str]]
    _linked_unmapped_files: Set[str]
    # Tables kept apps store into files on storage that was unknown when
    # their last read recorded them.
    _kept_storage_file_stores: Dict[str, List[str]]
    # The tables each app stores into such a file, linked at the end of
    # the run only if some app's lineage graph reads the file, as a STORE
    # in the script may never run.
    _unmapped_file_stores: DefaultDict[str, Dict[str, List[str]]]
    # Datasets emitted at the end of the run for the apps whose lineage
    # names them, added to those apps' checkpoints so a later run that
    # keeps an app keeps them too.
    _app_end_of_run_urns: DefaultDict[str, Set[str]]
    # Unmapped files each app's lineage read this run.
    _app_read_unmapped_files: DefaultDict[str, Set[str]]
    _app_read_graph_named: DefaultDict[str, Set[str]]
    # Upstream graphs of the apps read this run, fetched when a source only
    # they can name is met, None when the read failed, and the resources
    # only they name, by URN, with their labels and the app tables storing
    # into them.
    _app_qris: Dict[str, str]
    _lineage_upstreams: Dict[str, Optional[LineageUpstreams]]
    _upstream_graph_read_failures: List[str]
    # Apps whose last graph-named datasets are kept, as this run could not
    # tell whether they are still sources.
    _apps_graph_named_kept: Set[str]
    _graph_named_resources: Dict[str, str]
    _graph_resource_writers: Dict[str, Set[str]]
    # The files apps stored or loaded in their last reload, by app id.
    _reload_files: Dict[str, List[_ReloadFile]]
    # Apps whose last reload's statements could not be read.
    _reload_files_unread: Set[str]
    _reload_path_failures: List[str]
    # Spaces the API key can't see that lineage goes through, by lowercased
    # name, with the name as written and what lineage needs from them.
    _unseen_spaces: Dict[str, _UnseenSpace]
    _listed_space_names: Optional[Set[str]]
    # The apps whose upstream graph names each graph-named dataset.
    _graph_resource_readers: Dict[str, Set[str]]
    _allowed_space_ids: Set[str]
    _reported_no_graph: bool
    # Datasets with a column edge whose source field could not be named.
    _unnamed_field_lineage: Set[str]
    # A listed QVD of an allowed space had no secure QRI to match.
    _datasets_without_qri: bool
    # Every listed dataset, so one in a space the recipe leaves out is not
    # named from the lineage graph instead.
    _listed_dataset_qris: Set[str]
    # A listed QVD of a left-out space had no secure QRI to match.
    _excluded_datasets_without_qri: bool
    # App tables whose lineage waits for field names read later in the run.
    _pending_table_lineage: List[PendingTableLineage]
    _pending_stored_files: List[PendingStoredFile]
    # Table QRI -> dataset URN for the tables of apps read or kept this run.
    # Lineage only links to tables in here, as other URNs may never exist.
    _app_table_urns_by_qri: Dict[str, str]
    # The table URNs of each app read or kept this run.
    _table_urns_by_app: Dict[str, Set[str]]
    # Tables the last run recorded for apps whose tables are known this
    # run, so edges from a table since renamed or dropped are still owned.
    _previous_app_table_urns: Set[str]
    # Apps read or kept this run, by whether all their tables are known.
    _apps_tables_known: Set[str]
    _apps_tables_unknown: Set[str]
    # Apps whose tables are known, but whose lineage graph names tables
    # with no URN, so only references to those tables are unknown.
    _apps_graph_tables_unresolved: Set[str]
    # Fingerprints of apps with table lineage still pending, written after
    # it so a failed run can't skip the app as unchanged without it.
    _deferred_fingerprints: Dict[str, MetadataWorkUnit]
    # On-prem, the QVDs each app read or kept this run writes and reads, and
    # the apps whose QVDs are unknown.
    _app_qvds: Dict[str, AppQvds]
    # Apps whose QVD writes are not known this run, with why.
    _apps_qvds_unknown: Dict[str, _QvdsUnknownReason]
    # On Qlik Cloud, the files on storage each app read this run stores into.
    _app_stored_files: Dict[str, List[str]]
    _unresolved_qvd_loads: int
    _unlabelled_qvd_loads: int
    # On-prem, the URNs each app BINARY loads come from produced, read or
    # kept this run, the data model fields of those apps, the tables linked
    # to them once every app has been read, and the apps whose read
    # ingested tables a BINARY statement brought in.
    _binary_source_apps: Set[str]
    _app_urns: Dict[str, Set[str]]
    # Apps whose last read is kept, whose checkpoints may hold unmapped
    # files they linked then.
    _kept_app_ids: Set[str]
    _app_model_fields: Dict[str, Dict[str, List[str]]]
    _binary_links: List[_BinaryLink]
    _apps_with_binary_tables: Set[str]
    _last_parsed: Optional[_ParsedApp]
    # The data model fields of the app being emitted, by table identifier.
    _data_model_fields: Dict[str, List[QlikDatasetSchemaField]]
    # Datasets and the app table QRIs stored into them, linked once every
    # app has been read.
    _dataset_store_sources: Dict[str, List[str]]
    _dataset_store_field_sources: Dict[str, Dict[str, List[str]]]
    _dataset_store_fields_unknown: Set[str]
    # Graph tables whose fields have no type, such as a table the script
    # drops after storing it, by URN.
    # Values are UntypedTable, typed as the tuple it is so that plain tuples
    # still fit.
    _untyped_tables: Dict[str, Tuple[str, List[QlikDatasetSchemaField]]]
    _dataset_field_types: Dict[str, Dict[str, Optional[str]]]
    # Field names seen this run, by their field id in Qlik's lineage graph.
    _field_names_by_id: Dict[str, str]
    _fingerprint_salt: Optional[str]
    # Filled from worker threads, so only copied to the report at the end.
    _change_check_failures: List[str]

    @property
    def _on_cloud(self) -> bool:
        return self.config.on_prem is None
