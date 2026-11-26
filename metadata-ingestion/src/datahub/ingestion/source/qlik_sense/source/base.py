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
    stale_entity_removal_handler: Optional[StaleEntityRemovalHandler]

    _data_connections: Optional[Dict[str, DataConnectionInfo]]
    _file_sources_by_app: Dict[str, FileSourceResolver]
    _app_space_ids: Dict[str, str]
    _apps_variables_unread: Set[str]
    _only_added_tables: Dict[_OnlyAddedTablesKey, List[str]]
    _apps_file_paths_unknown: Set[str]
    _apps_with_includes: Set[str]
    _reported_include_apps: Set[str]
    _resolved_file_qris: Set[str]
    _unmapped_files: Dict[str, str]
    _script_spellings: Dict[str, Counter[str]]
    _script_spellings_built_from: Optional[List[str]]
    _linked_unmapped_files: Set[str]
    _kept_storage_file_stores: Dict[str, List[str]]
    _unmapped_file_stores: DefaultDict[str, Dict[str, List[str]]]
    _app_end_of_run_urns: DefaultDict[str, Set[str]]
    _app_read_unmapped_files: DefaultDict[str, Set[str]]
    _app_read_graph_named: DefaultDict[str, Set[str]]
    _app_qris: Dict[str, str]
    _lineage_upstreams: Dict[str, Optional[LineageUpstreams]]
    _upstream_graph_read_failures: List[str]
    _apps_graph_named_kept: Set[str]
    _graph_named_resources: Dict[str, str]
    _graph_resource_writers: Dict[str, Set[str]]
    _reload_files: Dict[str, List[_ReloadFile]]
    _reload_files_unread: Set[str]
    _reload_path_failures: List[str]
    _unseen_spaces: Dict[str, _UnseenSpace]
    _listed_space_names: Optional[Set[str]]
    _graph_resource_readers: Dict[str, Set[str]]
    _allowed_space_ids: Set[str]
    _reported_no_graph: bool
    _unnamed_field_lineage: Set[str]
    _datasets_without_qri: bool
    _listed_dataset_qris: Set[str]
    _excluded_datasets_without_qri: bool
    _pending_table_lineage: List[PendingTableLineage]
    _pending_stored_files: List[PendingStoredFile]
    _app_table_urns_by_qri: Dict[str, str]
    _table_urns_by_app: Dict[str, Set[str]]
    _previous_app_table_urns: Set[str]
    _apps_tables_known: Set[str]
    _apps_tables_unknown: Set[str]
    _apps_graph_tables_unresolved: Set[str]
    _deferred_fingerprints: Dict[str, MetadataWorkUnit]
    _app_qvds: Dict[str, AppQvds]
    _apps_qvds_unknown: Dict[str, _QvdsUnknownReason]
    _app_stored_files: Dict[str, List[str]]
    _unresolved_qvd_loads: int
    _unlabelled_qvd_loads: int
    _binary_source_apps: Set[str]
    _app_urns: Dict[str, Set[str]]
    _kept_app_ids: Set[str]
    _app_model_fields: Dict[str, Dict[str, List[str]]]
    _binary_links: List[_BinaryLink]
    _apps_with_binary_tables: Set[str]
    _last_parsed: Optional[_ParsedApp]
    _data_model_fields: Dict[str, List[QlikDatasetSchemaField]]
    _dataset_store_sources: Dict[str, List[str]]
    _dataset_store_field_sources: Dict[str, Dict[str, List[str]]]
    _dataset_store_fields_unknown: Set[str]
    # Values are UntypedTable, typed as the tuple it is so that plain tuples
    # still fit.
    _untyped_tables: Dict[str, Tuple[str, List[QlikDatasetSchemaField]]]
    _dataset_field_types: Dict[str, Dict[str, Optional[str]]]
    _field_names_by_id: Dict[str, str]
    _fingerprint_salt: Optional[str]
    _change_check_failures: List[str]

    @property
    def _on_cloud(self) -> bool:
        return self.config.on_prem is None
