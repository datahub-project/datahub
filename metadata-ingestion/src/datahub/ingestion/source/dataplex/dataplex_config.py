"""Configuration for Google Dataplex source."""

import logging
from typing import Dict, List, Literal, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProjectFilterConfig,
    GCPValidationError,
    validate_project_label_list,
)
from datahub.ingestion.source.dataplex.dataplex_helpers import parse_gcs_path
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulLineageConfigMixin,
)

logger = logging.getLogger(__name__)

DEFAULT_LINEAGE_LOCATIONS = [
    "us",
    "eu",
    "asia",
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-south1",
    "us-west1",
    "us-west2",
    "us-west3",
    "us-west4",
    "northamerica-northeast1",
    "northamerica-northeast2",
    "southamerica-east1",
    "southamerica-west1",
    "europe-central2",
    "europe-north1",
    "europe-southwest1",
    "europe-west1",
    "europe-west2",
    "europe-west3",
    "europe-west4",
    "europe-west6",
    "europe-west8",
    "europe-west9",
    "europe-west10",
    "europe-west12",
    "me-central1",
    "me-central2",
    "me-west1",
    "asia-east1",
    "asia-east2",
    "asia-northeast1",
    "asia-northeast2",
    "asia-northeast3",
    "asia-south1",
    "asia-south2",
    "asia-southeast1",
    "asia-southeast2",
    "australia-southeast1",
    "australia-southeast2",
    "africa-south1",
]


class EntriesFilterConfig(ConfigModel):
    """Filter configuration specific to Dataplex Entries API (Universal Catalog)."""

    pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Dataplex entry names to filter in ingestion.",
    )
    fqn_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Dataplex fully-qualified names to filter in ingestion.",
    )


class EntryGroupFilterConfig(ConfigModel):
    """Filter configuration for Dataplex entry groups."""

    pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for entry group resource names to include/exclude.",
    )


class DataplexFilterConfig(ConfigModel):
    """Filter configuration for Dataplex ingestion."""

    entry_groups: EntryGroupFilterConfig = Field(
        default_factory=EntryGroupFilterConfig,
        description="Filters for Dataplex entry group names.",
    )
    entries: EntriesFilterConfig = Field(
        default_factory=EntriesFilterConfig,
        description="Filters specific to Dataplex Entries API (Universal Catalog).",
    )


class DataplexExportJobConfig(ConfigModel):
    """Configuration for ``extraction_method: export``.

    One Dataplex ``metadataJobs.create`` EXPORT job is submitted per configured
    entries location, writing JSONL to a per-location Cloud Storage bucket. The
    bucket for each location is resolved from ``export_bucket_config[location]``
    first, falling back to ``{bucket_base_name}-{location}``.
    """

    export_job_runner_project: str = Field(
        description="GCP project that runs the Dataplex metadata export jobs. "
        "The service account needs roles/dataplex.metadataJobOwner on this project.",
    )

    export_bucket_config: Dict[str, str] = Field(
        default_factory=dict,
        description="Explicit mapping of GCP location to GCS bucket name. "
        "Example: {us: my-bucket-us, us-east5: my-bucket-east5}. "
        "Entries here take priority over 'bucket_base_name'.",
    )

    bucket_base_name: Optional[str] = Field(
        default=None,
        description="Fallback base GCS bucket name used when a location is not "
        "listed in 'export_bucket_config'. The bucket name is derived as "
        "'{bucket_base_name}-{location}'.",
    )

    prefix: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Optional folder prefix inside each export bucket. The "
        "Dataplex Metadata Export API limits the custom prefix to 128 characters.",
    )

    export_poll_seconds: int = Field(
        default=15,
        ge=1,
        description="Polling interval (seconds) while waiting for export jobs to finish.",
    )

    export_timeout_seconds: int = Field(
        default=3600,
        ge=1,
        description="Total wait timeout (seconds) for all export jobs to finish.",
    )

    def bucket_for_location(self, location: str) -> str:
        """Resolve the GCS bucket for a given location.

        Precedence: ``export_bucket_config[location]`` > ``{bucket_base_name}-{location}``.
        """
        if location in self.export_bucket_config:
            return self.export_bucket_config[location]
        if self.bucket_base_name:
            return f"{self.bucket_base_name}-{location}"
        raise ValueError(
            f"No bucket configured for location '{location}'. Add it to "
            "'export_bucket_config' or set 'bucket_base_name'."
        )


class DataplexReadExportConfig(ConfigModel):
    """Configuration for ``extraction_method: read_export``.

    Ingests the output of Dataplex metadata exports produced outside DataHub
    (Cloud Scheduler, Workflows, a separate pipeline, etc.). No export jobs are
    submitted, so the service account only needs roles/storage.objectViewer on
    the buckets — no Dataplex job-submission roles.
    """

    export_paths: Dict[str, str] = Field(
        description="Mapping of entries location to the 'gs://bucket[/prefix]' "
        "output path of an already-completed Dataplex metadata export, e.g. "
        "{us: 'gs://my-bucket-us/exports'}. If a path contains output from "
        "several export jobs (multiple 'job=<id>' partitions), only the most "
        "recently written partition is read; point the path at a specific "
        "'.../job=<id>' folder to pin an exact run. The entries stage reads "
        "exactly the locations in this mapping ('entries_locations' applies to "
        "the other stages: lineage, glossaries).",
    )

    @field_validator("export_paths")
    @classmethod
    def _validate_export_paths(cls, v: Dict[str, str]) -> Dict[str, str]:
        if not v:
            raise ValueError(
                "export_paths must contain at least one location -> gs:// path entry."
            )
        for location, path in v.items():
            try:
                parse_gcs_path(path)
            except ValueError as e:
                raise ValueError(
                    f"export_paths entry for location '{location}' is invalid: {e}"
                ) from e
        return v


class DataplexConfig(
    GcpProjectFilterConfig,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
    StatefulIngestionConfigBase,
    StatefulLineageConfigMixin,
):
    """Configuration for Google Dataplex source.

    Project selection (`project_ids`, `project_labels`, `project_id_pattern`) is
    inherited from `GcpProjectFilterConfig` and consumed by the shared
    `resolve_gcp_projects` helper. Auto-discovery (when `project_ids` is empty)
    uses Cloud Resource Manager `search_projects` and only requires
    `resourcemanager.projects.get` (e.g. `roles/browser`) on the candidate
    projects — no folder/org-level grant is needed.
    """

    credential: Optional[GCPCredential] = Field(
        default=None,
        description="GCP credential information. If not specified, uses Application Default Credentials.",
    )

    extraction_method: Literal["api", "export", "read_export"] = Field(
        default="api",
        description="How entries are fetched from the Universal Catalog. "
        "'api' (default) lists entries per project via list_entry_groups / "
        "list_entries / get_entry — this only sees entries physically created in "
        "the configured projects. 'export' submits a Dataplex metadata EXPORT job "
        "per entries location (scoped to the configured projects) that writes "
        "JSONL to a GCS bucket, then reads entries from that bucket; use it for "
        "central-catalog / federated architectures where assets surface in a "
        "catalog project via Dataplex catalog linking and are invisible to "
        "list_entries. Requires 'export_config'. 'read_export' ingests the "
        "output of export jobs you run outside DataHub — no jobs are submitted "
        "and only storage read access is needed. Requires 'read_export_config'.",
    )

    export_config: Optional[DataplexExportJobConfig] = Field(
        default=None,
        description="Settings for extraction_method 'export' (job runner "
        "project, GCS buckets, polling). Required for 'export', not allowed "
        "otherwise.",
    )

    read_export_config: Optional[DataplexReadExportConfig] = Field(
        default=None,
        description="Settings for extraction_method 'read_export' (paths of "
        "pre-existing export output). Required for 'read_export', not allowed "
        "otherwise.",
    )

    entries_locations: List[str] = Field(
        default_factory=lambda: ["us", "eu", "asia", "global"],
        description="List of GCP regions to scan for Universal Catalog entries extraction. "
        "This list may include multi-regions (for example 'us', 'eu', 'asia') and "
        "single regions (for example 'us-central1'). "
        "Entries scanning runs across all configured entries_locations. "
        "Default: ['us', 'eu', 'asia', 'global'].",
    )

    filter_config: DataplexFilterConfig = Field(
        default_factory=DataplexFilterConfig,
        description="Filters to control which Dataplex resources are ingested.",
    )

    aspect_type_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["datahub-.*"]),
        description="Regex allow/deny patterns matched against Dataplex aspect type "
        "names to decide which aspects are flattened into DataHub custom properties. "
        "Defaults to denying DataHub-authored aspects (those prefixed 'datahub-' that "
        "the DataHub sync-back writes, e.g. 'datahub-tags' / 'datahub-properties') so "
        "synced-back metadata does not return as custom-property noise.",
    )

    include_schema: bool = Field(
        default=True,
        description="Whether to extract and ingest schema metadata (columns, types, descriptions). "
        "Set to False to skip schema extraction for faster ingestion when only basic dataset metadata is needed. "
        "Disabling schema extraction can improve performance for large deployments. Default: True.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage information using Dataplex Lineage API. "
        "Extracts table-level lineage relationships between entries. "
        "Lineage API calls automatically retry transient errors (timeouts, rate limits) with exponential backoff.",
    )

    include_column_lineage: bool = Field(
        default=False,
        description="Whether to extract column-level lineage via column-scoped "
        "Data Lineage API lookups. For each entry that has table-level lineage, "
        "one extra search_links call is issued per batch of 20 columns per "
        "parent that returned links, so this multiplies Data Lineage API read "
        "volume. Lower 'max_workers_lineage' to slow that volume down: the pool "
        "size is the binding constraint on sustained call rate, while "
        "'lineage_max_calls_per_minute' is a ceiling that only engages once the "
        "pool can outrun it. Requires both 'include_lineage' and "
        "'include_schema'; if either is off, column lineage is disabled with a "
        "warning rather than failing the source.",
    )

    dpms_hive_metastore_service: Optional[str] = Field(
        default=None,
        description="Fallback '{project}.{location}.{service}' used to resolve "
        "'hive_metastore:...' lineage FQNs (which is how Spark and Dataproc "
        "jobs report lineage) to Dataproc Metastore table URNs when the "
        "(database, table) pair is not found among the tables ingested in this "
        "run. Leave unset to fall through to 'include_hive_metastore_nodes' "
        "handling.",
    )

    include_hive_metastore_nodes: bool = Field(
        default=True,
        description="Whether 'hive_metastore:...' upstreams that match no "
        "Dataproc Metastore table in this run — the table lives only in a "
        "Dataproc cluster's own metastore, with no catalog entry — are emitted "
        "as lineage-only datasets on the 'hive' platform named "
        "'{database}.{table}', the same treatment GCS bucket upstreams get. "
        "When false, those upstream edges are skipped and counted in the "
        "report. Only affects lineage FQNs that would otherwise be dropped as "
        "unparseable.",
    )

    include_lineage_operations: bool = Field(
        default=False,
        description="Whether to attach the producing GCP operation (process) "
        "to each lineage edge as a DataHub Query entity, rendered as an "
        "operation node on the edge in the UI. Adds batchSearchLinkProcesses, "
        "getProcess and listRuns Data Lineage API reads (through the same rate "
        "limiter; one getProcess and one single-row listRuns per distinct "
        "process per run thanks to caching). Requires 'include_lineage'; when "
        "off, lineage output is byte-identical to before this feature existed.",
    )

    include_lineage_operation_sql: bool = Field(
        default=False,
        description="Whether to fetch the real SQL text (plus job metadata) "
        "for BigQuery-origin operations from the BigQuery Jobs API. The Data "
        "Lineage API stores only the job id, never the statement. Needs "
        "roles/bigquery.resourceViewer on the compute projects, because jobs "
        "are private to their creator by default, and the google-cloud-bigquery "
        "package, which the dataplex extra does not install. Failures degrade "
        "to a synthetic statement. Requires 'include_lineage_operations'.",
    )

    lineage_operation_origin_types: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Allow/deny patterns matched against a GCP process's "
        "origin sourceType (BIGQUERY, DATAFLOW, COMPOSER, DATAPROC, "
        "DATA_FUSION, VERTEX_AI, CUSTOM, ...). Denied origins keep their "
        "lineage edges but get no operation node — the escape hatch when "
        "another connector already owns query nodes for those tables.",
    )

    lineage_operation_attribute_allowlist: List[str] = Field(
        default_factory=lambda: [
            "sql",
            "bigquery_job_id",
            "job_id",
            "process_id",
            "process_name",
            "dag_id",
            "task_id",
            "airflow_dag_id",
            "airflow_task_id",
            "spark.app.name",
            "spark.app.id",
        ],
        description="Process attribute keys copied into the operation node's "
        "custom properties. Attributes are producer-controlled free-form maps "
        "of up to 100 entries, so only allowlisted keys are hoisted; values "
        "are truncated to 1000 characters.",
    )

    include_storage_lineage: bool = Field(
        default=False,
        description="Whether to derive a bucket -> table lineage edge from a "
        "Dataproc Metastore table's own 'storage' aspect. Dataplex never "
        "reports that hop through the Data Lineage API, so it is the only way "
        "to see where such a table's files actually live. Off by default "
        "because it adds an upstream that no previous run emitted.",
    )

    include_lineage_only_upstreams: bool = Field(
        default=False,
        description="Whether to materialize minimal entities for lineage "
        "upstreams this run does not otherwise ingest — GCS buckets, "
        "uncatalogued hive tables, and tables in projects outside the "
        "configured scope. Without them the edge exists in the graph but the "
        "node is invisible in the UI, because searchAcrossLineage only returns "
        "entities that carry a status aspect. Writes are guarded: a URN that "
        "already exists and is soft-deleted is never revived, and a container "
        "this connector did not create is never overwritten. Needs a DataHub "
        "graph connection for those checks; without one, only nodes whose URN "
        "namespace belongs to this connector are written.",
    )

    remove_stale_lineage: bool = Field(
        default=True,
        description="Whether lineage mirrors the live Data Lineage API state. "
        "Default true: each run overwrites the upstreamLineage aspect with the "
        "edges the API currently reports. Set to false for STICKY lineage, "
        "where new edges are merged with the previously persisted aspect (so "
        "an edge the API no longer reports — for example past its retention "
        "window — is preserved) and lineage-only upstream entities are exempt "
        "from stale-metadata removal. Merging needs the DataHub graph client; "
        "dry runs without one emit the fresh state only. Either way, entities "
        "that disappear from the source are still soft-deleted by stateful "
        "ingestion.",
    )

    resolve_pubsub_subscriptions: bool = Field(
        default=False,
        description="Whether 'pubsub:subscription:...' upstream lineage FQNs "
        "(how Dataflow reports Pub/Sub sources) are resolved to their backing "
        "topic via the Pub/Sub Admin API, using one cached get_subscription "
        "call per distinct subscription per run. The resulting edge points at "
        "the topic's dataset URN — identical to the URN of an ingested Pub/Sub "
        "topic — so the edge joins the catalogued topic entity. Off by "
        "default: enabling it needs pubsub.subscriptions.get "
        "(roles/pubsub.viewer) on each subscription's project. Any failure "
        "(missing dependency, missing permission, deleted topic) skips the "
        "edge with a warning, which is exactly the behavior when this is off.",
    )

    lineage_locations: List[str] = Field(
        default_factory=lambda: list(DEFAULT_LINEAGE_LOCATIONS),
        description="List of GCP regions to scan for Dataplex lineage data. "
        "By default, includes all supported multi-regions and regions. "
        "Narrowing this list from the default is critical for better performance "
        "because lineage API calls scale with configured project/location pairs. "
        "This list may include multi-regions and single regions. "
        "In practice, lineage often resides in job regions while entries may be in "
        "multi-regions, so entries_locations and lineage_locations are configured separately. "
        "Example: ['eu', 'us-central1', 'europe-west1'].",
    )

    lineage_max_retries: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Maximum number of retry attempts for lineage API calls when encountering transient errors "
        "(timeouts, rate limits, service unavailable). Each attempt uses exponential backoff. "
        "Higher values increase resilience but may slow down ingestion. Default: 3.",
    )

    lineage_retry_backoff_multiplier: float = Field(
        default=1.0,
        ge=0.1,
        le=30.0,
        description="Multiplier for exponential backoff between lineage API retry attempts (in seconds). "
        "Wait time formula: multiplier * (2 ^ attempt_number), floored at 2 seconds and capped at "
        "'lineage_retry_max_wait_seconds'. "
        "Higher values reduce API load but increase ingestion time. Default: 1.0.",
    )

    lineage_retry_max_wait_seconds: int = Field(
        default=65,
        ge=2,
        le=600,
        description="Upper cap (seconds) on the exponential backoff between lineage API retries. "
        "Defaults to just over the Data Lineage API's 60-second per-minute quota window so that a "
        "retry can land in a fresh window instead of burning every attempt inside the same "
        "exhausted one. Only reached with a raised 'lineage_max_retries' / "
        "'lineage_retry_backoff_multiplier'. Default: 65.",
    )

    max_workers_entries: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of parallel worker threads for fetching entry details "
        "(get_entry API calls). Entry detail fetching is the main bottleneck in the "
        "entries stage because each entry requires one blocking RPC. Increasing this "
        "value reduces wall-clock time proportionally up to the API quota limit. "
        "Increase for large deployments (>1k entries). Default: 10.",
    )

    max_workers_lineage: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of parallel worker threads for lineage lookups "
        "(search_links API calls). Lineage lookup volume scales with entries × "
        "lineage_locations, so parallelism here has a large impact on total "
        "ingestion time. Increase for large entry × location matrices. Default: 10.",
    )

    lineage_max_calls_per_minute: int = Field(
        default=1000,
        ge=1,
        description="Client-side rate limit for Data Lineage API read calls, "
        "enforced across all lineage worker threads so the connector paces "
        "itself instead of relying on 429-and-retry. Google documents the read "
        "quota as 1000 requests/minute/project/user/region, which is the "
        "default here; lower it when several pipelines share the same quota. "
        "Default: 1000.",
    )

    glossary_lookup_max_calls_per_minute: int = Field(
        default=600,
        ge=1,
        description="Client-side rate limit for Dataplex lookupEntryLinks read "
        "calls, enforced across all glossary worker threads so the term-asset "
        "association scan paces itself instead of provoking 429s. Only applies "
        "when 'include_glossary_term_associations' is enabled. Default: 600.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for stale metadata removal.",
    )

    include_glossaries: bool = Field(
        default=True,
        description=(
            "Whether to ingest Dataplex Business Glossary entities as DataHub GlossaryNodes "
            "and GlossaryTerms. Glossaries, categories, and terms are emitted with correct "
            "parent hierarchy. Default: True."
        ),
    )

    include_glossary_term_associations: bool = Field(
        default=False,
        description=(
            "Whether to ingest term-to-asset associations via the Dataplex lookupEntryLinks API. "
            "For each ingested term, the API is called at the term's location to retrieve all linked "
            "assets (regardless of where those assets are located). "
            "Requires a role granting resourcemanager.projects.get (e.g. roles/browser) "
            "on all configured projects to resolve GCP project numbers needed by the "
            "lookupEntryLinks API."
        ),
    )

    glossary_locations: List[str] = Field(
        default_factory=lambda: ["global"],
        description=(
            "GCP locations to scan for Dataplex Business Glossaries. "
            "Dataplex glossaries are typically created in 'global' but can exist in any location. "
            "Default: ['global']."
        ),
    )

    max_workers_glossary: int = Field(
        default=10,
        ge=1,
        le=100,
        description=(
            "Number of parallel worker threads for glossary ingestion (fetching terms and "
            "categories per glossary) and term-asset association traversal "
            "(lookupEntryLinks calls). Default: 10."
        ),
    )

    dataplex_url: str = Field(
        default="https://console.cloud.google.com/dataplex",
        description="Base URL for Dataplex console (for generating external links).",
    )

    @model_validator(mode="before")
    @classmethod
    def project_id_backward_compatibility(cls, values: Dict) -> Dict:
        """Handle backward compatibility for project_id -> project_ids migration."""
        # Pydantic passes the raw input dict to mode="before" validators.
        # We create a new dict to avoid mutating the input (important for dict reuse in tests).
        project_id = values.get("project_id")
        project_ids = values.get("project_ids")

        if not project_ids and project_id:
            # Create a new dict without project_id, adding project_ids
            result = {k: v for k, v in values.items() if k != "project_id"}
            result["project_ids"] = [project_id]
            return result
        elif project_ids and project_id:
            logging.warning(
                "Both project_id and project_ids are set. Using project_ids. "
                "The project_id config is deprecated, please use project_ids instead."
            )
            # Remove project_id from the dict
            return {k: v for k, v in values.items() if k != "project_id"}

        return values

    @field_validator("project_labels")
    @classmethod
    def _validate_project_labels_format(cls, v: List[str]) -> List[str]:
        try:
            validate_project_label_list(v)
        except GCPValidationError as e:
            raise ValueError(str(e)) from e
        return v

    @model_validator(mode="after")
    def validate_project_ids(self) -> "DataplexConfig":
        """Ensure at least one means of selecting projects is configured."""
        has_non_default_pattern = (
            self.project_id_pattern != AllowDenyPattern.allow_all()
        )
        if (
            not self.project_ids
            and not self.project_labels
            and not has_non_default_pattern
        ):
            raise ValueError(
                "At least one project selector must be specified. Set project_ids "
                "explicitly, or use project_id_pattern / project_labels to "
                "auto-discover projects."
            )
        return self

    @model_validator(mode="after")
    def validate_location_configuration(self) -> "DataplexConfig":
        """Validate location configuration and warn about common mistakes."""
        if not self.entries_locations:
            raise ValueError(
                "At least one entries location must be specified via entries_locations."
            )
        if not self.lineage_locations:
            raise ValueError(
                "At least one lineage location must be specified via lineage_locations."
            )
        if self.include_glossaries and not self.glossary_locations:
            raise ValueError(
                "At least one glossary location must be specified via glossary_locations "
                "when include_glossaries is enabled."
            )

        return self

    @model_validator(mode="after")
    def validate_column_lineage_dependencies(self) -> "DataplexConfig":
        """Column lineage needs table lineage and schemas; degrade rather than fail.

        Turning ``include_lineage`` off must stay a safe escape hatch.
        """
        unmet = [
            name
            for name, enabled in (
                ("include_lineage", self.include_lineage),
                ("include_schema", self.include_schema),
            )
            if not enabled
        ]
        if self.include_column_lineage and unmet:
            logger.warning(
                "Disabling 'include_column_lineage': it requires %s to be enabled. "
                "Column-level lineage will be skipped; table-level lineage and the "
                "rest of Dataplex ingestion are unaffected. Set "
                "'include_column_lineage: false' explicitly to silence this.",
                " and ".join(f"'{name}'" for name in unmet),
            )
            self.include_column_lineage = False
        return self

    @model_validator(mode="after")
    def validate_lineage_operation_dependencies(self) -> "DataplexConfig":
        """Same degrade-don't-raise contract as column lineage."""
        if self.include_lineage_operations and not self.include_lineage:
            logger.warning(
                "Disabling 'include_lineage_operations': it requires "
                "'include_lineage' to be enabled. Set "
                "'include_lineage_operations: false' explicitly to silence this."
            )
            self.include_lineage_operations = False
        if self.include_lineage_operation_sql and not self.include_lineage_operations:
            logger.warning(
                "Disabling 'include_lineage_operation_sql': it requires "
                "'include_lineage_operations' to be enabled. Set "
                "'include_lineage_operation_sql: false' explicitly to silence this."
            )
            self.include_lineage_operation_sql = False
        return self

    @model_validator(mode="after")
    def validate_extraction_method_configuration(self) -> "DataplexConfig":
        """One rule: the selected method's config block must be present, the others absent."""
        required_block_by_method = {
            "api": None,
            "export": "export_config",
            "read_export": "read_export_config",
        }
        required_block = required_block_by_method[self.extraction_method]
        for block in ("export_config", "read_export_config"):
            if block == required_block:
                if getattr(self, block) is None:
                    raise ValueError(
                        f"{block} must be set when extraction_method is "
                        f"'{self.extraction_method}'."
                    )
            elif getattr(self, block) is not None:
                raise ValueError(
                    f"{block} is set but extraction_method is "
                    f"'{self.extraction_method}'. Remove it, or switch "
                    "extraction_method to the matching value."
                )

        if self.extraction_method != "export":
            return self
        assert self.export_config is not None
        # A key that is present but blank would pass the missing-bucket check
        # below and produce an invalid 'gs:///...' output path at runtime.
        blank_buckets = [
            loc
            for loc, bucket in self.export_config.export_bucket_config.items()
            if not bucket.strip()
        ]
        if blank_buckets:
            raise ValueError(
                f"export_bucket_config entries for locations {blank_buckets} are "
                "blank. Provide a bucket name or remove those entries."
            )
        # Every entries location must resolve to a bucket up front, so a
        # misconfiguration fails at recipe validation rather than mid-run.
        missing = [
            loc
            for loc in self.entries_locations
            if loc not in self.export_config.export_bucket_config
            and not self.export_config.bucket_base_name
        ]
        if missing:
            raise ValueError(
                f"Locations {missing} have no export bucket configured. Either add "
                "them to 'export_config.export_bucket_config' or set "
                "'export_config.bucket_base_name' as a fallback."
            )
        return self

    def get_credentials(self) -> Optional[Dict[str, str]]:
        """Get credentials dictionary for authentication."""
        if self.credential:
            # Use the first project_id for credential context
            project_id = self.project_ids[0] if self.project_ids else None
            return self.credential.to_dict(project_id)
        return None
