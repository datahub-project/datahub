from typing import Dict, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.sap_datasphere.constants import (
    PLATFORM_NAME_RE,
    XSUAA_URL_RE,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class ConnectionPlatformConfig(BaseModel):
    """How to project a SAP Datasphere source connection (or the Datasphere tenant's own
    managed storage) onto a DataHub platform.

    One of these is resolved per asset, then used to construct the asset's DatasetUrn.
    """

    platform: str = Field(
        description="The DataHub platform name (e.g. 'hana', 'snowflake', 's3'). Required.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Optional DataHub platform_instance. Use this to align with the "
            "platform_instance a separate connector (e.g. the DataHub Snowflake "
            "connector) emits for the same physical system."
        ),
    )
    env: Optional[str] = Field(
        default=None,
        description=(
            "Optional environment override (PROD/DEV/STAGING/...). Falls back to "
            "the connector's top-level `env` when None. Must be a valid DataHub "
            "FabricType."
        ),
    )
    enabled: bool = Field(
        default=True,
        description=(
            "If False, assets routed to this connection are skipped (counted under "
            "`assets_skipped_disabled` in the ingestion report)."
        ),
    )

    @field_validator("platform")
    @classmethod
    def _validate_platform(cls, v: str) -> str:
        # Don't enforce membership in the bundled platform registry (custom
        # platforms are legitimate), but enforce the kebab-case-ish shape so a
        # typo surfaces at config-load time rather than as a malformed URN.
        if not v or not v.strip():
            raise ValueError("platform must be a non-empty string")
        if not PLATFORM_NAME_RE.match(v):
            raise ValueError(
                f"platform={v!r} should be lowercase with hyphens/underscores only "
                f"(e.g., 'snowflake', 'bigquery', 'sap-hana')."
            )
        return v

    @field_validator("env")
    @classmethod
    def _validate_env(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        # Enumerate allowed envs from FabricTypeClass so the connector stays in
        # sync with the schema layer without hard-coding the full set.
        from datahub.metadata.schema_classes import FabricTypeClass

        allowed = {
            getattr(FabricTypeClass, name)
            for name in dir(FabricTypeClass)
            if not name.startswith("_")
            and isinstance(getattr(FabricTypeClass, name), str)
        }
        if v not in allowed:
            raise ValueError(
                f"env={v!r} is not a valid DataHub FabricType. "
                f"Allowed: {sorted(allowed)}"
            )
        return v


_BUILTIN_PLATFORM_TYPE_DEFAULTS: Dict[str, ConnectionPlatformConfig] = {
    # Keys are the canonical `typeId` values returned by
    # `/api/v1/datasphere/spaces/{space}/connections`.
    # Verified against a trial Datasphere tenant.
    #
    # SAP platform names deliberately match the sibling SAP connectors so lineage
    # stitches to the same dataPlatform URN. The SAC connector emits the bare,
    # lowercased system type (`hana`, `bw` — see sac.py `model.system_type.lower()`)
    # and the HANA SQL connector registers `id="hana"`. Follow that convention here
    # (no `sap-` prefix) for the whole SAP family.
    "HANA": ConnectionPlatformConfig(platform="hana"),
    "MSSQL": ConnectionPlatformConfig(platform="mssql"),
    "S3": ConnectionPlatformConfig(platform="s3"),
    "GCS": ConnectionPlatformConfig(platform="gcs"),
    "ABAP": ConnectionPlatformConfig(platform="abap"),
    "SAPS4HANACLOUD": ConnectionPlatformConfig(platform="s4hana"),
    "SAPBWMODELTRANSFER": ConnectionPlatformConfig(platform="bw"),
    # Other typeIds we haven't observed in a live tenant (Snowflake, BigQuery, Kafka,
    # Salesforce, etc.) default to disabled with a warning. Users opt in by adding
    # them under `platform_type_defaults` in their recipe.
}


class SpaceContainerKey(ContainerKey):
    space: str


class SapDatasphereConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    IncrementalLineageConfigMixin,
):
    base_url: str = Field(
        description="SAP Datasphere tenant URL, e.g. https://foo.eu10.hcs.cloud.sap"
    )
    # Back-compat alias for the legacy `tenant_url` field.
    _rename_tenant_url_to_base_url = pydantic_renamed_field(  # type: ignore[pydantic-field]
        "tenant_url", "base_url"
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful Ingestion Config with stale metadata removal (soft-deletes spaces/assets that disappear between runs).",
    )
    xsuaa_url: Optional[str] = Field(
        default=None,
        description="XSUAA token endpoint base URL. Auto-derived from base_url if not set.",
    )

    # Auth — priority: token > refresh_token > client_id+client_secret
    token: Optional[SecretStr] = Field(
        default=None,
        description="Raw bearer token for dev/testing. Takes priority over all OAuth flows.",
    )
    refresh_token: Optional[SecretStr] = Field(
        default=None,
        description="OAuth refresh token (authorization code flow). Takes priority over client credentials.",
    )
    client_id: Optional[str] = Field(
        default=None, description="XSUAA OAuth client ID for client credentials flow."
    )
    client_secret: Optional[SecretStr] = Field(
        default=None,
        description="XSUAA OAuth client secret for client credentials flow.",
    )

    space_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter SAP Datasphere spaces by name. "
            "Spaces are emitted as DataHub containers. "
            "Example: `{allow: ['^PROD_.*']}` to ingest only spaces with names starting with 'PROD_'."
        ),
    )
    asset_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter assets (views, analytical models, tables) by name within each space. "
            "Applied after space_pattern."
        ),
    )
    column_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex patterns to filter columns from emitted schemas. "
            "Applied to the EDMX property name. Reduces payload size for "
            "wide tables in large catalogs."
        ),
    )

    request_timeout_sec: int = Field(
        default=30,
        ge=1,
        le=600,
        description=(
            "Per-request timeout (seconds) for HTTP calls to the SAP Datasphere catalog and consumption APIs. "
            "Increase if you see frequent timeouts on large EDMX documents."
        ),
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description=(
            "Maximum number of retries for transient HTTP errors (5xx, connection errors) "
            "when calling SAP Datasphere APIs. Uses exponential backoff."
        ),
    )
    max_workers_assets: int = Field(
        default=10,
        ge=1,
        le=64,
        description=(
            "Maximum number of parallel workers for per-asset metadata fetch "
            "(EDMX schema + CSN lineage). At 1M assets this is the dominant "
            "performance lever. Default 10 gives ~10x speedup vs serial. Set to 1 "
            "to disable parallelism."
        ),
    )
    asset_batch_size: int = Field(
        default=5000,
        ge=1,
        le=100000,
        description=(
            "How many assets are submitted to the parallel executor at a time. "
            "The connector processes each space's assets in batches of this size so "
            "memory stays bounded regardless of how many assets a single space holds "
            "(the executor otherwise submits every task up front). Larger = slightly "
            "better parallelism overlap at higher memory; the default keeps peak "
            "memory low while preserving effectively full parallelism (batch size >> "
            "max_workers_assets). Only relevant when max_workers_assets > 1."
        ),
    )
    expose_for_consumption_only: bool = Field(
        default=False,
        description=(
            "If True, only emit datasets that have an `assetRelationalMetadataUrl` set "
            "(i.e., assets exposed for consumption via the SAP Datasphere consumption API). "
            "Assets without an exposure URL are counted in `assets_filtered`. "
            "Useful when you want DataHub to only show consumable views."
        ),
    )
    convert_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "Whether to lowercase emitted dataset and container URNs. The original "
            "case is preserved in display names and custom properties. Defaults to "
            "True to match the convention used by Snowflake and other DataHub sources "
            "so lineage merges case-insensitively across connectors."
        ),
    )
    include_lineage: bool = Field(
        default=False,
        description=(
            "Extract lineage from each asset's CSN. When enabled, the connector "
            "emits table-level upstream references (from SELECT FROM clauses and "
            "@remote.source annotations) AND column-level FineGrainedLineage "
            "(walking SELECT columns[] expressions). Uses the SAP-supported "
            "per-object-type endpoint under `/dwaas-core/api/v1/spaces/X/"
            "{views,analyticmodels}/Y` (same surface the official `datasphere` "
            "CLI uses); assets where CSN fetch fails are logged to "
            "`report.assets_csn_fetch_failed` and emitted without lineage. "
            "Cost: one extra HTTP call per asset."
        ),
    )
    emit_sap_semantics_as_tags: bool = Field(
        default=True,
        description=(
            "If True (default), emit SAP CDS semantic annotations "
            "(@Analytics.Dimension/Measure, @Common.IsCalendar*, "
            "@Common.IsCurrency/IsUnit, @Analytics.DimensionType) as DataHub "
            "tags on the relevant schema fields or datasets. Tags are "
            "searchable/filterable in DataHub Search and render as colored "
            "badges in the UI. The existing `sap_*` custom properties are "
            "emitted in parallel (additive) for backward compat — no breaking "
            "change. Set to False to suppress tag emission entirely."
        ),
    )
    include_local_tables: bool = Field(
        default=False,
        description=(
            "If True, ALSO discover Datasphere Local Tables (base tables not exposed "
            "for OData consumption) via the supported `/dwaas-core/api/v1/spaces/X/"
            "localtables` endpoint. These tables typically appear only as upstream "
            "lineage targets of views. Emitting them as Datasets closes the "
            "phantom-lineage gap. Their column schema is read from the per-table CSN "
            "when available (enabling column-level lineage to the base table); "
            "otherwise they are emitted as schema-less stubs carrying platform + "
            "subtype + container membership. Off by default to keep ingestion "
            "minimal; turn on when you want full catalog coverage."
        ),
    )

    include_data_flows: bool = Field(
        default=False,
        description=(
            "If True, discover SAP Datasphere Data Flows and emit each as a "
            "DataJob (grouped under a per-space DataFlow) with its source/target "
            "tables as input/output datasets and column-level lineage from the "
            "producer's attributeMappings. Data Flows are the primary lineage "
            "source for Local Tables populated by an ETL flow. Uses the supported "
            "`/dwaas-core/api/v1/spaces/X/dataflows` endpoint (one list call per "
            "space + one read call per flow)."
        ),
    )
    include_replication_flows: bool = Field(
        default=False,
        description=(
            "If True, discover SAP Datasphere Replication Flows and emit each as a "
            "DataJob with its source/target objects as input/output datasets and "
            "per-task column-level lineage. Replication Flows move data between two "
            "external systems; their source/target objects are routed to DataHub "
            "platforms via `connection_to_platform_map` / `platform_type_defaults` "
            "(objects on unmapped connections are skipped and reported under "
            "`flow_endpoints_unresolved`)."
        ),
    )
    include_remote_tables: bool = Field(
        default=False,
        description=(
            "If True, discover federated Remote Tables and emit each as a Dataset "
            "with upstream lineage to its external source object (parsed from the "
            "CSN `@DataWarehouse.remote.connection` / `@DataWarehouse.remote.entity` "
            "annotations). The external upstream is routed via "
            "`connection_to_platform_map` / `platform_type_defaults`; unmapped "
            "connections leave the remote table without upstream lineage "
            "(reported under `remote_table_source_unresolved`)."
        ),
    )
    include_transformation_flows: bool = Field(
        default=False,
        description=(
            "EXPERIMENTAL. If True, discover Transformation Flows and emit them as "
            "DataJobs. This path is parsed with the Data Flow process-graph reader "
            "but has NOT been verified against a live Transformation Flow payload, "
            "so lineage may be incomplete. Enable only if you can validate the "
            "output; please report payload samples so the parser can be hardened."
        ),
    )
    include_task_chains: bool = Field(
        default=False,
        description=(
            "EXPERIMENTAL. If True, discover Task Chains and emit each as a DataJob. "
            "No live Task Chain payload was available to reverse-engineer the "
            "member/reference grammar, so chains are currently surfaced as IO-less "
            "jobs (their presence + subtype only) without lineage edges. Enable to "
            "catalogue the objects; please report payload samples so lineage can be "
            "added."
        ),
    )

    include_view_definitions: bool = Field(
        default=True,
        description=(
            "Whether to emit each View's / Analytic Model's definition as the "
            "DataHub viewProperties aspect (shown in the View Definition tab). "
            "SQL-editor views emit their raw SQL with viewLanguage 'SQL'; "
            "graphical/modeled views emit the Core Schema Notation (CSN/CQN) query "
            "tree with viewLanguage 'CSN'. Requires fetching the CSN, which is also "
            "fetched when include_lineage is enabled."
        ),
    )

    # Back-compat alias for the legacy `include_table_lineage` field. Remove in a
    # future release once recipes have migrated.
    _rename_include_table_lineage = pydantic_renamed_field(  # type: ignore[pydantic-field]
        "include_table_lineage", "include_lineage"
    )

    connection_to_platform_map: Dict[str, ConnectionPlatformConfig] = Field(
        default_factory=dict,
        description=(
            "Per-Datasphere-connection mapping to DataHub platform/platform_instance/env "
            "for FEDERATED Remote Tables. Keyed by the Datasphere connection name (as "
            "returned by the Datasphere REST connections API). Managed assets (Views, "
            "Analytical Models, Local Tables that live in the tenant's own HANA Cloud) "
            "always emit on the `sap_datasphere` platform regardless of this mapping. "
            "Use this only to align FEDERATED Datasphere assets' URNs with the URNs "
            "other DataHub connectors emit for the same physical sources (e.g. snowflake)."
        ),
    )

    platform_type_defaults: Dict[str, ConnectionPlatformConfig] = Field(
        default_factory=lambda: dict(_BUILTIN_PLATFORM_TYPE_DEFAULTS),
        description=(
            "Fallback platform mapping keyed by Datasphere connection typeId (HANA, "
            "SNOWFLAKE, S3, etc.). Used when an asset's connection is NOT explicitly "
            "listed in `connection_to_platform_map`. Built-in defaults cover HANA, MSSQL, "
            "S3, GCS, ABAP, SAPS4HANACLOUD, SAPBWMODELTRANSFER; other typeIds default to "
            "disabled with a warning until you add them here."
        ),
    )

    @field_validator("base_url")
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        # A missing scheme silently produces broken request URLs and an unmatched
        # xsuaa_url, surfacing far downstream as a confusing OAuth error; validate
        # here where the message is actionable.
        if not v.startswith(("http://", "https://")):
            raise ValueError(
                f"base_url must start with 'https://' (or 'http://'), "
                f"e.g. 'https://foo.eu10.hcs.cloud.sap'; got '{v}'"
            )
        return v.rstrip("/")

    @field_validator("xsuaa_url")
    @classmethod
    def strip_xsuaa_trailing_slash(cls, v: Optional[str]) -> Optional[str]:
        return v.rstrip("/") if v else v

    @model_validator(mode="after")
    def derive_xsuaa_url(self) -> "SapDatasphereConfig":
        if self.xsuaa_url is None:
            match = XSUAA_URL_RE.match(self.base_url)
            if match:
                subdomain, region = match.group(1), match.group(2)
                self.xsuaa_url = (
                    f"https://{subdomain}.authentication.{region}.hana.ondemand.com"
                )
        return self

    @model_validator(mode="after")
    def _at_least_one_credential(self) -> "SapDatasphereConfig":
        # Runs after derive_xsuaa_url, so self.xsuaa_url reflects any auto-derived
        # value. Enforce supporting XSUAA fields even when a raw token is also
        # supplied, so incomplete OAuth config fails up-front rather than at
        # bearer-expiry mid-run.
        if self.refresh_token is not None and (
            self.xsuaa_url is None or self.client_id is None
        ):
            raise ValueError(
                "refresh_token requires both `client_id` and `xsuaa_url` to be set "
                "(the XSUAA token endpoint is used to exchange the refresh token)."
            )
        if self.client_secret is not None and (
            self.xsuaa_url is None or self.client_id is None
        ):
            raise ValueError(
                "client_secret requires both `client_id` and `xsuaa_url` to be set "
                "(the XSUAA token endpoint is used for the client_credentials grant)."
            )

        has_raw_token = bool(self.token)
        has_refresh = bool(self.refresh_token and self.client_id and self.xsuaa_url)
        has_client_creds = bool(
            self.client_id and self.client_secret and self.xsuaa_url
        )
        if not (has_raw_token or has_refresh or has_client_creds):
            raise ValueError(
                "At least one credential path must be configured for SAP Datasphere: "
                "(1) `token` (raw bearer token), or "
                "(2) `refresh_token` + `client_id` + `xsuaa_url`, or "
                "(3) `client_id` + `client_secret` + `xsuaa_url` (client_credentials grant)."
            )
        return self

    @model_validator(mode="after")
    def _merge_builtin_platform_type_defaults(self) -> "SapDatasphereConfig":
        # Merge user overrides on top of the built-in table so the user only lists
        # the entries they want to add or change.
        merged: Dict[str, ConnectionPlatformConfig] = dict(
            _BUILTIN_PLATFORM_TYPE_DEFAULTS
        )
        merged.update(self.platform_type_defaults)
        self.platform_type_defaults = merged
        return self
