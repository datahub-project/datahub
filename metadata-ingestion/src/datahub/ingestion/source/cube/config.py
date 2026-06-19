import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration._config_enum import ConfigEnum
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.cube.constants import (
    DEFAULT_REQUEST_TIMEOUT_SEC,
    MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.mapping import Constants

logger = logging.getLogger(__name__)


class CubeDeploymentType(ConfigEnum):
    CORE = "CORE"
    CLOUD = "CLOUD"


class CubeSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    IncrementalLineageConfigMixin,
):
    api_url: str = Field(
        description=(
            "Base URL of the Cube REST API, including the base path. "
            "For Cube Core this is typically `http://localhost:4000/cubejs-api`; "
            "for Cube Cloud it looks like `https://<name>.cubecloud.dev/cubejs-api`."
        ),
    )
    api_token: SecretStr = Field(
        description=(
            "API token used to authenticate against Cube. For Cube Core this is a "
            "JWT signed with `CUBEJS_API_SECRET`; for the Cube Cloud Metadata API "
            "use a token obtained from the Control Plane API."
        ),
    )
    deployment_type: CubeDeploymentType = Field(
        default=CubeDeploymentType.CORE,
        description="Whether the target is Cube Core (`CORE`) or Cube Cloud (`CLOUD`).",
    )
    use_metadata_api: bool = Field(
        default=True,
        description=(
            "Cube Cloud only. When enabled, the richer Metadata API "
            "(`/v1/entities`) is used to extract warehouse and column-level "
            "lineage, which is merged with the structural metadata from "
            "`/v1/meta`. When disabled, only the `/v1/meta` endpoint is used. "
            "Has no effect for Cube Core deployments."
        ),
    )

    cloud_api_key: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Cube Cloud Control Plane API key (Account → API keys). When set "
            "together with `deployment_id` and `environment_id`, the connector "
            "automatically mints a metadata-scoped JWT via the Control Plane "
            "`tokens-for-meta-sync` endpoint to access the Metadata API, instead "
            "of requiring a pre-generated token in `api_token`."
        ),
    )
    cloud_api_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the Cube Cloud Control Plane API (e.g. "
            "`https://<tenant>.cubecloud.dev`). If unset, it is derived from the "
            "scheme and host of `api_url`. Only used when `cloud_api_key` is set."
        ),
    )
    deployment_id: Optional[str] = Field(
        default=None,
        description="Cube Cloud deployment id, used to mint a Metadata API token via the Control Plane API.",
    )
    environment_id: Optional[str] = Field(
        default=None,
        description="Cube Cloud environment id, used to mint a Metadata API token via the Control Plane API.",
    )
    security_context: Dict = Field(
        default={},
        description=(
            "Security context embedded in the minted Metadata API token. Controls "
            "which parts of the data model are visible, following Cube's "
            "multi-tenancy rules."
        ),
    )
    meta_sync_token_expires_in: int = Field(
        default=86400,
        description="Expiry (in seconds) of the minted Metadata API token. Defaults to 24 hours.",
    )

    request_timeout_sec: int = Field(
        default=DEFAULT_REQUEST_TIMEOUT_SEC,
        description="Per-request timeout, in seconds.",
    )

    include_cubes: bool = Field(
        default=True,
        description="Whether to ingest base cubes as datasets.",
    )
    include_views: bool = Field(
        default=True,
        description="Whether to ingest views as datasets.",
    )
    cube_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for filtering cubes to ingest (matched on the cube name).",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for filtering views to ingest (matched on the view name).",
    )

    ingest_lineage: bool = Field(
        default=True,
        description=(
            "Whether to emit lineage. This includes view->cube lineage and, where "
            "available, lineage from cubes to their upstream warehouse tables."
        ),
    )
    include_column_lineage: bool = Field(
        default=True,
        description=(
            "Whether to emit column-level (fine-grained) lineage. Requires "
            "`ingest_lineage` to be enabled."
        ),
    )
    parse_sql_for_lineage: bool = Field(
        default=True,
        description=(
            "Cube Core only. When the `/v1/meta?extended` response includes a cube's "
            "SQL definition, parse it to derive upstream warehouse lineage. Requires "
            "`warehouse_platform` to be set. The Cloud Metadata API provides lineage "
            "directly, so this is ignored for Cube Cloud."
        ),
    )

    warehouse_platform: Optional[str] = Field(
        default=None,
        description=(
            "DataHub platform name of the warehouse that backs the Cube data model "
            "(e.g. `snowflake`, `bigquery`, `postgres`). Used to build upstream "
            "lineage URNs. If unset, it is auto-detected from the Cube data source "
            "type when the Metadata API is available."
        ),
    )
    warehouse_platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of the upstream warehouse, used when building lineage URNs.",
    )
    warehouse_env: str = Field(
        default=DEFAULT_ENV,
        description="Environment of the upstream warehouse datasets referenced by lineage.",
    )
    warehouse_database: Optional[str] = Field(
        default=None,
        description=(
            "Database name to prepend to upstream warehouse table references that do "
            "not already include one. If unset, it is taken from the Cube data source "
            "definition when available."
        ),
    )
    emit_siblings: bool = Field(
        default=True,
        description=(
            "Whether to emit a sibling relationship between a cube and its upstream "
            "warehouse table when the cube is a 1:1 projection of a single table "
            "(not a view, no joins to other cubes). Mirrors the dbt connector's "
            "sibling behaviour so the cube and table are merged in the UI."
        ),
    )
    cube_is_primary_sibling: bool = Field(
        default=False,
        description=(
            "When a sibling relationship is emitted, controls whether the Cube "
            "dataset or the warehouse table is the primary sibling. Defaults to the "
            "warehouse table being primary."
        ),
    )
    convert_lineage_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "Whether to lowercase upstream warehouse table and column names when "
            "building lineage URNs. Must match the `convert_urns_to_lowercase` "
            "setting of the warehouse connector (e.g. Snowflake ingests lowercased "
            "URNs by default) so that the lineage edges resolve."
        ),
    )

    deployment_url: Optional[str] = Field(
        default=None,
        description=(
            "Base URL of the Cube deployment UI, used to build an external link on "
            "the deployment container. If unset, it is derived from `api_url` by "
            "stripping the API base path."
        ),
    )

    tag_measures_and_dimensions: bool = Field(
        default=True,
        description=(
            "Whether to tag schema fields with `Measure`/`Dimension` (and `Temporal` "
            "for time dimensions) so the kinds of Cube members can be distinguished "
            "and filtered in DataHub."
        ),
    )

    include_hidden: bool = Field(
        default=False,
        description=(
            "Whether to ingest cubes, views, and members that Cube marks as hidden "
            "(`public: false` / `isVisible: false`). Hidden cubes are typically "
            "excluded from Cube's own API consumers; enable this to surface them in "
            "DataHub anyway."
        ),
    )

    emit_member_details: bool = Field(
        default=True,
        description=(
            "Whether to capture Cube member presentation hints (format, drill-down "
            "members, cumulative flag) as schema-field `jsonProps`, and structural "
            "metadata (joins, hierarchies, folders, pre-aggregations) as dataset "
            "custom properties."
        ),
    )

    enable_meta_mapping: bool = Field(
        default=True,
        description="Whether to process `meta_mapping` and `column_meta_mapping` rules.",
    )
    meta_mapping: Dict = Field(
        default={},
        description=(
            "Mapping rules applied to the `meta` of each cube/view to derive tags, "
            "glossary terms, owners, domains, and documentation links. Uses the same "
            "syntax as the dbt connector's `meta_mapping`."
        ),
    )
    column_meta_mapping: Dict = Field(
        default={},
        description=(
            "Mapping rules applied to the `meta` of each measure/dimension to derive "
            "schema-field tags and glossary terms."
        ),
    )
    tag_prefix: str = Field(
        default="",
        description="Prefix added to tags created via `meta_mapping`.",
    )
    strip_user_ids_from_email: bool = Field(
        default=False,
        description="Whether to strip the email domain from owners derived via `meta_mapping`.",
    )

    domain: Dict[str, AllowDenyPattern] = Field(
        default={},
        description=(
            "Regex patterns matched against a cube/view name to assign it to a "
            "DataHub domain (keyed by domain id or urn)."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration.",
    )

    @field_validator("meta_mapping", "column_meta_mapping")
    @classmethod
    def validate_meta_mapping(cls, value: Dict) -> Dict:
        for key, mapping in value.items():
            if Constants.OPERATION not in mapping:
                raise ValueError(
                    f"meta_mapping '{key}' is missing an '{Constants.OPERATION}' key"
                )
            if Constants.MATCH not in mapping:
                raise ValueError(
                    f"meta_mapping '{key}' is missing a '{Constants.MATCH}' key"
                )
        return value

    @field_validator("api_url")
    @classmethod
    def validate_api_url(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("api_url cannot be empty")
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("api_url must start with http:// or https://")
        return v.rstrip("/")

    @model_validator(mode="after")
    def validate_control_plane_trio(self) -> "CubeSourceConfig":
        # cloud_api_key, deployment_id, and environment_id must be supplied
        # together to mint a Metadata API token via the Control Plane API.
        provided = [
            bool(self.cloud_api_key),
            bool(self.deployment_id),
            bool(self.environment_id),
        ]
        if any(provided) and not all(provided):
            raise ValueError(
                "cloud_api_key, deployment_id, and environment_id must all be set "
                "together to mint a Metadata API token via the Control Plane API."
            )
        return self

    @field_validator("request_timeout_sec")
    @classmethod
    def validate_request_timeout_sec(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("request_timeout_sec must be positive")
        if v > MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD:
            logger.warning(
                f"request_timeout_sec is set to {v} seconds, which is quite high. "
                f"Consider a value below {MAX_REQUEST_TIMEOUT_WARNING_THRESHOLD} seconds."
            )
        return v


@dataclass
class CubeSourceReport(StaleEntityRemovalSourceReport):
    cubes_scanned: int = 0
    views_scanned: int = 0
    cubes_emitted: int = 0
    views_emitted: int = 0
    measures_scanned: int = 0
    dimensions_scanned: int = 0
    lineage_edges_emitted: int = 0
    column_lineage_edges_emitted: int = 0
    siblings_emitted: int = 0
    api_calls_count: int = 0
    data_sources_scanned: int = 0
    sql_parsing_failures: int = 0
    filtered_cubes: List[str] = dataclass_field(default_factory=list)
    filtered_views: List[str] = dataclass_field(default_factory=list)

    def report_entity_scanned(self, name: str, is_view: bool) -> None:
        if is_view:
            self.views_scanned += 1
        else:
            self.cubes_scanned += 1

    def report_entity_emitted(self, is_view: bool) -> None:
        if is_view:
            self.views_emitted += 1
        else:
            self.cubes_emitted += 1

    def report_entity_filtered(self, name: str, is_view: bool) -> None:
        if is_view:
            self.filtered_views.append(name)
        else:
            self.filtered_cubes.append(name)

    def report_api_call(self, count: int = 1) -> None:
        self.api_calls_count += count
