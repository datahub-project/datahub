from typing import Dict, List, Optional, Union

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import EnvConfigMixin
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mce_builder import ALL_ENV_TYPES, DEFAULT_ENV

# Display name and platform id for the logical-model platform ODCS contracts are
# materialized under. ODCS v3 describes a producer-published dataset spec, so it
# is modeled as a logical `dataset` on its own platform rather than a
# bilateral `dataContract`.
ODCS_PLATFORM = "odcs"


class ServerMapping(ConfigModel):
    """Maps an ODCS `servers[].server` identifier to a DataHub data platform.

    ODCS allows any free-form string in the `server` field of a server entry.
    To bind a logical ODCS dataset to a *physical* dataset (so quality rules can
    target real tables and the physical asset gets a `logicalParent` link), the
    source needs an explicit DataPlatform URN, which this mapping supplies.
    """

    server: str = Field(
        description="The value of `servers[].server` in the ODCS contract. "
        "If `match_any` is True, this is treated as a wildcard match for any server."
    )
    platform: str = Field(
        description="The DataHub data platform identifier (e.g. `postgres`, `snowflake`, `bigquery`)."
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="The environment / fabric type for the produced physical dataset URNs (e.g. PROD, DEV).",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional DataHub platform instance, used when the same platform is "
        "deployed multiple times (e.g. multiple Snowflake accounts).",
    )
    match_any: bool = Field(
        default=False,
        description="If True, this mapping matches any `servers[].server` not matched by a more specific mapping. "
        "Use a single catch-all mapping to apply one platform to all contracts.",
    )

    @field_validator("env")
    @classmethod
    def env_must_be_one_of(cls, v: str) -> str:
        if v.upper() not in ALL_ENV_TYPES:
            raise ValueError(f"env must be one of {ALL_ENV_TYPES}, found {v}")
        return v.upper()


class ODCSSourceConfig(EnvConfigMixin):
    """Config for the ODCS ingestion source.

    Reads Open Data Contract Standard (ODCS) v3.x YAML files from a path (file,
    directory, or glob) and materializes each `schema[]` entry as a **logical
    dataset** on the `odcs` platform: dataset properties, canonical schema
    metadata, ownership, tags, and a link to the source document. When a
    physical dataset can be resolved (via `servers_to_platform` or
    `physical_urn_overrides`), the source also emits a `logicalParent` link from
    the physical dataset and quality assertions against it.
    """

    path: Union[str, List[str]] = Field(
        description="Path to an ODCS YAML file, a directory containing ODCS YAML files, "
        "or a glob pattern. May also be a list of any of the above.",
    )
    servers_to_platform: List[ServerMapping] = Field(
        default_factory=list,
        description="List of mappings from ODCS `servers[].server` values to DataHub platforms. "
        "Used to resolve the *physical* dataset a logical ODCS dataset binds to. Optional: "
        "without a binding, the logical dataset and its schema are still emitted, but no "
        "physical `logicalParent` link or quality assertions are produced.",
    )
    tag_prefix: Optional[str] = Field(
        default=None,
        description="Optional prefix prepended to every tag emitted from ODCS `tags` and "
        "property-level `tags` (e.g. `odcs.`). Useful for distinguishing ODCS-sourced tags.",
    )
    strict_validation: bool = Field(
        default=False,
        description=(
            "If True, ODCS files failing JSON-Schema validation are skipped (logged as warnings). "
            "If False (default), schema violations are reported as warnings; the contract is still "
            "ingested. Default False matches real-world ODCS files that use deprecated forms (e.g. "
            "top-level `quality[]`); set True to fail-fast on schema violations."
        ),
    )
    replicate_contract_metadata: bool = Field(
        default=True,
        description=(
            "If True (default), contract-level Ownership and GlobalTags are written to the logical "
            "dataset on every ingest run. Set False to skip emitting contract-level Ownership and "
            "GlobalTags so manual UI edits to those aspects survive subsequent ingest runs (the "
            "contract is then a one-time enricher rather than a source of truth)."
        ),
    )
    state_file_path: Optional[str] = Field(
        default=None,
        description=(
            "Optional path to a JSON file used to track the logical `odcs` Dataset and Assertion "
            "URNs ODCS emitted on the previous run. On the next run, URNs ODCS no longer emits are "
            "soft-deleted (Status.removed=true). Physical datasets and their `logicalParent` links "
            "are NEVER soft-deleted by ODCS. If unset, no cross-run state is persisted; soft-delete "
            "falls back to a per-invocation diff (within-run consistency only)."
        ),
    )
    odcs_versions: List[str] = Field(
        default_factory=lambda: ["3.0.0", "3.0.1", "3.0.2", "3.1.0"],
        description="List of supported ODCS `apiVersion` values. Contracts with `apiVersion` "
        "outside this list are skipped with a warning.",
    )
    emit_assertions: bool = Field(
        default=True,
        description="Whether to emit Assertion entities derived from the ODCS `quality[]` rules. "
        "Assertions are only emitted for schema entries that resolve to a physical dataset "
        "(strict binding); without a binding no assertions are produced regardless of this flag.",
    )
    emit_logical_parent: bool = Field(
        default=True,
        description="Whether to emit a `logicalParent` link from each resolved physical dataset to "
        "its logical ODCS dataset (the `PhysicalInstanceOf` relationship). Disable to keep ODCS "
        "from writing any aspect onto physical datasets.",
    )
    physical_urn_overrides: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Map of ODCS contract `id` to a list of explicit physical DataHub Dataset URNs, "
        "one per `schema[]` entry in order. Bypasses the `servers_to_platform` lookup for the named "
        "contracts. Use an empty string to leave a given schema entry unbound. When supplied, the "
        "list is authoritative: any entry left empty — or omitted by a list shorter than `schema[]` — "
        "is treated as deliberately unbound and does NOT fall back to `servers_to_platform`.",
    )
    logical_dataset_name_template: str = Field(
        default="{contract_id}.{schema_name}",
        description="Template for the logical `odcs` dataset name (the URN name segment). "
        "Available placeholders: `{contract_id}`, `{schema_name}`, `{contract_version}`.",
    )
    file_extensions: List[str] = Field(
        default_factory=lambda: [".yaml", ".yml", ".odcs.yaml", ".odcs.yml"],
        description="File extensions considered ODCS YAML files when scanning a directory.",
    )
    max_input_file_bytes: int = Field(
        default=5 * 1024 * 1024,
        description="Maximum size of an ODCS YAML file to load. Files exceeding this are skipped "
        "with a warning. Defaults to 5 MB. This guards the YAML parser from unbounded inputs.",
    )
    follow_symlinks: bool = Field(
        default=False,
        description="If true, follow symlinks when discovering ODCS files. Off by default to "
        "prevent disclosure via symlink escape in shared/multi-tenant directories. When enabled, "
        "the source still requires that resolved targets stay within the configured root.",
    )

    # Removed when ODCS pivoted from emitting `dataContract` entities to logical
    # datasets: the raw contract was embedded in DataContractProperties.rawContract,
    # which no longer exists, and dataset URN overrides became physical_urn_overrides
    # (a list per contract to match the schema[] fan-out).
    _removed_raw_contract_size_limit = pydantic_removed_field(
        "raw_contract_size_limit_bytes", "June", 2026
    )
    _removed_dataset_urn_overrides = pydantic_removed_field(
        "dataset_urn_overrides", "June", 2026
    )
