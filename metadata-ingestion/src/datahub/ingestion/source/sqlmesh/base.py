import logging
import re
from typing import (
    Dict,
    List,
    Optional,
)

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sqlmesh.compat import (
    Snapshot,
    SqlmeshContextType,
    SqlmeshModel,
)
from datahub.ingestion.source.sqlmesh.constants import (
    DEFAULT_GATEWAY,
    DEFAULT_MODEL_SUBTYPE,
    ENV_SUFFIX_TARGET_CATALOG,
    ENV_SUFFIX_TARGET_TABLE,
    PROD_ENVIRONMENT,
    SNOWFLAKE_PLATFORM,
    SQLMESH_PLATFORM,
    UNKNOWN_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.models import (
    _CapabilityProbes,
    _EffectiveProjectConfig,
)
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    MODEL_KIND_TO_SUBTYPE,
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
    map_sqlmesh_platform,
)
from datahub.utilities.urns.tag_urn import TagUrn

logger = logging.getLogger(__name__)


class SqlmeshSourceBase:
    """Shared state + URN/naming/gateway-resolution helpers for SqlmeshSource.

    Duty mixins (lineage, assertions, profiling) inherit this so their
    cross-cutting calls to URN/effective-config helpers resolve, while the
    final SqlmeshSource composes them all."""

    config: SqlmeshSourceConfig
    report: SqlmeshSourceReport
    ctx: PipelineContext
    platform: str
    compiled_owner_extraction_pattern: Optional[re.Pattern]
    _platform_registered: bool
    _resolved_effective: Optional[_EffectiveProjectConfig]
    _effective_by_gateway: Dict[str, _EffectiveProjectConfig]
    _selected_gateway: Optional[str]
    _capabilities: _CapabilityProbes
    _sqlmesh_urn_by_model_key: Dict[str, str]
    _warned_column_lineage_unavailable: bool
    _warned_missing_gateways: set

    def _effective_from_config(self) -> _EffectiveProjectConfig:
        """Build the base _EffectiveProjectConfig straight from ``self.config``.

        The pre-Context-load view of the project (before auto-detection and
        per-gateway resolution run). Centralised so the several call sites that
        need this exact ``self.config``-to-model mapping don't drift when a new
        field is added.
        """
        return _EffectiveProjectConfig(
            project_path=self.config.project_path,
            gateway=self.config.gateway,
            environment=self.config.environment,
            target_platform=self.config.target_platform,
            target_platform_instance=self.config.target_platform_instance,
            sqlmesh_platform_instance=self.config.sqlmesh_platform_instance,
            default_catalog=self.config.default_catalog,
            convert_urns_to_lowercase=self.config.convert_urns_to_lowercase,
        )

    def _detect_target_platform(
        self, sqlmesh_ctx: "SqlmeshContextType", effective: _EffectiveProjectConfig
    ) -> str:
        if effective.target_platform:
            return effective.target_platform

        try:
            connection_type = sqlmesh_ctx.connection_config.type_
            platform = map_sqlmesh_platform(connection_type)
            if not platform:
                # A gateway with no connection type_ gives us nothing to map, so
                # fall through to the report.warning + "unknown" fallback below
                # rather than silently returning None.
                raise ValueError(
                    f"gateway connection has no usable type_: {connection_type!r}"
                )
            logger.info(
                "Auto-detected target_platform=%r from gateway connection type %r",
                platform,
                connection_type,
            )
            return platform
        except Exception as e:
            # Falling back to "unknown" yields structurally-valid-but-wrong
            # warehouse URNs (urn:li:dataPlatform:unknown,...), so record it on
            # the report — not just a logger.warning that never reaches the summary.
            self.report.warning(
                title="Could not auto-detect target_platform",
                message="Falling back to 'unknown'; warehouse sibling URNs will be wrong. Set target_platform explicitly in your recipe config.",
                exc=e,
            )
            return UNKNOWN_PLATFORM

    def _read_selected_gateway(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        effective: _EffectiveProjectConfig,
    ) -> str:
        """Return the canonical name of the currently-selected gateway.

        SQLMesh normalises gateway names to lowercase. We prefer the value
        SQLMesh resolved (``ctx.selected_gateway``) over the user's config
        because the user can omit the gateway and let the project default
        kick in.
        """
        gw = getattr(sqlmesh_ctx, "selected_gateway", None) or effective.gateway
        if not gw:
            # Final fallback — SQLMesh always has SOME selected gateway, but
            # be defensive about API drift.
            gw = DEFAULT_GATEWAY
        return str(gw).lower()

    def _build_per_gateway_effectives(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        default_effective: _EffectiveProjectConfig,
    ) -> Dict[str, _EffectiveProjectConfig]:
        """Build one _EffectiveProjectConfig per gateway visible to the Context.

        Single-gateway projects produce a one-entry dict that mirrors
        ``default_effective`` exactly — no behavioral change for those.

        For multi-gateway projects we iterate ``ctx.engine_adapters``, read
        each adapter's dialect to auto-detect ``target_platform``, and layer
        the user's ``gateway_overrides`` on top. The default gateway always
        appears under its own name so ``_effective_for_model`` can fall back
        when ``model.gateway`` is None.
        """
        result: Dict[str, _EffectiveProjectConfig] = {}

        # Always include the default gateway exactly as the caller resolved
        # it — preserves auto-detection / Snowflake-lowercase / etc. that
        # already ran for the default.
        result[
            self._selected_gateway or default_effective.gateway or DEFAULT_GATEWAY
        ] = default_effective

        # Discover additional gateways from ctx.engine_adapters when SQLMesh
        # exposes the multi-gateway map. Older / minimal Context mocks may
        # not, so degrade silently to single-gateway in that case.
        engine_adapters = getattr(sqlmesh_ctx, "engine_adapters", None) or {}
        default_catalogs = (
            getattr(sqlmesh_ctx, "default_catalog_per_gateway", None) or {}
        )

        for gw_name in engine_adapters:
            gw_key = str(gw_name).lower()
            if gw_key in result:
                continue  # already covered by the default-gateway entry

            override = self.config.gateway_overrides.get(
                gw_name
            ) or self.config.gateway_overrides.get(gw_key)

            # Auto-detect target_platform from this gateway's adapter dialect.
            # We can't reuse _detect_target_platform because it reads
            # ctx.connection_config (default-gateway only); for non-default
            # gateways we inspect engine_adapters[gw].dialect directly.
            auto_platform = None
            try:
                dialect = str(engine_adapters[gw_name].dialect).lower()
                auto_platform = map_sqlmesh_platform(dialect)
            except Exception as e:
                self.report.warning(
                    title="Could not auto-detect target_platform for a gateway",
                    message="Reading the gateway's engine-adapter dialect failed; set target_platform for this gateway under gateway_overrides or its warehouse sibling URNs will be wrong.",
                    context=gw_key,
                    exc=e,
                )
            target_platform = (
                (override.target_platform if override else None)
                or auto_platform
                or UNKNOWN_PLATFORM
            )
            if target_platform == UNKNOWN_PLATFORM:
                # Mirrors _detect_target_platform: an "unknown" platform yields
                # structurally-valid-but-wrong warehouse URNs, silently breaking
                # sibling stitching for every model on this gateway. Surface it.
                self.report.warning(
                    title="Unresolved target_platform for a gateway",
                    message="Falling back to 'unknown'; warehouse sibling URNs for models on this gateway will be wrong. Set target_platform for it under gateway_overrides.",
                    context=gw_key,
                )

            # convert_urns_to_lowercase: project-level default, with
            # auto-on for Snowflake matching the default-gateway logic.
            convert_lc = (
                override.convert_urns_to_lowercase
                if override and override.convert_urns_to_lowercase is not None
                else (
                    default_effective.convert_urns_to_lowercase
                    or target_platform == SNOWFLAKE_PLATFORM
                )
            )

            result[gw_key] = _EffectiveProjectConfig(
                project_path=default_effective.project_path,
                gateway=gw_name,
                environment=default_effective.environment,
                target_platform=target_platform,
                target_platform_instance=(
                    override.target_platform_instance if override else None
                ),
                sqlmesh_platform_instance=default_effective.sqlmesh_platform_instance,
                default_catalog=(override.default_catalog if override else None)
                or default_catalogs.get(gw_name),
                convert_urns_to_lowercase=convert_lc,
                env_suffix_target=default_effective.env_suffix_target,
                env_catalog_mapping=dict(default_effective.env_catalog_mapping),
            )

        return result

    def _effective_for_model(
        self, model: Optional["SqlmeshModel"]
    ) -> _EffectiveProjectConfig:
        """Resolve the right _EffectiveProjectConfig for this model's gateway.

        Falls back to the selected/default gateway when the model has no
        explicit ``gateway`` field, when the gateway isn't in our map (e.g.
        SQLMesh added it after Context load — unlikely), or when called
        with a None model (legacy code paths). Always returns a non-None
        value so callers can use it without further guarding.
        """
        if not self._effective_by_gateway:
            # Pre-Context-load paths — return _resolved_effective if set,
            # else a stub so we never raise.
            return self._resolved_effective or self._effective_from_config()

        gw_name = None
        if model is not None:
            gw_name = getattr(model, "gateway", None)
        gw_key = str(gw_name).lower() if gw_name else (self._selected_gateway or "")

        resolved = self._effective_by_gateway.get(gw_key)
        if resolved is not None:
            return resolved

        # The model names a gateway we don't have an effective config for.
        # Falling back to another gateway's config produces
        # structurally-valid-but-wrong warehouse sibling URNs and lineage
        # targets, exactly the failure mode _detect_target_platform /
        # _build_per_gateway_effectives surface — so warn (once per gateway)
        # rather than resolving silently. Only warn for an explicit,
        # unrecognised model gateway; a None gateway legitimately means "use
        # the selected gateway".
        if gw_name and gw_key not in self._warned_missing_gateways:
            self._warned_missing_gateways.add(gw_key)
            self.report.warning(
                title="Model gateway not found; using fallback config",
                message="A model references a gateway with no resolved effective config; its warehouse sibling URN and lineage may target the wrong platform. Ensure the gateway is defined in your SQLMesh project so it appears in ctx.engine_adapters, then set target_platform under gateway_overrides if auto-detection still fails.",
                context=gw_key,
            )

        # Last-resort: the selected gateway's config. Better than raising
        # and stopping ingest because of one quirky model.
        return self._effective_by_gateway.get(
            self._selected_gateway or "",
            self._resolved_effective or next(iter(self._effective_by_gateway.values())),
        )

    def _build_physical_name_map(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        effective: _EffectiveProjectConfig,
    ) -> Dict[str, str]:
        # Computed from model attributes rather than sqlmesh_ctx.snapshots,
        # which triggers an internal ProcessPoolExecutor(mp_context=fork) that
        # hangs on macOS when the DataHub async sink thread pool is running.
        # Used for a custom property only.
        result: Dict[str, str] = {}
        for model in sqlmesh_ctx.models.values():
            try:
                # Multi-gateway: each model's gateway has its own catalog
                # naming + URN-lowercasing settings. Single-gateway projects
                # see this as a no-op since every model resolves to the same
                # effective.
                model_effective = self._effective_for_model(model)
                catalog = getattr(model, "catalog", None)
                physical_schema = getattr(model, "physical_schema", None)
                schema_name = getattr(model, "schema_name", None)
                view_name = getattr(model, "view_name", None)
                data_hash = getattr(model, "data_hash", None)
                model_name = str(getattr(model, "name", ""))

                if physical_schema and schema_name and view_name and data_hash:
                    parts = [
                        f"{physical_schema}.{schema_name}__{view_name}__{data_hash}"
                    ]
                    if catalog:
                        parts = [catalog] + parts
                    phys = self._normalize_name(".".join(parts), model_effective)
                    logical_fqn = self._build_logical_fqn(model_name, model_effective)
                    result[logical_fqn] = phys
            except Exception as e:
                self.report.num_snapshots_without_physical_name += 1
                logger.debug(
                    "Could not resolve physical table name for model %s: %s",
                    getattr(model, "name", "?"),
                    e,
                )
        return result

    def _normalize_name(self, name: str, effective: _EffectiveProjectConfig) -> str:
        parts = []
        for part in name.split("."):
            cleaned = part.strip(" \t\"'`")
            if cleaned:
                parts.append(cleaned)
        joined = ".".join(parts)
        return joined.lower() if effective.convert_urns_to_lowercase else joined

    def _qualify_fqn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """Prepend default_catalog to 2-part names to match warehouse connector URNs."""
        if effective.default_catalog and fqn.count(".") < 2:
            catalog = effective.default_catalog
            if effective.convert_urns_to_lowercase:
                catalog = catalog.lower()
            return f"{catalog}.{fqn}"
        return fqn

    def _build_logical_fqn(
        self, raw_name: str, effective: _EffectiveProjectConfig
    ) -> str:
        return self._qualify_fqn(self._normalize_name(raw_name, effective), effective)

    def _make_sqlmesh_urn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=SQLMESH_PLATFORM,
            name=fqn,
            platform_instance=effective.sqlmesh_platform_instance,
            env=self.config.env,
        )

    def _apply_env_suffix(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """
        Apply SQLMesh's environment suffix to a model FQN to get the warehouse view name
        for non-prod environments.

        SQLMesh's environment_suffix_target config controls where the env name is appended:
        - schema (default): catalog.schema__<env>.model
        - table:            catalog.schema.model__<env>
        - catalog:          catalog__<env>.schema.model

        For prod, or when environment_catalog_mapping overrides the env, apply those instead.
        Auto-detected from context.config — no user configuration needed.
        """
        env = effective.environment.lower()
        if env == PROD_ENVIRONMENT:
            return fqn  # no suffix in prod

        parts = fqn.split(".")

        # environment_catalog_mapping takes precedence over suffix modes.
        # It maps env name regex → catalog name for that environment.
        for pattern, catalog_override in effective.env_catalog_mapping.items():
            if re.search(pattern, env):
                # The rest of the FQN was already lowercased by _normalize_name;
                # apply the same normalization to the mapped catalog so a
                # mixed-case mapping value doesn't break sibling stitching when
                # convert_urns_to_lowercase is on.
                if effective.convert_urns_to_lowercase:
                    catalog_override = catalog_override.lower()
                # Replace the catalog component with the mapped catalog
                if len(parts) >= 3:
                    parts[0] = catalog_override
                elif len(parts) == 2:
                    parts = [catalog_override] + parts
                return ".".join(parts)

        # No catalog mapping matched — apply suffix based on mode.
        suffix = f"__{env}"
        mode = effective.env_suffix_target  # "schema", "table", or "catalog"

        if mode == ENV_SUFFIX_TARGET_CATALOG:
            if parts:
                parts[0] = f"{parts[0]}{suffix}"
        elif mode == ENV_SUFFIX_TARGET_TABLE:
            if parts:
                parts[-1] = f"{parts[-1]}{suffix}"
        else:  # "schema" (default)
            if len(parts) >= 2:
                parts[-2] = f"{parts[-2]}{suffix}"
            elif len(parts) == 1:
                parts[0] = f"{parts[0]}{suffix}"

        return ".".join(parts)

    def _make_warehouse_urn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        # Apply the environment suffix for non-prod before any other transforms.
        name = self._apply_env_suffix(fqn, effective)

        if not self.config.include_database_name:
            # Drop the catalog prefix for platforms like Athena that omit it.
            parts = name.split(".")
            if len(parts) >= 3:
                name = ".".join(parts[1:])

        return make_dataset_urn_with_platform_instance(
            platform=effective.target_platform or UNKNOWN_PLATFORM,
            name=name,
            platform_instance=effective.target_platform_instance,
            env=self.config.env,
        )

    def _get_kind_name(self, model: "SqlmeshModel") -> Optional[str]:
        kind = getattr(model, "kind", None)
        if kind is None:
            return None
        kind_name = getattr(kind, "model_kind_name", None)
        return str(kind_name) if kind_name is not None else None

    def _lookup_snapshot(
        self, model: "SqlmeshModel", sqlmesh_ctx: "SqlmeshContextType"
    ) -> Optional["Snapshot"]:
        # SQLMesh state keys snapshots by fqn, but older/renamed models are
        # sometimes only reachable by name — try both. Raises on a state-read
        # failure so each caller picks its own reporting level (hard warning vs
        # soft debug fallback).
        snapshots = sqlmesh_ctx.snapshots
        return snapshots.get(str(getattr(model, "fqn", "") or "")) or snapshots.get(
            str(getattr(model, "name", ""))
        )

    def _snapshot_updated_ts(
        self, model: "SqlmeshModel", sqlmesh_ctx: "SqlmeshContextType"
    ) -> int:
        # updated_ts (epoch millis) from the model's snapshot, or 0 when unknown.
        # Raises on a state-read failure — callers decide how loudly to report.
        snapshot = self._lookup_snapshot(model, sqlmesh_ctx)
        return int(getattr(snapshot, "updated_ts", 0)) if snapshot is not None else 0

    def _is_filtered_by_kind(self, model: "SqlmeshModel") -> bool:
        # True when model_kind_filter is set and this model's kind is excluded,
        # so no sqlmesh entity is emitted for it. A model with no resolvable kind
        # is never filtered (matches the ingestion-side filter in _ingest_project).
        if not self.config.model_kind_filter:
            return False
        kind_name = self._get_kind_name(model)
        return bool(kind_name) and kind_name not in self.config.model_kind_filter

    def _is_embedded(self, model: "SqlmeshModel") -> bool:
        kind = getattr(model, "kind", None)
        return bool(getattr(kind, "is_embedded", False)) if kind else False

    def _get_subtype(self, model: "SqlmeshModel") -> Optional[str]:
        kind_name = self._get_kind_name(model)
        return (
            MODEL_KIND_TO_SUBTYPE.get(kind_name, DEFAULT_MODEL_SUBTYPE)
            if kind_name
            else DEFAULT_MODEL_SUBTYPE
        )

    def _get_tags(self, model: "SqlmeshModel") -> List[str]:
        raw = getattr(model, "tags", None)
        raw_tags: List[str] = [t for t in (raw or []) if isinstance(t, str)]
        if not raw_tags:
            return []
        prefix = self.config.tag_prefix
        return [str(TagUrn(f"{prefix}{tag}")) for tag in raw_tags]

    def _get_owner_urn(self, model: "SqlmeshModel") -> Optional[str]:
        owner_raw = getattr(model, "owner", None)
        if not owner_raw or not isinstance(owner_raw, str):
            return None

        if self.compiled_owner_extraction_pattern:
            match = self.compiled_owner_extraction_pattern.search(owner_raw)
            if match and match.lastindex:
                owner_raw = match.group(1)
            elif match:
                owner_raw = match.group(0)

        return make_user_urn(owner_raw)
