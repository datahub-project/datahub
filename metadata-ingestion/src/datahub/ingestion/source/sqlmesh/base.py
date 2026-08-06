import logging
import re
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from datahub.emitter import mce_builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sqlmesh.compat import (
    Snapshot,
    SqlmeshContextType,
    SqlmeshModel,
)
from datahub.ingestion.source.sqlmesh.constants import (
    SQLMESH_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.models import (
    _CapabilityProbes,
    _EffectiveProjectConfig,
)
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    MODEL_KIND_TO_SUBTYPE,
    SQLMESH_TO_DATAHUB_PLATFORM,
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
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

    def _detect_target_platform(
        self, sqlmesh_ctx: "SqlmeshContextType", effective: _EffectiveProjectConfig
    ) -> str:
        """
        Resolve the warehouse platform name, auto-detecting from the gateway
        connection type if not explicitly configured.
        """
        if effective.target_platform:
            return effective.target_platform

        try:
            connection_type = sqlmesh_ctx.connection_config.type_
            platform = SQLMESH_TO_DATAHUB_PLATFORM.get(connection_type, connection_type)
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
            return "unknown"

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
            gw = "default"
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
        result[self._selected_gateway or default_effective.gateway or "default"] = (
            default_effective
        )

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
                auto_platform = SQLMESH_TO_DATAHUB_PLATFORM.get(dialect, dialect)
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
                or "unknown"
            )
            if target_platform == "unknown":
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
                    or target_platform == "snowflake"
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
            return self._resolved_effective or _EffectiveProjectConfig(
                project_path=self.config.project_path,
                gateway=self.config.gateway,
                environment=self.config.environment,
                target_platform=self.config.target_platform,
                target_platform_instance=self.config.target_platform_instance,
                sqlmesh_platform_instance=self.config.sqlmesh_platform_instance,
                default_catalog=self.config.default_catalog,
                convert_urns_to_lowercase=self.config.convert_urns_to_lowercase,
            )

        gw_name = None
        if model is not None:
            gw_name = getattr(model, "gateway", None)
        gw_key = str(gw_name).lower() if gw_name else (self._selected_gateway or "")

        return self._effective_by_gateway.get(
            gw_key,
            # Last-resort: the selected gateway's config. Better than raising
            # and stopping ingest because of one quirky model.
            self._effective_by_gateway.get(
                self._selected_gateway or "",
                self._resolved_effective
                or next(iter(self._effective_by_gateway.values())),
            ),
        )

    # -------------------------------------------------------------------------
    # Project ingestion
    # -------------------------------------------------------------------------

    def _build_physical_name_map(
        self,
        sqlmesh_ctx: "SqlmeshContextType",
        effective: _EffectiveProjectConfig,
    ) -> Dict[str, str]:
        """Map logical FQN → physical fingerprinted table name (for custom property only).

        Computed from model attributes (catalog, physical_schema, schema_name, view_name,
        data_hash) to avoid sqlmesh_ctx.snapshots, which triggers an internal
        ProcessPoolExecutor(mp_context=fork) that hangs on macOS when the DataHub async
        sink thread pool is already running.

        Physical table name format:
          {catalog}.{physical_schema}.{schema_name}__{view_name}__{data_hash}
        e.g. analytics.sqlmesh__myschema.myschema__orders__3732581953
        """
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

    def _snapshot_physical_name(
        self, snapshot: "Snapshot", effective: _EffectiveProjectConfig
    ) -> Optional[str]:
        """Extract physical table name from snapshot, handling SQLMesh API version differences."""
        for kwargs in [
            {"is_dev": False, "ignore_mapping": True},
            {"is_dev": False},
        ]:
            try:
                result = snapshot.table_name(**kwargs)
                return self._normalize_name(str(result), effective) if result else None
            except TypeError:
                continue
            except Exception as e:
                logger.debug(
                    "snapshot.table_name(%s) raised unexpected error: %s", kwargs, e
                )
                break

        try:
            fallback: Any = snapshot.table_name
            if callable(fallback):
                fallback = fallback()
            return self._normalize_name(str(fallback), effective) if fallback else None
        except Exception as e:
            logger.debug("Fallback physical name access failed: %s", e)
            return None

    # -------------------------------------------------------------------------
    # Name and URN helpers
    # -------------------------------------------------------------------------

    def _normalize_name(self, name: str, effective: _EffectiveProjectConfig) -> str:
        """Strip SQL quoting, return dot-separated name, optionally lowercased."""
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
        """Normalize + catalog-qualify a model name."""
        return self._qualify_fqn(self._normalize_name(raw_name, effective), effective)

    def _make_sqlmesh_urn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """URN for the SQLMesh entity (urn:li:dataPlatform:sqlmesh,...)."""
        return mce_builder.make_dataset_urn_with_platform_instance(
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
        if env == "prod":
            return fqn  # no suffix in prod

        parts = fqn.split(".")

        # environment_catalog_mapping takes precedence over suffix modes.
        # It maps env name regex → catalog name for that environment.
        for pattern, catalog_override in effective.env_catalog_mapping.items():
            if re.search(pattern, env):
                # Replace the catalog component with the mapped catalog
                if len(parts) >= 3:
                    parts[0] = catalog_override
                elif len(parts) == 2:
                    parts = [catalog_override] + parts
                return ".".join(parts)

        # No catalog mapping matched — apply suffix based on mode.
        suffix = f"__{env}"
        mode = effective.env_suffix_target  # "schema", "table", or "catalog"

        if mode == "catalog":
            if parts:
                parts[0] = f"{parts[0]}{suffix}"
        elif mode == "table":
            if parts:
                parts[-1] = f"{parts[-1]}{suffix}"
        else:  # "schema" (default)
            if len(parts) >= 2:
                parts[-2] = f"{parts[-2]}{suffix}"
            elif len(parts) == 1:
                parts[0] = f"{parts[0]}{suffix}"

        return ".".join(parts)

    def _make_warehouse_urn(self, fqn: str, effective: _EffectiveProjectConfig) -> str:
        """URN for the warehouse view sibling (urn:li:dataPlatform:<target_platform>,...)."""
        # Apply environment suffix for non-prod environments before any other transforms.
        name = self._apply_env_suffix(fqn, effective)

        if not self.config.include_database_name:
            # Drop the catalog prefix for platforms like Athena that omit it.
            parts = name.split(".")
            if len(parts) >= 3:
                name = ".".join(parts[1:])

        return mce_builder.make_dataset_urn_with_platform_instance(
            platform=effective.target_platform or "unknown",
            name=name,
            platform_instance=effective.target_platform_instance,
            env=self.config.env,
        )

    # -------------------------------------------------------------------------
    # Per-model workunit emission
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Model kind helpers
    # -------------------------------------------------------------------------

    def _get_kind_name(self, model: "SqlmeshModel") -> Optional[str]:
        kind = getattr(model, "kind", None)
        if kind is None:
            return None
        kind_name = getattr(kind, "model_kind_name", None)
        return str(kind_name) if kind_name is not None else None

    def _is_embedded(self, model: "SqlmeshModel") -> bool:
        kind = getattr(model, "kind", None)
        return bool(getattr(kind, "is_embedded", False)) if kind else False

    def _get_subtype(self, model: "SqlmeshModel") -> Optional[str]:
        kind_name = self._get_kind_name(model)
        return MODEL_KIND_TO_SUBTYPE.get(kind_name, "Model") if kind_name else "Model"

    def _get_tags(self, model: "SqlmeshModel") -> List[str]:
        """Build DataHub tag URNs from model.tags with the configured prefix."""
        raw = getattr(model, "tags", None)
        raw_tags: List[str] = [t for t in (raw or []) if isinstance(t, str)]
        if not raw_tags:
            return []
        prefix = self.config.tag_prefix
        return [str(TagUrn(f"{prefix}{tag}")) for tag in raw_tags]

    def _get_owner_urn(self, model: "SqlmeshModel") -> Optional[str]:
        """Extract owner URN from model.owner, applying extraction pattern if set."""
        owner_raw = getattr(model, "owner", None)
        if not owner_raw or not isinstance(owner_raw, str):
            return None

        if self.compiled_owner_extraction_pattern:
            match = self.compiled_owner_extraction_pattern.search(owner_raw)
            if match and match.lastindex:
                owner_raw = match.group(1)
            elif match:
                owner_raw = match.group(0)

        return mce_builder.make_user_urn(owner_raw)

    # -------------------------------------------------------------------------
    # Per-model workunit emission
    # -------------------------------------------------------------------------
