import hashlib
import json
import logging
import re
import time
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
)

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import (
    DATAHUB_FIELD_TYPE,
    resolve_snowflake_modified_type,
    resolve_sql_type,
)
from datahub.ingestion.source.sqlmesh.assertions import AssertionMixin
from datahub.ingestion.source.sqlmesh.compat import (
    SqlmeshContext,
    SqlmeshContextType,
    SqlmeshModel,
    _install_enterprise_config_compat_patches,
    _install_tobiko_local_state_fallback_shim,
    _scoped_tobiko_cloud_env,
    _sqlmesh_context_load_lock,
)
from datahub.ingestion.source.sqlmesh.constants import (
    DATASET_NAME_DELIMITER,
    DEFAULT_GATEWAY,
    ENV_SUFFIX_TARGET_SCHEMA,
    MODEL_KIND_EXTERNAL,
    PROP_AUDITS,
    PROP_CRON,
    PROP_ENVIRONMENT,
    PROP_FINGERPRINT_STALE,
    PROP_GATEWAY,
    PROP_GRAIN,
    PROP_MODEL_KIND,
    PROP_MODEL_NAME,
    PROP_PARTITIONED_BY,
    PROP_PHYSICAL_TABLE,
    PROP_START,
    PROP_TIME_COLUMN,
    PROP_WAREHOUSE,
    PROP_WAREHOUSE_INSTANCE,
    SNOWFLAKE_PLATFORM,
    SQLMESH_DISPLAY_NAME,
    SQLMESH_LOGO_URL,
    SQLMESH_PLATFORM,
    SUBTYPE_DATABASE,
    SUBTYPE_SCHEMA,
    UNKNOWN_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.lineage import LineageMixin
from datahub.ingestion.source.sqlmesh.models import (
    _CapabilityProbes,
    _EffectiveProjectConfig,
    _MetadataTestSpec,
    _probe_capabilities,
    parse_model_audits,
)
from datahub.ingestion.source.sqlmesh.profiling import ProfilingMixin
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    UpstreamLineageClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    NullTypeClass,
    PlatformTypeClass,
    SiblingsClass,
    StatusClass,
    TestDefinitionClass,
    TestDefinitionTypeClass,
    TestInfoClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import Dataset
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


@platform_name("SQLMesh")
@config_class(SqlmeshSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DESCRIPTIONS, "Supported when model descriptions are defined"
)
class SqlmeshSource(
    LineageMixin,
    AssertionMixin,
    ProfilingMixin,
    StatefulIngestionSourceBase,
):
    """
    Ingests metadata from SQLMesh projects into DataHub, following the same
    pattern as the dbt connector.

    Creates ``urn:li:dataPlatform:sqlmesh`` entities for each model and links
    them as siblings to the corresponding warehouse view (Snowflake, BigQuery,
    etc.). The warehouse connector handles runtime metadata (tags, query history,
    profiling, usage); SQLMesh contributes lineage, schema, and model definitions.
    DataHub's SiblingAssociationHook merges both in the UI.

    **Recommended workflow:**

    1. Run warehouse ingestion with ``schema_pattern.deny: ["^sqlmesh__.*"]`` to
       exclude SQLMesh's internal fingerprinted tables.
    2. Run this connector — creates SQLMesh entities and siblings.
    3. DataHub merges both views automatically.

    **URN stitching:** sibling URNs must match exactly. Key settings:

    - ``target_platform``: auto-detected from gateway connection; override only
      when detection is wrong (e.g. force ``postgres`` instead of ``gcp_postgres``)
    - ``target_platform_instance``: must match your warehouse connector's
      ``platform_instance`` exactly
    - ``default_catalog``: set when model names are 2-part (``schema.model``) but
      your warehouse connector emits 3-part URNs (``catalog.schema.table``)

    Example recipe (OSS SQLMesh on Snowflake, run from GitHub Actions)::

        source:
          type: sqlmesh
          config:
            project_path: .              # checked-out repo root
            gateway: snowflake_prod
            target_platform_instance: prod_snowflake  # must match Snowflake connector
            default_catalog: analytics                 # if model names are 2-part
            env: PROD
    """

    config: SqlmeshSourceConfig
    report: SqlmeshSourceReport

    def __init__(self, config: SqlmeshSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = SqlmeshSourceReport()
        self._platform_registered = False
        self.platform = SQLMESH_PLATFORM  # used by StaleEntityRemovalHandler for job ID
        # Surface config flags in report (matches Snowflake/BigQuery pattern)
        self.report.include_lineage = config.include_lineage
        self.report.include_column_lineage = config.include_column_lineage
        self.compiled_owner_extraction_pattern: Optional[re.Pattern] = None
        if config.owner_extraction_pattern:
            self.compiled_owner_extraction_pattern = re.compile(
                config.owner_extraction_pattern
            )
        # Resolved project config (with auto-detected target_platform and env
        # suffix settings from the loaded SQLMesh Context). Populated by
        # _ingest_project's per-model loop so _emit_audit_run_events can build
        # warehouse URNs identical to those used in _emit_assertions —
        # keeping assertion-definition and run-event URN hashes consistent.
        self._resolved_effective: Optional[_EffectiveProjectConfig] = None
        # Per-gateway resolved configs for multi-gateway projects. Keyed by
        # gateway name. Built after Context loads from ctx.engine_adapters.
        # Single-gateway projects end up with a one-entry dict; emitters use
        # _effective_for_model(model) to look up the right one. None until
        # _ingest_project_with_worker populates it.
        self._effective_by_gateway: Dict[str, _EffectiveProjectConfig] = {}
        self._selected_gateway: Optional[str] = None
        # Capability probes (set after Context loads). Emitters consult these
        # to choose signal sources; e.g. pipeline-freshness prefers state but
        # falls back to engine_adapter, volume prefers engine_adapter but
        # falls back to Graph profile.
        self._capabilities: _CapabilityProbes = _CapabilityProbes()
        # SQLMesh dataset URN per model key (logical FQN, model.name, model.fqn),
        # populated while emitting models. _emit_audit_run_events looks up here
        # so run events land on the URN the assertion definitions used, even for
        # models routed through a non-default gateway.
        self._sqlmesh_urn_by_model_key: Dict[str, str] = {}
        # One-shot guard so a missing sqlmesh.core.lineage import is reported
        # once for the run instead of once per model.
        self._warned_column_lineage_unavailable = False
        # Gateways we've already warned about resolving via fallback config, so
        # a multi-gateway project warns once per unknown gateway, not per model.
        self._warned_missing_gateways: set = set()
        # Set True only after a project load completes; post-ingest emitters
        # (metadata tests, audit run events) are gated on it so a failed setup
        # doesn't write partial, unlinkable metadata.
        self._ingest_succeeded = False

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SqlmeshSource":
        config = SqlmeshSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> SqlmeshSourceReport:
        return self.report

    def _report_tobiko_local_state_fallback(self, reason: str) -> None:
        self.report.warning(
            title="Tobiko Cloud state store replaced by a local stub",
            message="Everything derived from SQLMesh state (last-rebuild operation aspects, row-count profiles, stale-fingerprint detection) is unavailable for this run.",
            context=reason,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_platform_registration()
        self._ingest_succeeded = False
        yield from self._ingest_project()
        # Metadata tests and audit run events depend on state populated during a
        # successful project load (_resolved_effective, the URN cache). If
        # ingestion aborted on a config/IO failure (e.g. an unreadable
        # tobiko_cloud_token_file), skip them so a failed run stays atomic
        # rather than writing partial, unlinkable metadata.
        if not self._ingest_succeeded:
            return
        if self.config.emit_metadata_tests:
            yield from self._emit_metadata_tests()
        if self.config.audit_results_path:
            yield from self._emit_audit_run_events(self.config.audit_results_path)

    def _emit_metadata_tests(self) -> Iterable[MetadataWorkUnit]:
        """Emit governance Metadata Test entities scoped to this project's models.

        The Test entity is part of the core metadata model, so any DataHub
        instance accepts and stores these definitions; evaluating them requires
        a deployment with a Metadata Tests runner (DataHub Cloud). The test URN
        is derived from the platform/instance scope so re-ingestion is
        idempotent and two projects with distinct ``sqlmesh_platform_instance``
        values get distinct tests.
        """
        platform_urn = mce_builder.make_data_platform_urn(SQLMESH_PLATFORM)
        conditions: List[Dict[str, Any]] = [
            {
                "property": "dataPlatformInstance.platform",
                "operator": "equals",
                "value": platform_urn,
            }
        ]
        scope_key = platform_urn
        scope_label = SQLMESH_DISPLAY_NAME
        if self.config.sqlmesh_platform_instance:
            instance_urn = mce_builder.make_dataplatform_instance_urn(
                SQLMESH_PLATFORM, self.config.sqlmesh_platform_instance
            )
            conditions.append(
                {
                    "property": "dataPlatformInstance.instance",
                    "operator": "equals",
                    "value": instance_urn,
                }
            )
            scope_key = instance_urn
            scope_label = (
                f"{SQLMESH_DISPLAY_NAME} ({self.config.sqlmesh_platform_instance})"
            )

        tests = [
            _MetadataTestSpec(
                suffix="documentation",
                name=f"{scope_label}: models have documentation",
                description=(
                    "Every SQLMesh model in this project should carry a description, "
                    "either from the model definition or added in DataHub."
                ),
                rules={
                    "or": [
                        {
                            "property": "datasetProperties.description",
                            "operator": "exists",
                        },
                        {
                            "property": "editableDatasetProperties.description",
                            "operator": "exists",
                        },
                    ]
                },
            ),
            _MetadataTestSpec(
                suffix="ownership",
                name=f"{scope_label}: models have owners",
                description=(
                    "Every SQLMesh model in this project should have an owner, "
                    "either from the model's owner field or assigned in DataHub."
                ),
                rules={
                    "and": [
                        {"property": "ownership.owners.owner", "operator": "exists"}
                    ]
                },
            ),
        ]
        scope_hash = hashlib.md5(scope_key.encode("utf-8")).hexdigest()[:12]
        for test in tests:
            definition = {
                "on": {"types": ["dataset"], "conditions": {"and": conditions}},
                "rules": test.rules,
            }
            yield MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:test:sqlmesh-{scope_hash}-{test.suffix}",
                aspect=TestInfoClass(
                    name=test.name,
                    category=SQLMESH_DISPLAY_NAME,
                    description=test.description,
                    definition=TestDefinitionClass(
                        type=TestDefinitionTypeClass.JSON,
                        json=json.dumps(definition, indent=2),
                    ),
                ),
            ).as_workunit()

    def _emit_platform_registration(self) -> Iterable[MetadataWorkUnit]:
        """Register the sqlmesh platform in DataHub so entities render with correct branding."""
        platform_urn = mce_builder.make_data_platform_urn(SQLMESH_PLATFORM)
        yield MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=DataPlatformInfoClass(
                name=SQLMESH_PLATFORM,
                displayName=SQLMESH_DISPLAY_NAME,
                type=PlatformTypeClass.OTHERS,
                datasetNameDelimiter=DATASET_NAME_DELIMITER,
                # Must match the bootstrap entry in data-platforms.yaml so this
                # UPSERT doesn't wipe the logo on every ingestion run.
                logoUrl=SQLMESH_LOGO_URL,
            ),
        ).as_workunit()

    def _ingest_project(self) -> Iterable[MetadataWorkUnit]:
        if SqlmeshContext is None:
            raise ImportError(
                "sqlmesh package is required for this source. "
                "Install it with: pip install 'acryl-datahub[sqlmesh]'"
            )

        effective = _EffectiveProjectConfig(
            project_path=self.config.project_path,
            gateway=self.config.gateway,
            environment=self.config.environment,
            target_platform=self.config.target_platform,
            target_platform_instance=self.config.target_platform_instance,
            sqlmesh_platform_instance=self.config.sqlmesh_platform_instance,
            default_catalog=self.config.default_catalog,
            convert_urns_to_lowercase=self.config.convert_urns_to_lowercase,
        )

        init_kwargs: Dict[str, Any] = {"paths": [effective.project_path]}
        if effective.gateway:
            init_kwargs["gateway"] = effective.gateway

        # Apply EnterpriseConfig load-time compat patches (Snowflake application
        # Literal + loader convert_config_type isinstance short-circuit). Both
        # are gated on tobikodata being installed, so OSS-only projects are
        # untouched. Idempotent.
        _install_enterprise_config_compat_patches()

        try:
            tobiko_token = self.config.resolve_tobiko_cloud_token()
        except Exception as e:
            # A bad tobiko_cloud_token_file path raises here; surface it as the
            # same clean report.failure every other config/IO problem in this
            # method produces, rather than a raw unhandled traceback.
            self.report.failure(
                title="Could not resolve Tobiko Cloud token",
                message="Failed to read the configured Tobiko Cloud token; ingestion aborted. Check tobiko_cloud_token_file path and permissions.",
                context=self.config.project_path,
                exc=e,
            )
            return
        if tobiko_token is None:
            # No creds configured: let RemoteCloudSchedulerConfig fall back to
            # a local DuckDB stub on the specific "Cloud scheduler requires a
            # cloud state connection" ConfigError so Context init succeeds
            # against an EnterpriseConfig project. Pure no-op when the project
            # doesn't use Tobiko Cloud.
            _install_tobiko_local_state_fallback_shim(
                on_fallback=self._report_tobiko_local_state_fallback
            )

        # tobikodata reads the cloud token lazily on first state access, not
        # during Context.__init__. The env-var scope must cover the entire
        # ingestion — from Context init through the capability probe and all
        # subsequent state reads.
        with _scoped_tobiko_cloud_env(
            token=tobiko_token,
            gateway=effective.gateway,
            url=self.config.tobiko_cloud_url,
        ):
            try:
                logger.info(
                    "Acquiring SQLMesh context load lock for project: %s",
                    effective.project_path,
                )
                with self.report.context_load_sec, _sqlmesh_context_load_lock:
                    sqlmesh_ctx = SqlmeshContext(**init_kwargs)
                logger.info(
                    "SQLMesh context loaded and lock released for project: %s",
                    effective.project_path,
                )
            except Exception as e:
                self.report.failure(
                    title="Failed to load SQLMesh project",
                    message="Could not initialize SQLMesh context.",
                    context=effective.project_path,
                    exc=e,
                )
                return

            try:
                # Probe capabilities once. The result drives which optional
                # signals (operation aspects, row-count profiles) can be emitted.
                self._capabilities = _probe_capabilities(
                    sqlmesh_ctx, self.ctx.graph, self.report
                )
                self.report.has_state_store_access = self._capabilities.has_state
                self.report.has_warehouse_query_access = (
                    self._capabilities.has_warehouse_query
                )
                self.report.has_graph_access = self._capabilities.has_graph
                logger.info(
                    "SQLMesh capability probes: state=%s warehouse=%s graph=%s",
                    self._capabilities.has_state,
                    self._capabilities.has_warehouse_query,
                    self._capabilities.has_graph,
                )

                # Resolve target_platform (auto-detect if not configured) — for the
                # default gateway. Multi-gateway projects get one effective per
                # gateway built immediately below.
                target_platform = self._detect_target_platform(sqlmesh_ctx, effective)

                # Read environment suffix config directly from the loaded Context — no user config needed.
                env_suffix_target = ENV_SUFFIX_TARGET_SCHEMA
                env_catalog_mapping: Dict[str, str] = {}
                try:
                    env_suffix_target = (
                        str(sqlmesh_ctx.config.environment_suffix_target)
                        .split(".")[-1]
                        .lower()
                    )  # e.g. "EnvironmentSuffixTarget.SCHEMA" → "schema"
                    env_catalog_mapping = dict(
                        getattr(sqlmesh_ctx.config, "environment_catalog_mapping", {})
                        or {}
                    )
                except Exception as e:
                    logger.debug(
                        "Could not read environment suffix config from context: %s", e
                    )

                effective = _EffectiveProjectConfig(
                    project_path=effective.project_path,
                    gateway=effective.gateway,
                    environment=effective.environment,
                    target_platform=target_platform,
                    target_platform_instance=effective.target_platform_instance,
                    sqlmesh_platform_instance=effective.sqlmesh_platform_instance,
                    default_catalog=effective.default_catalog,
                    convert_urns_to_lowercase=effective.convert_urns_to_lowercase
                    or target_platform == SNOWFLAKE_PLATFORM,
                    env_suffix_target=env_suffix_target,
                    env_catalog_mapping=env_catalog_mapping,
                )
                # Cache for _emit_audit_run_events so it can build warehouse URNs
                # the same way _emit_assertions does (consistent assertion hash).
                self._resolved_effective = effective

                # Build per-gateway effectives. For single-gateway projects this
                # produces a one-entry dict equivalent to `effective`; multi-gateway
                # projects get one entry per gateway with platform / instance /
                # catalog auto-detected per gateway and user overrides applied.
                self._selected_gateway = self._read_selected_gateway(
                    sqlmesh_ctx, effective
                )
                self._effective_by_gateway = self._build_per_gateway_effectives(
                    sqlmesh_ctx, effective
                )
                if len(self._effective_by_gateway) > 1:
                    logger.info(
                        "Multi-gateway project: %d gateways (%s)",
                        len(self._effective_by_gateway),
                        ", ".join(sorted(self._effective_by_gateway)),
                    )

                logger.info(
                    "Ingesting SQLMesh project %r (gateway=%r, env=%r, warehouse=%r)",
                    effective.project_path,
                    effective.gateway,
                    effective.environment,
                    target_platform,
                )

                physical_name_by_model: Dict[str, str] = self._build_physical_name_map(
                    sqlmesh_ctx, effective
                )

                # Build the full FQN list first (needed for containers, preview, and changed-mode).
                # For multi-gateway projects each model uses its own gateway's
                # default_catalog when qualifying its name; single-gateway projects
                # see no difference because every lookup returns the same effective.
                all_fqns: Dict[str, "SqlmeshModel"] = {}  # fqn → model
                for model_name_key, model in sqlmesh_ctx.models.items():
                    model_effective = self._effective_for_model(model)
                    fqn = self._build_logical_fqn(str(model_name_key), model_effective)
                    if not self.config.model_name_pattern.allowed(fqn):
                        continue
                    if self.config.model_kind_filter:
                        kind_name = self._get_kind_name(model)
                        if kind_name and kind_name not in self.config.model_kind_filter:
                            continue
                    all_fqns[fqn] = model

                # Print URN pairs before emitting anything so a --dry-run can
                # validate sibling stitching without writing metadata.
                if self.config.preview_urns:
                    self._log_urn_preview(all_fqns, effective)

                # Emit containers before models so browsing works on the first run.
                with self.report.container_emission_sec:
                    yield from self._emit_containers(set(all_fqns.keys()), effective)

                for fqn, model in all_fqns.items():
                    self.report.models_scanned += 1
                    try:
                        yield from self._emit_model(
                            model, fqn, physical_name_by_model, effective, sqlmesh_ctx
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to process model %s: %s", fqn, e, exc_info=True
                        )
                        self.report.report_model_failed(fqn, str(e))

                # Release state-sync and evaluator resources so that repeated Context()
                # calls in the same process (e.g. multi-project recipes) don't accumulate
                # open connections or file handles.
                sqlmesh_ctx.close()

                # Project load + model emission completed; post-ingest emitters
                # (metadata tests, audit run events) are now safe to run.
                self._ingest_succeeded = True
            except Exception as e:
                # Any failure in post-load setup (capability probe, per-gateway
                # effectives, physical-name map, container emission) should surface
                # as a report failure, not crash ingestion with a raw traceback.
                self.report.failure(
                    title="Failed during SQLMesh ingestion setup",
                    message="Error after loading the SQLMesh context; ingestion aborted.",
                    context=effective.project_path,
                    exc=e,
                )
                return

    def _emit_containers(
        self, fqns: Set[str], effective: _EffectiveProjectConfig
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Database and Schema container entities for the sqlmesh platform."""
        seen_databases: Set[str] = set()
        seen_schemas: Set[str] = set()

        for fqn in sorted(fqns):
            parts = fqn.split(".")
            if len(parts) >= 3:
                catalog, schema = parts[0], parts[1]
            elif len(parts) == 2:
                catalog, schema = None, parts[0]
            else:
                continue  # 1-part name — no containers

            if catalog and catalog not in seen_databases:
                seen_databases.add(catalog)
                db_key = DatabaseKey(
                    platform=SQLMESH_PLATFORM,
                    instance=effective.sqlmesh_platform_instance,
                    env=self.config.env,
                    database=catalog,
                )
                yield from gen_containers(
                    container_key=db_key,
                    name=catalog,
                    sub_types=[SUBTYPE_DATABASE],
                )
                self.report.num_containers_emitted += 1

            schema_key_str = f"{catalog}.{schema}" if catalog else schema
            if schema_key_str not in seen_schemas:
                self.report.num_containers_emitted += 1
                seen_schemas.add(schema_key_str)
                if catalog:
                    db_key = DatabaseKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database=catalog,
                    )
                    schema_key = SchemaKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database=catalog,
                        schema=schema,
                    )
                    yield from gen_containers(
                        container_key=schema_key,
                        name=schema,
                        sub_types=[SUBTYPE_SCHEMA],
                        parent_container_key=db_key,
                    )
                else:
                    schema_key = SchemaKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database="",
                        schema=schema,
                    )
                    yield from gen_containers(
                        container_key=schema_key,
                        name=schema,
                        sub_types=[SUBTYPE_SCHEMA],
                    )

    def _log_urn_preview(
        self, all_fqns: Dict[str, "SqlmeshModel"], effective: _EffectiveProjectConfig
    ) -> None:
        """
        Log a sample of sqlmesh ↔ warehouse URN pairs before emitting.
        Helps users validate that sibling URNs will match their warehouse connector.

        For multi-gateway projects each entry shows the gateway in brackets so
        users can spot routing problems (e.g. a model on the wrong gateway).
        """
        sample = list(all_fqns.items())[: self.config.preview_urns_sample_size]
        lines = ["URN preview (sqlmesh → warehouse sibling):"]
        for fqn, model in sample:
            model_effective = self._effective_for_model(model)
            sqlmesh_urn = self._make_sqlmesh_urn(fqn, model_effective)
            warehouse_urn = self._make_warehouse_urn(fqn, model_effective)
            # Always show the gateway label so multi-gateway routing is
            # diagnosable. The default-gateway effective has gateway=None
            # because the top-level config field can be unset; fall back to
            # the SQLMesh-resolved selected_gateway in that case.
            gw_name = (
                getattr(model, "gateway", None)
                or model_effective.gateway
                or self._selected_gateway
                or DEFAULT_GATEWAY
            )
            lines.append(f"  sqlmesh : {sqlmesh_urn} [{gw_name}]")
            lines.append(f"  warehouse: {warehouse_urn}")
            lines.append("")
        logger.info("\n".join(lines))

    def _emit_model(
        self,
        model: "SqlmeshModel",
        fqn: str,
        physical_name_by_model: Dict[str, str],
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> Iterable[MetadataWorkUnit]:
        # For multi-gateway projects, the model's own gateway dictates the
        # warehouse platform / instance / catalog. For single-gateway projects
        # this returns the same value as the `effective` parameter.
        effective = self._effective_for_model(model)
        physical_name = physical_name_by_model.get(fqn) or physical_name_by_model.get(
            self._build_logical_fqn(str(getattr(model, "name", fqn)), effective)
        )

        is_embedded = self._is_embedded(model)
        if is_embedded:
            self.report.num_embedded_models += 1

        kind_name = self._get_kind_name(model)
        if kind_name == MODEL_KIND_EXTERNAL:
            self.report.num_external_models += 1

        custom_props: Optional[Dict[str, str]] = None
        if self.config.include_model_properties:
            custom_props = self._build_custom_properties(
                fqn, physical_name, effective, model
            )
            if self.config.detect_stale_fingerprints and self._is_fingerprint_stale(
                model, sqlmesh_ctx
            ):
                custom_props[PROP_FINGERPRINT_STALE] = "true"

        with self.report.schema_extraction_sec:
            schema_fields = (
                self._build_schema_fields(model, effective)
                if self.config.include_schema
                else None
            )

        tags = self._get_tags(model)
        owner_urn = self._get_owner_urn(model)

        # Compute the sqlmesh URN up front so _build_column_lineage can use it
        # for field URN construction before the Dataset object is created.
        sqlmesh_urn = mce_builder.make_dataset_urn_with_platform_instance(
            platform=SQLMESH_PLATFORM,
            name=fqn,
            platform_instance=effective.sqlmesh_platform_instance,
            env=self.config.env,
        )

        # Remember the URN under every name an audit-results file might use, so
        # _emit_audit_run_events links run events to the same URN the assertion
        # definitions were emitted against — including for models on a
        # non-default gateway, whose FQN can't be rebuilt from the default
        # gateway's effective config.
        for key in (
            fqn,
            str(getattr(model, "name", "") or ""),
            str(getattr(model, "fqn", "") or ""),
        ):
            if key:
                self._sqlmesh_urn_by_model_key[key] = sqlmesh_urn

        # Build table-level and column-level lineage, then combine into a single
        # UpstreamLineage aspect. This avoids emitting duplicate aspect writes.
        combined_upstreams: Optional[UpstreamLineageClass] = None
        if self.config.include_lineage:
            with self.report.lineage_extraction_sec:
                table_lineage = self._build_upstreams(model, effective, sqlmesh_ctx)
            with self.report.column_lineage_sec:
                fine_grained = (
                    self._build_column_lineage(
                        model, sqlmesh_urn, effective, sqlmesh_ctx
                    )
                    if self.config.include_column_lineage
                    else []
                )
            if fine_grained:
                self.report.num_models_with_column_lineage += 1
                self.report.num_columns_with_lineage += len(fine_grained)
            if table_lineage or fine_grained:
                combined_upstreams = UpstreamLineageClass(
                    upstreams=table_lineage.upstreams if table_lineage else [],
                    fineGrainedLineages=fine_grained if fine_grained else None,
                )

        # Emit status FIRST so the MAE consumer can always hydrate the entity,
        # even if it processes this MCL before other aspects are committed.
        # dbt uses the same pattern (StatusClass appended before MCE bundling).
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Emit the SQLMesh entity on the sqlmesh platform
        dataset = Dataset(
            platform=SQLMESH_PLATFORM,
            name=fqn,
            platform_instance=effective.sqlmesh_platform_instance,
            env=self.config.env,
            description=getattr(model, "description", None) or None,
            custom_properties=custom_props,
            schema=schema_fields,
            upstreams=combined_upstreams,
            subtype=self._get_subtype(model),
            tags=tags if tags else None,
            owners=[CorpUserUrn.from_string(owner_urn)] if owner_urn else None,
        )
        yield from dataset.as_workunits()

        # Link dataset to its schema container.
        parts = fqn.split(".")
        if len(parts) >= 3:
            catalog, schema = parts[0], parts[1]
            schema_key: Optional[SchemaKey] = SchemaKey(
                platform=SQLMESH_PLATFORM,
                instance=effective.sqlmesh_platform_instance,
                env=self.config.env,
                database=catalog,
                schema=schema,
            )
        elif len(parts) == 2:
            schema_key = SchemaKey(
                platform=SQLMESH_PLATFORM,
                instance=effective.sqlmesh_platform_instance,
                env=self.config.env,
                database="",
                schema=parts[0],
            )
        else:
            schema_key = None

        if schema_key is not None:
            yield from add_dataset_to_container(schema_key, str(dataset.urn))

        # EMBEDDED models have no warehouse object — skip sibling.
        # All other kinds (including EXTERNAL) have a warehouse view to link to.
        if not is_embedded:
            warehouse_urn = self._make_warehouse_urn(fqn, effective)
            yield from self._emit_siblings(sqlmesh_urn, warehouse_urn)

        # Audits are properties of the SQLMesh model definition, not of any
        # particular materialized output. In SQLMesh the "physical counterpart"
        # is a virtual view pointing at a fingerprint table that rotates as
        # the model evolves — there is no stable physical target equivalent to
        # dbt's model→table mapping. The SQLMesh URN is the only stable,
        # semantically meaningful target for the audit; siblings let users
        # navigate from the logical model to its current materialization.
        yield from self._emit_assertions(model, sqlmesh_urn)

        # Freshness and volume monitoring is left to DataHub monitors the user
        # creates against these two timeseries aspects. We don't synthesise
        # FRESHNESS / VOLUME assertion definitions: nothing in the connector
        # (or in Cloud, without an explicit monitor) would ever evaluate them.
        yield from self._emit_pipeline_operation(
            sqlmesh_urn=sqlmesh_urn, model=model, sqlmesh_ctx=sqlmesh_ctx
        )
        yield from self._emit_row_count_profile(
            model=model,
            sqlmesh_urn=sqlmesh_urn,
            physical_name=physical_name,
            sqlmesh_ctx=sqlmesh_ctx,
        )

    def _is_fingerprint_stale(
        self,
        model: "SqlmeshModel",
        sqlmesh_ctx: Optional["SqlmeshContextType"],
    ) -> bool:
        """Whether the model's fingerprint table is older than the staleness threshold.

        Uses the same SQLMesh state lookup as ``_emit_pipeline_operation``
        (``ctx.snapshots[...].updated_ts``, epoch millis). Returns False when
        state is unreachable or the snapshot carries no timestamp — unknown is
        deliberately not reported as stale.
        """
        if not self._capabilities.has_state or sqlmesh_ctx is None:
            return False
        try:
            snapshots = sqlmesh_ctx.snapshots
            snapshot = snapshots.get(
                str(getattr(model, "fqn", "") or "")
            ) or snapshots.get(str(getattr(model, "name", "")))
            updated_ts = (
                int(getattr(snapshot, "updated_ts", 0)) if snapshot is not None else 0
            )
        except Exception as e:
            # Returning "not stale" on a read failure is a false negative for a
            # staleness monitor — the worst failure mode for this feature — so
            # surface it on the report instead of a silent debug log.
            self.report.warning(
                title="Stale-fingerprint check failed for a model",
                message="Could not read snapshot.updated_ts from SQLMesh state; the model is NOT flagged stale even though staleness could not be determined.",
                context=str(getattr(model, "name", "?")),
                exc=e,
            )
            return False
        if updated_ts <= 0:
            return False
        threshold_ms = self.config.fingerprint_staleness_threshold_hours * 3_600_000
        return (int(time.time() * 1000) - updated_ts) > threshold_ms

    def _build_custom_properties(
        self,
        fqn: str,
        physical_name: Optional[str],
        effective: _EffectiveProjectConfig,
        model: "SqlmeshModel",
    ) -> Dict[str, str]:
        props: Dict[str, str] = {
            PROP_MODEL_NAME: fqn,
            PROP_ENVIRONMENT: effective.environment,
            PROP_WAREHOUSE: effective.target_platform or UNKNOWN_PLATFORM,
        }
        if effective.gateway:
            props[PROP_GATEWAY] = effective.gateway
        if physical_name:
            props[PROP_PHYSICAL_TABLE] = physical_name
        if effective.target_platform_instance:
            props[PROP_WAREHOUSE_INSTANCE] = effective.target_platform_instance
        kind = getattr(model, "kind", None)
        if kind is not None:
            props[PROP_MODEL_KIND] = str(kind)

        cron = getattr(model, "cron", None)
        if cron:
            props[PROP_CRON] = str(cron)

        start = getattr(model, "start", None)
        if start:
            props[PROP_START] = str(start)

        time_column = getattr(model, "time_column", None)
        if time_column is not None:
            try:
                props[PROP_TIME_COLUMN] = str(time_column.column)
            except Exception:
                props[PROP_TIME_COLUMN] = str(time_column)

        model_name = str(getattr(model, "name", "?"))
        partitioned_by = getattr(model, "partitioned_by", None)
        if partitioned_by:
            try:
                cols = [str(c.name) for c in partitioned_by if hasattr(c, "name")]
                if cols:
                    props[PROP_PARTITIONED_BY] = ",".join(cols)
            except Exception:
                # Best-effort enrichment: an unexpected partitioned_by shape
                # just omits the property, but log it so a new SQLMesh version
                # dropping this isn't invisible.
                logger.debug(
                    "Could not extract partitioned_by for %s",
                    model_name,
                    exc_info=True,
                )

        grains = getattr(model, "grains", None)
        if grains:
            try:
                grain_cols = [str(g.name) for g in grains if hasattr(g, "name")]
                if grain_cols:
                    props[PROP_GRAIN] = ",".join(grain_cols)
            except Exception:
                logger.debug(
                    "Could not extract grains for %s", model_name, exc_info=True
                )

        audit_names = [audit.name for audit in parse_model_audits(model)]
        if audit_names:
            props[PROP_AUDITS] = ",".join(audit_names)

        return props

    def _resolve_column_type(
        self, type_str: str, platform: str
    ) -> Optional[DATAHUB_FIELD_TYPE]:
        """Resolve a column type, preferring the target platform's own mapping.

        ``resolve_sql_type`` consults a merged cross-platform mapping where the
        last-registered platform wins on conflicts — so ``TIMESTAMP`` resolves
        to SQL Server's ``BytesType`` rather than Snowflake's ``TimeType``.
        These columns describe the resolved warehouse, and the SQLMesh dataset
        is a sibling of that warehouse entity, so a mismatched type renders
        confusingly across the pair. Consult the platform-specific resolver
        first for platforms where the merged mapping is known to conflict.
        """
        if type_str and platform.lower() == SNOWFLAKE_PLATFORM:
            snowflake_type = resolve_snowflake_modified_type(type_str.upper())
            if snowflake_type is not None:
                return snowflake_type()
        return resolve_sql_type(type_str, platform.lower())

    def _build_schema_fields(
        self, model: "SqlmeshModel", effective: _EffectiveProjectConfig
    ) -> Optional[List[SchemaField]]:
        columns_to_types: Dict[str, Any] = (
            getattr(model, "columns_to_types", None) or {}
        )
        if not columns_to_types:
            logger.debug(
                "Model %s has no column type information; skipping schema",
                getattr(model, "name", "?"),
            )
            return None

        col_descriptions: Dict[str, str] = (
            getattr(model, "column_descriptions", None) or {}
        )

        fields = []
        for col_name, col_type in columns_to_types.items():
            type_str = str(col_type) if col_type is not None else ""
            resolved = self._resolve_column_type(
                type_str, effective.target_platform or ""
            )
            fields.append(
                SchemaField(
                    fieldPath=col_name,
                    type=SchemaFieldDataType(type=resolved or NullTypeClass()),
                    nativeDataType=type_str,
                    nullable=True,
                    description=col_descriptions.get(col_name) or None,
                )
            )
        return fields or None

    def _emit_siblings(
        self, sqlmesh_urn: str, warehouse_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Link the SQLMesh entity and its warehouse counterpart as siblings.

        SQLMesh is primary by default (it owns the model definition, lineage and
        descriptions), matching dbt's ``dbt_is_primary_sibling=True``.

        The SQLMesh entity's aspect is written outright — this connector owns
        that entity. The warehouse entity is *patched* instead, so a sibling
        edge added by another connector (dbt, or a second SQLMesh project) isn't
        clobbered, and the workunit is marked non-authoritative because we are
        not the source of truth for warehouse metadata. Same split as dbt.
        """
        sqlmesh_is_primary = self.config.sqlmesh_is_primary_sibling

        # TODO: migrate to SDK V2 when SiblingsClass is supported
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn,
            aspect=SiblingsClass(siblings=[warehouse_urn], primary=sqlmesh_is_primary),
        ).as_workunit()

        warehouse_patch = DatasetPatchBuilder(warehouse_urn)
        warehouse_patch.add_sibling(sqlmesh_urn, primary=not sqlmesh_is_primary)
        for mcp in warehouse_patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp),
                mcp_raw=mcp,
                is_primary_source=False,
            )
