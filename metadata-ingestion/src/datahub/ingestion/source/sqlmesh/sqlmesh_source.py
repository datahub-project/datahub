import contextlib
import logging
import re
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
)

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    SchemaKey,
    add_dataset_to_container,
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
from datahub.ingestion.source.sqlmesh.assertions import AssertionMixin
from datahub.ingestion.source.sqlmesh.compat import (
    SqlmeshContext,
    SqlmeshContextType,
    SqlmeshModel,
    _install_enterprise_config_compat_patches,
    _install_process_pool_patch,
    _install_tobiko_local_state_fallback_shim,
    _scoped_tobiko_cloud_env,
    _sqlmesh_context_load_lock,
)
from datahub.ingestion.source.sqlmesh.constants import (
    DATASET_NAME_DELIMITER,
    DEFAULT_GATEWAY,
    ENV_SUFFIX_TARGET_SCHEMA,
    MODEL_KIND_EXTERNAL,
    PROP_FINGERPRINT_STALE,
    SNOWFLAKE_PLATFORM,
    SQLMESH_DISPLAY_NAME,
    SQLMESH_LOGO_URL,
    SQLMESH_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.containers import ContainerMixin
from datahub.ingestion.source.sqlmesh.lineage import LineageMixin
from datahub.ingestion.source.sqlmesh.models import (
    _CapabilityProbes,
    _EffectiveProjectConfig,
    _probe_capabilities,
)
from datahub.ingestion.source.sqlmesh.profiling import ProfilingMixin
from datahub.ingestion.source.sqlmesh.project_location import resolve_project_location
from datahub.ingestion.source.sqlmesh.schema import SchemaMixin
from datahub.ingestion.source.sqlmesh.siblings import SiblingsMixin
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
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    PlatformTypeClass,
    StatusClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import Dataset

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
    ContainerMixin,
    SchemaMixin,
    SiblingsMixin,
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

    Sibling stitching hinges on the sqlmesh and warehouse URNs matching exactly,
    which is why ``target_platform`` / ``target_platform_instance`` /
    ``default_catalog`` exist — see their config field docs and the connector
    guide for setup.
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
        # (audit run events) are gated on it so a failed setup doesn't write
        # partial, unlinkable metadata.
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
        # Audit run events depend on state populated during a successful project
        # load (_resolved_effective, the URN cache). If ingestion aborted on a
        # config/IO failure (e.g. an unreadable tobiko_cloud_token_file), skip
        # them so a failed run stays atomic rather than writing partial,
        # unlinkable metadata.
        if not self._ingest_succeeded:
            return
        if self.config.audit_results_path:
            yield from self._emit_audit_run_events(self.config.audit_results_path)

    def _emit_platform_registration(self) -> Iterable[MetadataWorkUnit]:
        """Register the sqlmesh platform in DataHub so entities render with correct branding."""
        platform_urn = make_data_platform_urn(SQLMESH_PLATFORM)
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

        # A git checkout / S3 download is materialised into a temp dir that must
        # survive the whole ingestion (the SQLMesh Context reads project files
        # lazily, not just during __init__), so this stack stays open across
        # every yield in _ingest_resolved_project below.
        with contextlib.ExitStack() as location_stack:
            try:
                local_project_path = resolve_project_location(
                    self.config, self.report, location_stack
                )
            except Exception as e:
                self.report.failure(
                    title="Failed to resolve SQLMesh project location",
                    message="Could not fetch the SQLMesh project from the configured git_info / s3:// location.",
                    context=self.config.project_path,
                    exc=e,
                )
                return
            yield from self._ingest_resolved_project(local_project_path)

    def _ingest_resolved_project(
        self, local_project_path: str
    ) -> Iterable[MetadataWorkUnit]:
        effective = self._effective_from_config()
        # project_path from config may be an s3:// URI or a repo-relative path;
        # swap in the resolved local directory SQLMesh actually loads from.
        effective.project_path = local_project_path

        init_kwargs: Dict[str, Any] = {"paths": [effective.project_path]}
        if effective.gateway:
            init_kwargs["gateway"] = effective.gateway

        # Redirect SQLMesh's process-pool factory to a synchronous in-process
        # one before any Context is constructed, so model parsing can't fork a
        # worker that deadlocks against the DataHub async sink. Idempotent.
        _install_process_pool_patch()

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
                    if self._is_filtered_by_kind(model):
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

                # Project load + model emission completed; post-ingest emitters
                # (audit run events) are now safe to run.
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
            finally:
                # Always release state-sync/evaluator resources — even on the
                # failure path — so repeated Context() calls in the same process
                # (e.g. multi-project recipes) don't leak connections/file handles.
                try:
                    sqlmesh_ctx.close()
                except Exception as e:
                    logger.debug("Error closing SQLMesh context: %s", e)

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
        sqlmesh_urn = make_dataset_urn_with_platform_instance(
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

        # Combine table- and column-level lineage into a single UpstreamLineage
        # aspect to avoid emitting two competing writes for the same aspect.
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
