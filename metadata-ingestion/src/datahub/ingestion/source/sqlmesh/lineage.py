import logging
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
)

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.compat import (
    SqlmeshContextType,
    SqlmeshModel,
)
from datahub.ingestion.source.sqlmesh.constants import (
    MODEL_KIND_EXTERNAL,
)
from datahub.ingestion.source.sqlmesh.models import (
    _EffectiveProjectConfig,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)

logger = logging.getLogger(__name__)


class LineageMixin(SqlmeshSourceBase):
    def _dep_effective(
        self,
        dep_name: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> _EffectiveProjectConfig:
        """Gateway-effective config that owns a dependency.

        Falls back to the caller's config for undeclared deps, where we have no
        better signal. Callers must use this — not their own effective — when
        building a dep's FQN, so the name they filter on is the same name the
        emitted URN is built from.
        """
        dep_model = sqlmesh_ctx.get_model(dep_name)
        return (
            self._effective_for_model(dep_model) if dep_model is not None else effective
        )

    def _resolve_dep_urn(
        self,
        dep_name: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
        count_undeclared: bool = True,
    ) -> str:
        """
        Map a dependency name to the correct DataHub URN using 3-category logic.

        Category 1 — managed model: sqlmesh URN
        Category 2 — declared external (EXTERNAL kind): sqlmesh URN by default,
                      warehouse URN when skip_external_models_in_lineage=True
        Category 3 — undeclared implicit (get_model returns None): warehouse URN

        For multi-gateway projects: the dep's OWN gateway determines its
        warehouse URN, not the caller's gateway. If the dep is in our model
        map we resolve via _effective_for_model(dep_model); otherwise we
        fall back to the caller's effective (the dep is undeclared and we
        have no better signal).

        ``count_undeclared`` gates the category-3 report counter. Table lineage
        visits each dep exactly once, so it counts; column lineage revisits the
        same deps once per column and must not inflate the number.
        """
        dep_model = sqlmesh_ctx.get_model(dep_name)
        dep_effective = self._dep_effective(dep_name, effective, sqlmesh_ctx)
        dep_fqn = self._build_logical_fqn(dep_name, dep_effective)

        if dep_model is None:
            logger.debug(
                "Dep %r not in SQLMesh context; routing lineage to warehouse URN",
                dep_name,
            )
            if count_undeclared:
                self.report.num_undeclared_upstream_refs += 1
            return self._make_warehouse_urn(dep_fqn, dep_effective)

        kind = getattr(dep_model, "kind", None)
        is_external = (
            str(getattr(kind, "model_kind_name", "")).upper() == MODEL_KIND_EXTERNAL
        )
        if is_external and self.config.skip_external_models_in_lineage:
            return self._make_warehouse_urn(dep_fqn, dep_effective)
        if self._is_filtered_by_kind(dep_model):
            # Excluded from ingestion by model_kind_filter, so no sqlmesh entity
            # was emitted for it. Point at the warehouse table instead of a
            # sqlmesh URN that would dangle.
            return self._make_warehouse_urn(dep_fqn, dep_effective)
        return self._make_sqlmesh_urn(dep_fqn, dep_effective)

    def _build_upstreams(
        self,
        model: "SqlmeshModel",
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> Optional[UpstreamLineageClass]:
        # Each dep is routed to its URN by _resolve_dep_urn (managed vs declared
        # external vs undeclared taxonomy lives there); here we only drop deps
        # denied by model_name_pattern and build the UpstreamClass list.
        raw_deps: Set[Any] = getattr(model, "depends_on", None) or set()
        if not raw_deps:
            logger.debug(
                "Model %s has no dependencies; skipping lineage",
                getattr(model, "name", "?"),
            )
            return None

        upstreams = []
        # raw_deps is a set; sort so upstream ordering (and the emitted MCP) is
        # stable run-to-run rather than dependent on set iteration order.
        for dep in sorted(raw_deps, key=str):
            dep_name = str(dep)
            dep_effective = self._dep_effective(dep_name, effective, sqlmesh_ctx)
            dep_fqn = self._build_logical_fqn(dep_name, dep_effective)
            if not self.config.model_name_pattern.allowed(dep_fqn):
                continue
            upstreams.append(
                UpstreamClass(
                    dataset=self._resolve_dep_urn(dep_name, effective, sqlmesh_ctx),
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        return UpstreamLineageClass(upstreams=upstreams) if upstreams else None

    def _build_column_lineage(
        self,
        model: "SqlmeshModel",
        model_sqlmesh_urn: str,
        effective: _EffectiveProjectConfig,
        sqlmesh_ctx: "SqlmeshContextType",
    ) -> List[FineGrainedLineageClass]:
        """
        Build column-level lineage using SQLMesh's column_dependencies API.

        SQLMesh parses all SQL through SQLGlot, so column lineage is available natively
        for SQL models without a separate parsing step. Python DataFrame models may not
        have column-level lineage available.

        The first call per model is CPU-expensive (renders Jinja + qualifies full AST)
        but SQLMesh caches the result per model object identity.
        """
        try:
            from sqlmesh.core.lineage import column_dependencies
        except ImportError as e:
            # include_column_lineage defaults on, so a project-wide loss of the
            # feature must be visible — not a per-model debug log. Warn once.
            if not self._warned_column_lineage_unavailable:
                self._warned_column_lineage_unavailable = True
                self.report.warning(
                    title="Column lineage unavailable",
                    message="sqlmesh.core.lineage could not be imported (private API moved in this sqlmesh version); column-level lineage is skipped for the entire run.",
                    exc=e,
                )
            return []

        columns_to_types: Dict[str, Any] = (
            getattr(model, "columns_to_types", None) or {}
        )
        if not columns_to_types:
            return []

        model_name = str(getattr(model, "name", ""))
        convert_lower = (
            self.config.convert_column_urns_to_lowercase
            if self.config.convert_column_urns_to_lowercase is not None
            else effective.convert_urns_to_lowercase
        )

        fine_grained: List[FineGrainedLineageClass] = []
        for col_name in columns_to_types:
            try:
                deps: Dict[str, Set[str]] = column_dependencies(
                    sqlmesh_ctx, model_name, col_name
                )
            except Exception as e:
                # Column lineage is a headline feature (include_column_lineage
                # defaults on), so a parse failure must be visible in the
                # summary — not just counted and logged at debug.
                self.report.num_column_lineage_parse_failures += 1
                self.report.column_lineage_failures.append(f"{model_name}.{col_name}")
                self.report.warning(
                    title="Column lineage extraction failed",
                    message="Could not resolve column dependencies; column lineage skipped for this column (Python model or unsupported SQL).",
                    context=f"{model_name}.{col_name}",
                    exc=e,
                )
                continue

            if not deps:
                continue

            downstream_col = col_name.lower() if convert_lower else col_name
            downstream_field_urn = make_schema_field_urn(
                model_sqlmesh_urn, downstream_col
            )

            upstream_field_urns: List[str] = []
            for upstream_model_name, upstream_cols in deps.items():
                dep_name = str(upstream_model_name)
                # Honour model_name_pattern here too; otherwise a model denied
                # at the table-lineage level reappears as a column-lineage edge.
                dep_effective = self._dep_effective(dep_name, effective, sqlmesh_ctx)
                if not self.config.model_name_pattern.allowed(
                    self._build_logical_fqn(dep_name, dep_effective)
                ):
                    continue
                upstream_dataset_urn = self._resolve_dep_urn(
                    dep_name,
                    effective,
                    sqlmesh_ctx,
                    # Table lineage already counted this dep once; counting again
                    # per column would multiply the reported number by the column
                    # count.
                    count_undeclared=False,
                )
                # upstream_cols is a set; sort for deterministic URN ordering.
                for upstream_col in sorted(upstream_cols):
                    up_col = upstream_col.lower() if convert_lower else upstream_col
                    upstream_field_urns.append(
                        make_schema_field_urn(upstream_dataset_urn, up_col)
                    )

            if upstream_field_urns:
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        # Sort so the FIELD_SET is stable regardless of the order
                        # upstream models/columns were iterated.
                        upstreams=sorted(upstream_field_urns),
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field_urn],
                    )
                )

        return fine_grained
