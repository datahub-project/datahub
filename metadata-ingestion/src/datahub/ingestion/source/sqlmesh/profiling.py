import logging
import time
from typing import (
    Any,
    Iterable,
    Optional,
)

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.compat import (
    SqlmeshContextType,
    SqlmeshModel,
)
from datahub.ingestion.source.sqlmesh.constants import (
    INGEST_ACTOR,
    MODEL_KIND_EMBEDDED,
    MODEL_KIND_EXTERNAL,
    OPERATION_FINGERPRINT_REBUILD,
)
from datahub.ingestion.source.sqlmesh.models import (
    _build_count_query,
)
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    OperationClass,
    OperationTypeClass,
)

logger = logging.getLogger(__name__)


class ProfilingMixin(SqlmeshSourceBase):
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
            updated_ts = self._snapshot_updated_ts(model, sqlmesh_ctx)
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

    def _emit_pipeline_operation(
        self,
        *,
        sqlmesh_urn: str,
        model: "SqlmeshModel",
        sqlmesh_ctx: Optional["SqlmeshContextType"],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit an ``OperationAspect`` carrying the fingerprint rebuild time.

        ``OperationAspect.lastUpdatedTimestamp`` is the canonical timeseries the
        rest of DataHub reads to answer "when was this dataset last touched?".
        Every warehouse connector emits this aspect for INSERT/UPDATE events on
        the source table; we do the equivalent for SQLMesh by mapping a
        fingerprint rebuild to a single ``CUSTOM`` operation at
        ``snapshot.updated_ts``. Users see the operation history on the
        dataset's Activity tab, and can point a freshness monitor at it.

        Source of the timestamp: ``ctx.snapshots[fqn].updated_ts`` from
        SQLMesh state. Skipped when state is unreachable.
        """
        if not self._capabilities.has_state or sqlmesh_ctx is None:
            return

        try:
            updated_ts = self._snapshot_updated_ts(model, sqlmesh_ctx)
        except Exception as e:
            self.report.num_operations_skipped += 1
            self.report.warning(
                title="Could not read snapshot timestamp; skipping operation",
                message="Failed to read snapshot.updated_ts; the pipeline operation aspect (last-rebuild time) is missing for this model.",
                context=str(getattr(model, "name", "?")),
                exc=e,
            )
            return

        if updated_ts <= 0:
            return

        now_ms = int(time.time() * 1000)
        operation = OperationClass(
            timestampMillis=now_ms,
            operationType=OperationTypeClass.CUSTOM,
            customOperationType=OPERATION_FINGERPRINT_REBUILD,
            lastUpdatedTimestamp=updated_ts,
            actor=make_user_urn(INGEST_ACTOR),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn, aspect=operation
        ).as_workunit()

    def _engine_adapter_for_model(
        self, model: "SqlmeshModel", sqlmesh_ctx: "SqlmeshContextType"
    ) -> Any:
        """Return the engine adapter for the model's own gateway.

        Multi-gateway projects route different models to different warehouses,
        so a ``COUNT(*)`` must run on the adapter for the model's gateway —
        using the default ``ctx.engine_adapter`` would query the wrong
        warehouse (the same bug class already fixed for assertion URNs). Falls
        back to the default adapter when the gateway isn't in
        ``ctx.engine_adapters`` (single-gateway projects, or API drift).
        """
        default_adapter = getattr(sqlmesh_ctx, "engine_adapter", None)
        gw_name = getattr(model, "gateway", None) or self._selected_gateway
        if not gw_name:
            return default_adapter
        adapters = getattr(sqlmesh_ctx, "engine_adapters", None) or {}
        # engine_adapters is keyed by gateway name; SQLMesh normalises those to
        # lowercase, so try both the raw and lowercased forms.
        return (
            adapters.get(gw_name)
            or adapters.get(str(gw_name).lower())
            or default_adapter
        )

    def _emit_row_count_profile(
        self,
        *,
        model: "SqlmeshModel",
        sqlmesh_urn: str,
        physical_name: Optional[str] = None,
        sqlmesh_ctx: Optional["SqlmeshContextType"] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit ``DatasetProfile.rowCount`` from a ``SELECT COUNT(*)`` on the
        model's current fingerprint table.

        This is the canonical timeseries warehouse profilers populate, so a
        volume monitor created in DataHub against the SQLMesh entity has real
        history to work with. Cheap on most warehouses (Snowflake/BigQuery serve
        it from metadata; DuckDB streams the table); a full scan on
        Postgres-style engines for huge tables — accepted for now.

        External and embedded models are skipped (no materialised output of
        their own to count).
        """
        kind_name = self._get_kind_name(model) or ""
        if kind_name.upper() in (MODEL_KIND_EXTERNAL, MODEL_KIND_EMBEDDED):
            return

        if sqlmesh_ctx is None:
            return

        # Prefer the snapshot's authoritative table_name() because the
        # derived `physical_name` (built from model.data_hash) doesn't
        # always match the hash SQLMesh actually used to materialise the
        # fingerprint. ``snapshot.table_name()`` returns a SQL fragment
        # already quoted for the dialect (e.g. ``"sushi-example".schema.t``)
        # so we splice it directly. The model-attribute fallback is
        # unquoted, so it goes through SQLGlot to be safe.
        live_physical_name = None
        snapshot_provided = False
        try:
            if self._capabilities.has_state:
                snapshot = self._lookup_snapshot(model, sqlmesh_ctx)
                if snapshot is not None:
                    tn = snapshot.table_name()
                    if tn:
                        live_physical_name = str(tn)
                        snapshot_provided = True
        except Exception as e:
            logger.debug(
                "Could not read snapshot.table_name() for %s (%s); using derived name",
                getattr(model, "name", "?"),
                e,
            )

        if not live_physical_name:
            live_physical_name = physical_name
        if not live_physical_name:
            return

        # Route the COUNT(*) to the model's own gateway adapter, not the
        # default one — otherwise multi-gateway projects query the wrong
        # warehouse.
        engine_adapter = self._engine_adapter_for_model(model, sqlmesh_ctx)
        if engine_adapter is None:
            return

        # has_warehouse_query is probed only against the default gateway's
        # adapter, so a failing default probe must not suppress profiles for
        # models whose own gateway adapter is healthy. Only honour the probe as
        # a quiet skip when this model resolves to that same default adapter;
        # for a distinct gateway adapter, attempt the query and let the
        # try/except below surface any genuine per-gateway failure.
        default_adapter = getattr(sqlmesh_ctx, "engine_adapter", None)
        if not self._capabilities.has_warehouse_query and (
            engine_adapter is default_adapter
        ):
            return

        try:
            if snapshot_provided:
                # SQLMesh's table_name() is already dialect-quoted; splice directly.
                query = f"SELECT COUNT(*) FROM {live_physical_name}"
            else:
                dialect = getattr(engine_adapter, "DIALECT", None)
                if not isinstance(dialect, str):
                    dialect = None
                query = _build_count_query(live_physical_name, dialect=dialect)
            row = engine_adapter.fetchone(query)
            # COUNT(*) must return exactly one non-null row. A missing row or
            # NULL column is an adapter anomaly, not a real zero-row table —
            # emitting a fabricated 0 here would feed volume monitors a false
            # data point. Treat it as a profiling failure instead.
            if not row or row[0] is None:
                raise ValueError(
                    "COUNT(*) returned no row / a NULL value; treating as a "
                    "profiling failure rather than emitting rowCount=0"
                )
            row_count = int(row[0])
        except Exception as e:
            # Profiling was requested; a query failure (permissions, table not
            # materialized, dialect quoting) must surface, not vanish silently.
            self.report.num_profiles_failed += 1
            self.report.warning(
                title="Row-count query failed; skipping profile",
                message="Could not query the physical table row count; the row-count profile is missing for this model.",
                context=live_physical_name,
                exc=e,
            )
            return

        ts_ms = int(time.time() * 1000)
        profile = DatasetProfileClass(
            timestampMillis=ts_ms,
            rowCount=row_count,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn, aspect=profile
        ).as_workunit()
