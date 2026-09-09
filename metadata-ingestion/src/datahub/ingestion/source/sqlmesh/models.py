import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sqlmesh.compat import SqlmeshContextType, SqlmeshModel
from datahub.ingestion.source.sqlmesh.constants import (
    ENV_SUFFIX_TARGET_SCHEMA,
)
from datahub.ingestion.source.sqlmesh.sqlmesh_config import SqlmeshSourceReport

logger = logging.getLogger(__name__)


class AuditResultsMetadata(BaseModel):
    # The ``metadata`` block of a ``sqlmesh audit --output`` file. Only
    # generated_at matters here (it anchors run/incident IDs); ignore the rest.
    model_config = ConfigDict(extra="ignore")

    generated_at: str = ""


class AuditResultEntry(BaseModel):
    # One entry from the ``results`` array of a SQLMesh audit-results file.
    # model/audit/status are required: an entry missing them is malformed and
    # must fail validation so it lands on the warning path in
    # _emit_audit_run_events rather than being silently skipped as an empty one.
    # audit/status are normalised to lowercase so downstream comparisons against
    # the lowercase audit map and status literals can't miss on casing.
    model_config = ConfigDict(extra="ignore")

    model: str
    audit: str
    columns: List[str] = Field(default_factory=list)
    status: str
    failing_rows: int = 0

    @field_validator("audit", "status", mode="after")
    @classmethod
    def _lowercase(cls, v: str) -> str:
        return v.lower()


def _build_count_query(physical_name: str, dialect: Optional[str] = None) -> str:
    """Render ``SELECT COUNT(*) FROM <table>`` with dialect-correct quoting.

    The naive ``f"SELECT COUNT(*) FROM {physical_name}"`` form breaks on
    catalogs / schemas / tables that contain hyphens or other identifier-
    significant characters (e.g. SQLMesh's example sushi project uses the
    catalog ``sushi-example``, which DuckDB parses as ``sushi - example``).
    Splitting on ``.`` and building the Table expression by parts lets
    SQLGlot quote each identifier per dialect.
    """
    # Imported lazily: sqlglot isn't a base dependency (it arrives only via the
    # ``[sqlmesh]`` extra's transitive sqlmesh dep), so a module-level import would
    # break loading this module with base deps — which `datahub check plugins` and
    # the CI plugin-import validation rely on. This helper only runs on the
    # row-count profiling path, which already requires the extra to be installed.
    from sqlglot import exp

    parts = physical_name.split(".")
    if len(parts) == 3:
        table_expr = exp.Table(
            this=exp.to_identifier(parts[2]),
            db=exp.to_identifier(parts[1]),
            catalog=exp.to_identifier(parts[0]),
        )
    elif len(parts) == 2:
        table_expr = exp.Table(
            this=exp.to_identifier(parts[1]),
            db=exp.to_identifier(parts[0]),
        )
    else:
        table_expr = exp.Table(this=exp.to_identifier(physical_name))

    return (
        exp.select(exp.Count(this=exp.Star()))
        .from_(table_expr)
        .sql(dialect=dialect, identify=True)
    )


class _EffectiveProjectConfig(BaseModel):
    project_path: str
    gateway: Optional[str]
    environment: str
    target_platform: Optional[str]  # None until auto-detected from context
    target_platform_instance: Optional[str]
    sqlmesh_platform_instance: Optional[str]
    default_catalog: Optional[str]
    convert_urns_to_lowercase: bool
    # Set after context loads — controls how non-prod warehouse sibling URNs are named.
    # One of "schema" (default), "table", or "catalog".
    env_suffix_target: str = ENV_SUFFIX_TARGET_SCHEMA
    # Maps env name regex → catalog override (mutually exclusive with catalog suffix mode).
    env_catalog_mapping: Dict[str, str] = Field(default_factory=dict)


class _CapabilityProbes(BaseModel):
    """Which data sources are reachable for this ingestion.

    State store, data warehouse, and DataHub Graph are three INDEPENDENT
    access concerns. Tobiko Cloud puts state in an HTTP API while data
    stays on the user's warehouse; multi-gateway OSS configs can also
    split state and data across gateways. Each emitter consults the
    relevant probe and picks the appropriate fallback signal.
    """

    has_state: bool = False
    has_warehouse_query: bool = False
    has_graph: bool = False


class _ModelAudit(BaseModel):
    # SQLMesh exposes ``model.audits`` as ``(name, kwargs)`` tuples; parsing
    # each into this model early keeps the connector off positional indexing.
    name: str
    # SQLGlot expressions keyed by audit argument name — genuinely arbitrary
    # per audit, so the values stay untyped.
    arguments: Dict[str, Any] = Field(default_factory=dict)


def parse_model_audits(model: "SqlmeshModel") -> List["_ModelAudit"]:
    audits: List[_ModelAudit] = []
    for entry in getattr(model, "audits", None) or []:
        if not entry:
            continue
        # Guard each entry independently: a single unexpected audit shape (e.g.
        # a non-subscriptable entry from a future sqlmesh version) must not abort
        # the whole model's emission — skip it and keep the rest.
        try:
            name = str(entry[0])
            raw_kwargs = entry[1] if len(entry) > 1 else None
        except (TypeError, IndexError, KeyError):
            logger.warning(
                "Skipping malformed audit entry %r on model %s",
                entry,
                getattr(model, "name", "?"),
                exc_info=True,
            )
            continue
        arguments = raw_kwargs if isinstance(raw_kwargs, dict) else {}
        audits.append(_ModelAudit(name=name, arguments=arguments))
    return audits


def _probe_capabilities(
    sqlmesh_ctx: "SqlmeshContextType",
    graph: Optional["DataHubGraph"],
    report: SqlmeshSourceReport,
) -> _CapabilityProbes:
    """Probe each signal once. Failures degrade gracefully, but each one silently
    removes an emission, so a failed probe is reported as a warning rather than
    only logged.
    """
    probes = _CapabilityProbes(has_graph=graph is not None)

    # State probe: smallest possible call into the state reader. Listing
    # environments hits the state store but doesn't load any per-model
    # snapshot detail, so it's cheap even on large projects.
    try:
        sqlmesh_ctx.state_reader.get_environments()
        probes.has_state = True
    except Exception as e:
        report.warning(
            title="SQLMesh state store unreachable",
            message="Last-rebuild operation aspects and stale-fingerprint detection are skipped for this run. Check the state connection / state schema grants.",
            exc=e,
        )

    # Warehouse-query probe: ping the engine adapter. We use a no-op
    # `SELECT 1` rather than schema introspection so the probe stays
    # uniform across dialects.
    try:
        sqlmesh_ctx.engine_adapter.fetchone("SELECT 1")
        probes.has_warehouse_query = True
    except Exception as e:
        report.warning(
            title="Data warehouse unreachable via SQLMesh engine adapter",
            message="Row-count dataset profiles are skipped for this run. Check the gateway connection and SELECT grants on the SQLMesh physical tables.",
            exc=e,
        )

    return probes
