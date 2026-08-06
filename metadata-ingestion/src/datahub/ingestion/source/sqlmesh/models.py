from dataclasses import dataclass, field
from typing import Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sqlmesh.compat import SqlmeshContextType
from datahub.ingestion.source.sqlmesh.sqlmesh_config import SqlmeshSourceReport


def _build_count_query(physical_name: str, dialect: Optional[str] = None) -> str:
    """Render ``SELECT COUNT(*) FROM <table>`` with dialect-correct quoting.

    The naive ``f"SELECT COUNT(*) FROM {physical_name}"`` form breaks on
    catalogs / schemas / tables that contain hyphens or other identifier-
    significant characters (e.g. SQLMesh's example sushi project uses the
    catalog ``sushi-example``, which DuckDB parses as ``sushi - example``).
    Parsing the dotted form back into a SQLGlot Table also fails for the
    same reason. The robust path is to split on ``.`` and build the Table
    expression by parts, letting SQLGlot quote per dialect.

    Supports 1-, 2-, and 3-part names. The default identifier policy uses
    SQLGlot's ``identify=True`` which double-quotes everything (or backticks
    for BigQuery).
    """
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


@dataclass
class _EffectiveProjectConfig:
    """Per-project resolved config: project-level overrides merged with global defaults."""

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
    env_suffix_target: str = "schema"
    # Maps env name regex → catalog override (mutually exclusive with catalog suffix mode).
    env_catalog_mapping: Dict[str, str] = field(default_factory=dict)


@dataclass
class _CapabilityProbes:
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
