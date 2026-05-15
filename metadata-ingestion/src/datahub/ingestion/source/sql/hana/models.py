from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class DataSourceRef(BaseModel):
    """A ``<DataSource>`` declared inside a calculation view."""

    name: str = ""
    path: str = ""
    type: str = ""


class ColumnEdge(BaseModel):
    """An ``<input>``/``<mapping>`` edge inside a calculation node."""

    source: str
    kind: Literal["column", "formula"] = "column"


class CalcViewNode(BaseModel):
    """A nested ``<calculationView>`` node in the DAG."""

    id: str
    type: str
    branches: Dict[str, Dict[str, ColumnEdge]] = Field(default_factory=dict)
    formulas: Dict[str, str] = Field(default_factory=dict)
    union_branches: List[str] = Field(default_factory=list)


class OutputMapping(BaseModel):
    """A ``logicalModel`` attribute or measure mapping."""

    source_column: str
    source_node: str
    kind: Literal["attribute", "measure"]


class CalcViewModel(BaseModel):
    """Intermediate in-memory representation of a parsed calculation view."""

    view_name: str
    sources: Dict[str, DataSourceRef] = Field(default_factory=dict)
    source_aliases: Dict[str, str] = Field(default_factory=dict)
    outputs: Dict[str, OutputMapping] = Field(default_factory=dict)
    nodes: Dict[str, CalcViewNode] = Field(default_factory=dict)
    scripts: Dict[str, str] = Field(default_factory=dict)


class UpstreamColumnRef(BaseModel):
    """A resolved upstream column from lineage tracing."""

    column: str
    source_name: str
    source_type: str
    source_path: str


class ColumnLineage(BaseModel):
    """The lineage for one downstream output column."""

    downstream_column: str
    upstreams: List[UpstreamColumnRef] = Field(default_factory=list)


class ScriptViewDefinition(BaseModel):
    """A captured SqlScriptView body for SQL-based lineage extraction."""

    node_id: str
    definition: str


class ScriptTableRef(BaseModel):
    """A schema-qualified table reference extracted from a SQLScript body."""

    schema_name: str
    name: str


class HanaObservedQueryRow(BaseModel):
    """A deduplicated ``_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`` row.

    Each row represents one distinct ``(statement_hash,
    last_execution_timestamp)`` observation — i.e. the most recent moment
    a cached plan was executed. Two snapshots of the same plan with the
    same ``LAST_EXECUTION_TIMESTAMP`` collapse to one row (the plan did
    not actually re-execute between snapshots).
    """

    statement_hash: str
    statement_string: str
    user_name: Optional[str] = None
    schema_name: Optional[str] = None
    application_name: Optional[str] = None
    last_execution_timestamp: Optional[datetime] = None


class HanaProcedureRow(BaseModel):
    """A single row from ``SYS.PROCEDURES`` plus its argument signature.

    Carries everything we need to construct a ``BaseProcedure`` without
    re-querying the catalog.
    """

    schema_name: str
    name: str
    definition: Optional[str] = None
    language: str = "SQL"
    procedure_type: Optional[str] = None
    create_time: Optional[str] = None
    argument_signature: Optional[str] = None
    return_type: Optional[str] = None
    comment: Optional[str] = None
