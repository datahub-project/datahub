from datahub.utilities.str_enum import StrEnum


class HanaSourceType(StrEnum):
    """The ``type`` attribute on a ``<DataSource>`` inside a calculation view."""

    DATA_BASE_TABLE = "DATA_BASE_TABLE"
    TABLE_FUNCTION = "TABLE_FUNCTION"
    CALCULATION_VIEW = "CALCULATION_VIEW"


class CalcViewNodeType(StrEnum):
    """The ``xsi:type`` attribute on a nested ``<calculationView>`` node."""

    PROJECTION = "Calculation:ProjectionView"
    JOIN = "Calculation:JoinView"
    AGGREGATION = "Calculation:AggregationView"
    UNION = "Calculation:UnionView"
    RANK = "Calculation:RankView"
    SQL_SCRIPT = "Calculation:SqlScriptView"


class ColumnEdgeKind(StrEnum):
    """Discriminator on a ``ColumnEdge`` between literal columns and formulas.

    Stored verbatim in :class:`ColumnEdge.kind`; the DAG traversal switches
    between "follow this source column to its leaf" (``COLUMN``) and "parse
    this expression for referenced columns" (``FORMULA``).
    """

    COLUMN = "column"
    FORMULA = "formula"


class OutputMappingKind(StrEnum):
    """Discriminator on a logical-model :class:`OutputMapping`.

    Calculation views expose two kinds of output bindings: dimensional
    attributes (under ``<attributes>``) and aggregatable measures (under
    ``<baseMeasures>``). We retain the kind so downstream consumers can tell
    them apart even after the source XML is gone.
    """

    ATTRIBUTE = "attribute"
    MEASURE = "measure"


# ``_SYS_BIC`` is the runtime schema HANA materializes activated calculation
# views into; users query them as ``"_SYS_BIC"."<package>/<view>"``.
SYS_BIC_SCHEMA = "_SYS_BIC"


# ---------------------------------------------------------------------------
# SAP-managed schemas denied by default. ``_SYS_BIC`` stays *allowed* — it
# exposes activated calculation views and is the entry point for calc-view
# lineage.
# ---------------------------------------------------------------------------

DEFAULT_DENY_SCHEMAS = [
    r"^SYS$",
    r"^_SYS_AUDIT$",
    r"^_SYS_BI$",
    r"^_SYS_BIC_CDS$",
    r"^_SYS_DATA_ANONYMIZATION$",
    r"^_SYS_EPM$",
    r"^_SYS_PLAN_STABILITY$",
    r"^_SYS_REPO$",
    r"^_SYS_RT$",
    r"^_SYS_SECURITY$",
    r"^_SYS_SQL_ANALYZER$",
    r"^_SYS_STATISTICS$",
    r"^_SYS_TASK$",
    r"^_SYS_TELEMETRY$",
    r"^_SYS_WORKLOAD_REPLAY$",
    r"^_SYS_XS$",
    r"^HANA_XS_BASE$",
]


# HANA's ``DUMMY`` is the analogue of Oracle's ``DUAL`` — a single-row
# pseudo-table that carries no lineage value.
HANA_PSEUDO_TABLES = frozenset({"DUMMY"})


# ---------------------------------------------------------------------------
# HANA native-type groupings used to re-assemble the canonical SQL spelling
# from ``SYS.VIEW_COLUMNS`` (DATA_TYPE_NAME + LENGTH + SCALE).
# ---------------------------------------------------------------------------

# HANA types whose canonical spelling carries both precision and scale, e.g.
# ``DECIMAL(15,2)``. ``SMALLDECIMAL`` is excluded: per the SAP HANA SQL
# Reference it is a self-describing variable-precision decimal that does
# not accept user-specified ``(precision, scale)``.
NUMERIC_WITH_PRECISION_SCALE_TYPES = frozenset({"DECIMAL", "NUMERIC"})

# HANA character / binary types whose canonical spelling carries a length,
# e.g. ``NVARCHAR(100)`` or ``VARBINARY(64)``.
LENGTH_BEARING_STRING_TYPES = frozenset(
    {"VARCHAR", "NVARCHAR", "ALPHANUM", "SHORTTEXT", "VARBINARY"}
)

# HANA's ``FLOAT(n)`` carries a bit-width (1-53) in ``SYS.VIEW_COLUMNS.LENGTH``
# but no scale.
PRECISION_ONLY_TYPES = frozenset({"FLOAT"})


# ---------------------------------------------------------------------------
# Calculation-view XML schema names.
#
# These are the element and attribute names defined by SAP's
# ``BiModelCalculation.ecore`` schema (referenced via the
# ``http://www.sap.com/ndb/BiModelCalculation.ecore`` namespace). Centralising
# them here keeps the parser declarative and makes a future XML-schema bump
# (e.g. HANA 2 → HANA Cloud calc-view dialect) a one-file change.
# ---------------------------------------------------------------------------


class CalcViewXmlElement(StrEnum):
    """Element names inside a HANA calculation-view XML."""

    DATA_SOURCE = "DataSource"
    COLUMN_OBJECT = "columnObject"
    RESOURCE_URI = "resourceUri"
    CALCULATION_VIEW = "calculationView"
    DEFINITION = "definition"
    INPUT = "input"
    CALCULATED_VIEW_ATTRIBUTE = "calculatedViewAttribute"
    FORMULA = "formula"
    MAPPING = "mapping"
    KEY_MAPPING = "keyMapping"
    MEASURE_MAPPING = "measureMapping"


class CalcViewXmlAttribute(StrEnum):
    """Attribute names inside a HANA calculation-view XML."""

    ID = "id"
    TYPE = "type"
    NODE = "node"
    SOURCE = "source"
    TARGET = "target"
    COLUMN_NAME = "columnName"
    COLUMN_OBJECT_NAME = "columnObjectName"
    SCHEMA_NAME = "schemaName"


# XPaths into the ``<logicalModel>`` block where output bindings live.
LOGICAL_MODEL_ATTRIBUTES_XPATH = ".//logicalModel/attributes/attribute"
LOGICAL_MODEL_MEASURES_XPATH = ".//logicalModel/baseMeasures/measure"

# An ``<input node="#NodeId"/>`` reference always starts with this sigil.
INPUT_NODE_REF_SIGIL = "#"

# XML schema-instance ``type`` attribute used on nested ``<calculationView>``
# nodes inside calc-view definitions. Stored as a Clark-notation qualified
# name because ``ElementTree`` already expands namespaces this way.
XSI_TYPE_ATTR = "{http://www.w3.org/2001/XMLSchema-instance}type"


# ---------------------------------------------------------------------------
# DataHub ``DatasetPropertiesClass.customProperties`` keys emitted for calc
# views. Centralised so dashboards / search filters can reference them by
# name without depending on the source code.
# ---------------------------------------------------------------------------


class CalcViewProperty(StrEnum):
    """``customProperties`` keys emitted on calculation-view datasets."""

    VIEW_TYPE = "view_type"
    PACKAGE_ID = "package_id"
    RUNTIME_VIEW_NAME = "runtime_view_name"


# The ``view_type`` customProperty value. Kept as a constant so search /
# filtering downstream pivots on a stable token.
CALCULATION_VIEW_TYPE_TAG = "CALCULATION_VIEW"
