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


class HanaObjectType(StrEnum):
    """``_SYS_REPO.ACTIVE_OBJECT.OBJECT_SUFFIX`` values we ingest."""

    CALCULATION_VIEW = "calculationview"
    PROCEDURE = "procedure"


# ``_SYS_BIC`` is the runtime schema HANA materializes activated calculation
# views into; users query them as ``"_SYS_BIC"."<package>/<view>"``.
SYS_BIC_SCHEMA = "_SYS_BIC"

# Container name used to group HANA stored procedures into a DataFlow per
# schema (matches the convention shared with Oracle / MSSQL).
STORED_PROCEDURES_CONTAINER = "stored_procedures"

# Statistics-service schema that snapshots ``M_SQL_PLAN_CACHE`` over time and
# the historical view holding those snapshots. Required for usage extraction.
STATISTICS_SCHEMA = "_SYS_STATISTICS"
HOST_SQL_PLAN_CACHE_VIEW = "HOST_SQL_PLAN_CACHE"

# HANA technical users we always exclude from usage attribution. ``SYS`` is the
# system schema owner; ``_SYS_*`` users (``_SYS_REPO``, ``_SYS_STATISTICS``,
# ``_SYS_TASK``, ``_SYS_XB`` etc.) own internal background work and never
# represent real query traffic. Match against ``UPPER(USER_NAME)``.
HANA_SYSTEM_USER_NAME = "SYS"
HANA_SYSTEM_USER_PREFIX = "_SYS_"


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
