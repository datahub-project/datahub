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

# XML schema-instance ``type`` attribute used on nested ``<calculationView>``
# nodes inside calc-view definitions.
XSI_TYPE_ATTR = "{http://www.w3.org/2001/XMLSchema-instance}type"

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
