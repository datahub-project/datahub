from typing import Dict, Optional, Protocol, Sequence, Union, runtime_checkable

from datahub.configuration.source_common import PlatformDetail
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.source.common.m_query.config import (
    DataBricksPlatformDetail,
    OraclePlatformDetail,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

# The concrete DataHub type-class union carried by a model column. Declared here
# (rather than importing a connector's type) so the engine stays source-agnostic.
DataHubFieldType = Union[
    BooleanTypeClass, DateTypeClass, NullTypeClass, NumberTypeClass, StringTypeClass
]


@runtime_checkable
class AthenaTableOverride(Protocol):
    # Structural view of an Athena federated-table platform override; the
    # engine only reads these four attributes when redirecting ODBC/Athena
    # lineage to the real upstream platform.
    @property
    def database(self) -> str: ...

    @property
    def table(self) -> str: ...

    @property
    def platform(self) -> str: ...

    @property
    def dsn(self) -> Optional[str]: ...


@runtime_checkable
class MQueryLineageConfig(Protocol):
    # The exact configuration surface the M-Query lineage engine reads. Any
    # connector (Power BI, Azure Analysis Services, …) can drive the engine by
    # providing an object with these attributes — no shared base class required.
    native_query_parsing: bool
    m_query_parse_timeout: int
    enable_advance_lineage_sql_construct: bool
    convert_lineage_urns_to_lowercase: bool
    server_to_platform_instance: Dict[
        str, Union[OraclePlatformDetail, DataBricksPlatformDetail, PlatformDetail]
    ]
    dsn_to_platform_name: Dict[str, str]
    dsn_to_database_schema: Dict[str, str]

    # Read-only so mypy treats the element type covariantly: a connector's
    # ``List[<own override>]`` satisfies this as long as each element is
    # structurally an ``AthenaTableOverride``.
    @property
    def athena_table_platform_override(self) -> Sequence[AthenaTableOverride]: ...


@runtime_checkable
class MQueryColumn(Protocol):
    name: str

    # Read-only properties so a connector that derives these from other fields
    # (e.g. via ``@property``) conforms just as well as one exposing plain
    # settable attributes.
    @property
    def dataType(self) -> str: ...

    @property
    def datahubDataType(self) -> DataHubFieldType: ...


@runtime_checkable
class MQueryTable(Protocol):
    # A model table carrying an M-Query ``expression``. ``full_name`` is used
    # only for logging/report context; ``name`` disambiguates unqualified
    # references in native SQL; ``columns`` feeds column-level lineage.
    name: str

    # Read-only properties so both a dataclass with plain fields (Power BI) and
    # a pydantic model deriving these via ``@property`` (Azure Analysis
    # Services) satisfy the protocol. ``columns`` is read-only for the same
    # reason plus covariance: a connector's ``List[<own column>]`` conforms
    # despite ``List`` being invariant.
    @property
    def full_name(self) -> str: ...

    @property
    def expression(self) -> Optional[str]: ...

    @property
    def columns(self) -> Optional[Sequence[MQueryColumn]]: ...


class MQueryReporter(Protocol):
    # Counters mutated in place by the engine, plus the structured-log methods
    # inherited from DataHub's SourceReport. A connector's own report satisfies
    # this by mixing in ``MQueryLineageReport``.
    m_query_parse_timer: PerfTimer
    m_query_parse_attempts: int
    m_query_parse_successes: int
    m_query_parse_timeouts: int
    m_query_native_query_skipped: int
    m_query_non_mquery_expressions: int
    m_query_parse_validation_errors: int
    m_query_parse_unexpected_character_errors: int
    m_query_parse_unknown_errors: int
    m_query_resolver_errors: int
    m_query_resolver_no_lineage: int
    m_query_resolver_successes: int

    @property
    def warnings(self) -> LossyList[StructuredLogEntry]: ...

    @property
    def infos(self) -> LossyList[StructuredLogEntry]: ...

    def warning(
        self,
        *,
        title: Optional[str] = None,
        message: str,
        context: Optional[str] = None,
        exc: Optional[BaseException] = None,
    ) -> None: ...

    def info(
        self,
        *,
        title: Optional[str] = None,
        message: str,
        context: Optional[str] = None,
    ) -> None: ...
