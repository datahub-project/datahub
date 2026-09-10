import dataclasses
import enum
import gzip
import hashlib
import json
import logging
import pathlib
import tempfile
from functools import cached_property
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

from snowflake.connector import errors as snowflake_errors
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tenacity.before_sleep import before_sleep_log
from typing_extensions import assert_never

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_group_urn,
)
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessor
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    GenericContainerSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_openflow_config import (
    SnowflakeOpenflowSourceConfig,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_models import (
    COL_CONNECTOR_URL,
    COL_CREATED_ON,
    OpenflowConnector,
    OpenflowDeployment,
    OpenflowRuntime,
    RowModel,
    get_col,
    get_str,
    merge_show_and_history,
    timestamp_shape,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_query import (
    SnowflakeOpenflowQuery,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_report import (
    LARGE_HISTORY_MESSAGE,
    SnowflakeOpenflowReport,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_lowercase_urns import (
    AutoLowercaseUrnsProcessor,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipTypeClass,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.utilities.sentinels import unset

logger = logging.getLogger(__name__)

PLATFORM = "openflow"

SNOWFLAKE_SOURCE_HINT = (
    "Not applicable: Openflow moves data and holds no catalog of its own. Use the "
    "`snowflake` source for the destination tables."
)

# --- Config-derived lineage constants ---------------------------------------

CONFIG_FILENAME = "config.json"
SECTION_SOURCE = "Source"
JDBC_PREFIX = "jdbc:"
# The SQL Server driver spells it databaseName; some tools emit `database`.
JDBC_DATABASE_PROPERTIES = frozenset({"databasename", "database"})
SECTION_REPLICATION = "Replication table schema"
SECTION_DESTINATION = "Destination details"
# The observed property name is "Source Database Connection URL" (probe Result 37,
# section [0] "Source"), NOT "JDBC URL". Both are tried because different connector
# definitions may name it differently -- OPENFLOW_POSTGRES_CDC is the only one
# observed. On a miss source_database stays None, which costs the upstream inlet on
# the three-tier platforms (see UpstreamNaming); the keys that WERE seen are reported
# so the miss is visible rather than silent.
PROP_SOURCE_URL_CANDIDATES = ("Source Database Connection URL", "JDBC URL")
PROP_INCLUDED_TABLE_NAMES = "Included Comma Separated Source Table Names"
PROP_INCLUDED_TABLE_PATTERN = "Included Source Table Pattern"
PROP_DESTINATION_DATABASE = "Snowflake Destination Database"
PROP_SCHEMA_STRATEGY = "Destination Schema Strategy"
SCHEMA_STRATEGY_SOURCE_SCHEMA = "SOURCE_SCHEMA"

# --- Stage GET retry --------------------------------------------------------

# The per-connector config read is the one query in this source that is not a
# single bulk statement: it runs once per connector and, unlike the three bulk
# queries, it downloads a file over the network. SnowflakeConnection.query()
# will not retry it -- its retry gates on "ACCOUNT_USAGE" appearing in the
# query text, which a GET never does, and that gate is deliberately narrow
# because it is shared with sources that also issue writes. So the retry lives
# here, where the caller knows its own statement is a read.
# Above this many DISTINCT RUNTIMES the DESCRIBE that fetches each runtime's
# canvas URL stops being a rounding error. It is one query per runtime, not per
# connector, because the URL is per-runtime and cached -- so this bound is only
# reached by an account with hundreds of separate runtimes, which _PLANNING.md's
# tenant assumption ("tens of runtimes") calls unusual. Setting
# include_connector_external_url explicitly overrides it at any size.
# Snowflake caps SHOW output at this many rows and does not flag the truncation.
_SHOW_ROW_CAP = 10_000
# A history view is read in full however many pages that takes, so correctness
# does not depend on this number; it only decides when the operator is told the
# read was unusually large, as the two sibling O(N) surfaces here already are.
#
# Sized from the measured row grain: incarnation-style, one row per object
# updated in place, so the row count tracks objects ever created rather than
# lifecycle events -- see probes/2026-09-03-openflow-sql-surface.md Round 16.
# That rules out the tens of thousands an event grain could reach, and with it
# the previous 50-page bar, which could never have fired. Measured on the one
# object each view held, so treat ~5,000 as headroom over a large account's
# ~500 connectors, not as a proven ceiling.
_HISTORY_PAGES_BEFORE_WARNING = 5
_MAX_RUNTIMES_FOR_URL_LOOKUP = 500

# Beyond this the readable per-table job id is replaced by a content hash, so
# the DataJob urn cannot outgrow what DataHub accepts.
# GMS rejects an aspect whose URL-encoded urn exceeds this; see
# metadata-utils UrnValidationUtil.URN_NUM_BYTES_LIMIT.
_MAX_URN_BYTES = 512
# A DataJob urn nests its DataFlow urn whole, so the flow must leave room for a
# job to exist at all: "urn:li:dataJob:(" + <flow urn> + "," + <job id> + ")".
# Encoded, the wrapper is 24 + 3 + 3 bytes and the smallest job id the ladder
# can produce is a 16-character digest, so 48 bytes of headroom. Without this a
# flow could sit at 509 bytes -- legal on its own -- and no job under it could
# ever fit, which is precisely what the previous revision shipped.
_NESTED_JOB_HEADROOM = 48
_FLOW_URN_BUDGET = _MAX_URN_BYTES - _NESTED_JOB_HEADROOM
# How much of the readable name a shortened id may keep, tried longest-first.
# The 0 is the floor and is load-bearing: it means the digest alone, which is
# ASCII and therefore fits whatever the identifier's encoding cost.
_SHORTENED_PREFIXES = (80, 40, 20, 0)

_RETRY_MAX_ATTEMPTS = 3
# Seconds. Produces waits of ~1s then ~2s. Read at call time rather than baked
# into a module-level Retrying, so a unit test can set it to 0 and exercise the
# loop without sleeping.
_RETRY_BACKOFF_MULTIPLIER = 1.0

# Only transient, connection-class failures are retried. A ProgrammingError
# (no READ on the version stage, no config.json at that URI) fails identically
# on every attempt, so retrying it only triples the time to the same warning --
# and a gate wide enough to catch it would also swallow real problems.
# Enumerated one by one because snowflake-connector's error hierarchy is flat:
# every class below derives straight from `Error`, alongside the deterministic
# ones, so there is no transient base class to catch instead.
_RETRYABLE_SNOWFLAKE_ERRORS: Tuple[Type[BaseException], ...] = (
    OSError,  # socket level: ConnectionError, TimeoutError, DNS failures
    snowflake_errors.OperationalError,
    snowflake_errors.InterfaceError,
    snowflake_errors.RequestTimeoutError,
    snowflake_errors.RequestExceedMaxRetryError,
    snowflake_errors.BadGatewayError,
    snowflake_errors.GatewayTimeoutError,
    snowflake_errors.InternalServerError,
    snowflake_errors.ServiceUnavailableError,
    snowflake_errors.OtherHTTPRetryableError,
)


class UpstreamNaming(enum.Enum):
    """How DataHub composes a dataset name on the upstream platform.

    A dataset URN whose *name* is at the wrong tier is still a well-formed URN,
    so nothing in the emit path raises -- the edge simply points at a dataset
    that cannot exist. The tier therefore has to be stated per platform, next
    to the platform itself, rather than assumed by a single shared formula.
    """

    # {database}.{schema}.{table}. The platform has a real schema layer between
    # database and table.
    #   postgres: PostgresSource.get_identifier, sql/postgres/source.py
    #             -> f"{self.config.database}.{schema}.{entity}"
    #   mssql:    SQLServerSource.get_identifier, sql/mssql/source.py
    #             -> f"{self.current_database}.{schema}.{entity}"
    DATABASE_SCHEMA_TABLE = "database.schema.table"

    # {database}.{table}. The platform has no schema layer at all, so the URN
    # carries two parts.
    #   mysql: modelled by TwoTierSQLAlchemySource (sql/two_tier_sql_source.py).
    #          get_allowed_schemas yields db_name as the "schema", and
    #          MySQLConfig.get_identifier (sql/mysql.py) returns
    #          f"{schema}.{table}" -- i.e. f"{database}.{table}".
    DATABASE_TABLE = "database.table"


@dataclasses.dataclass(frozen=True)
class UpstreamPlatform:
    platform: str
    naming: UpstreamNaming


# CONNECTOR_DEFINITION -> the upstream side's DataHub platform AND the tier its
# dataset names use. Both live in one entry on purpose: an earlier version
# carried only the platform here and hard-coded a three-tier name formula ~540
# lines away, which silently produced `mydb.mydb.mytable` for MySQL. Adding a
# fifth definition now forces its author to state the shape.
#
# OPENFLOW_KAFKA is deliberately absent. Kafka dataset names are the bare topic
# (KafkaSource, source/kafka/kafka.py: make_dataset_urn_with_platform_instance(
# ..., name=topic, ...)), which this connector cannot derive: the upstream side
# is reconstructed from a `jdbc:` Source URL and a schema-qualified table list,
# neither of which a Kafka connector carries. An unmapped definition is
# reported rather than guessed at -- see _lineage_for_connector.
CONNECTOR_DEFINITION_PLATFORM: Dict[str, UpstreamPlatform] = {
    "OPENFLOW_POSTGRES_CDC": UpstreamPlatform(
        "postgres", UpstreamNaming.DATABASE_SCHEMA_TABLE
    ),
    "OPENFLOW_MYSQL_CDC": UpstreamPlatform("mysql", UpstreamNaming.DATABASE_TABLE),
    "OPENFLOW_SQLSERVER_CDC": UpstreamPlatform(
        "mssql", UpstreamNaming.DATABASE_SCHEMA_TABLE
    ),
}


def upstream_identifier(
    upstream: UpstreamPlatform,
    source_database: Optional[str],
    source_schema: str,
    source_table: str,
) -> Optional[str]:
    """The upstream dataset's dotted name, at that platform's own tier.

    Returns None when the config did not carry what the tier needs, so the
    caller omits the inlet rather than emitting a URN with a missing part.
    """
    if upstream.naming is UpstreamNaming.DATABASE_SCHEMA_TABLE:
        if not source_database:
            return None
        return f"{source_database}.{source_schema}.{source_table}"
    if upstream.naming is UpstreamNaming.DATABASE_TABLE:
        # MySQL has no schema layer, so the qualifier in the connector's
        # "Included Comma Separated Source Table Names" entry (`db.table`) IS
        # the database -- that is the value used here, NOT the database parsed
        # out of the JDBC URL.
        #
        # Why the per-entry qualifier and not the URL's: the qualifier is the
        # only value that is per-table, so it stays correct for a CDC connector
        # replicating across more than one database, and it is still present
        # when the JDBC URL names no database at all (`jdbc:mysql://host:3306/`).
        # Where both are present and agree, the two readings coincide.
        #
        # Precedent: kafka_connect's Debezium handling resolves the same
        # ambiguity the same way -- DebeziumSourceConnector.
        # _get_database_name_for_platform (kafka_connect/source_connectors.py)
        # returns None for "mysql", so get_dataset_name(None, "db.table")
        # yields the two-part "db.table" while postgres/mssql get a database
        # prefixed on.
        #
        # AMBIGUITY, stated plainly: this has not been confirmed against a live
        # OPENFLOW_MYSQL_CDC connector -- none exists in the test account -- so
        # it rests on MySQL's own namespace rules plus the precedent above.
        return f"{source_schema}.{source_table}"
    assert_never(upstream.naming)


# --- Container keys -------------------------------------------------------


class OpenflowDeploymentKey(ContainerKey):
    deployment: str


class OpenflowRuntimeKey(OpenflowDeploymentKey):
    runtime: str


# --- Config-derived lineage ---------------------------------------------


@dataclasses.dataclass
class OpenflowLineage:
    source_database: Optional[str] = None
    # Kept only so an unparseable URL can be described in a warning.
    source_url: Optional[str] = None
    source_tables: List[Tuple[str, str]] = dataclasses.field(default_factory=list)
    table_pattern: Optional[str] = None
    destination_database: Optional[str] = None
    schema_strategy: Optional[str] = None
    unparseable_tables: List[str] = dataclasses.field(default_factory=list)
    # Populated when the Source section carried none of the candidate URL keys, so the
    # caller can report which keys it DID see rather than silently emitting no upstream.
    unrecognised_source_url_keys: List[str] = dataclasses.field(default_factory=list)


def property_value(properties: Dict[str, Any], key: str) -> Optional[str]:
    # Every observed config property is wrapped as {"valueType": ..., "value": ...}.
    # An unset property carries the valueType but NO "value" key at all (not `null`,
    # not ""), and a non-literal valueType -- ASSET_REFERENCE, SECRET_REFERENCE --
    # carries assetIds / fullyQualifiedSecretName instead of "value". A live crash
    # (TypeError: unhashable type: 'slice', from treating the wrapper dict as the
    # URL string itself) is what surfaced this: an earlier version of this parser
    # was written against a probe artifact's flattened notes rather than the JSON,
    # and the notes had silently unwrapped every property for readability.
    #
    # "value" is read whenever present, regardless of valueType, rather than gating
    # on valueType == "STRING_LITERAL" -- more tolerant of types this connector
    # has not seen, since a wrapper with no "value" key is never mistaken for one
    # that has a value.
    wrapped = properties.get(key)
    if isinstance(wrapped, str):
        # Defensive: every observed property is wrapped, but a future config
        # format version could flatten one to a bare string.
        return wrapped
    if isinstance(wrapped, dict):
        value = wrapped.get("value")
        if isinstance(value, str):
            return value
    return None


def _canvas_url(connector_url: str) -> Optional[str]:
    """The runtime's Openflow canvas, derived from the connector URL.

    CONNECTOR_URL's own fragment does not resolve: it points at
    `#/connectors/<snowflake-connector-id>/`, and the canvas app has no such
    route -- it uses `#/process-groups/<nifi-process-group-id>`, whose id is a
    NiFi-generated one that appears in no Snowflake surface (measured: the
    ids differ entirely, and only the NiFi REST API would supply the real one).
    So the fragment is dropped rather than rewritten, since a guessed id would
    404 exactly as the reported one does.

    Everything up to and including `/nifi/` IS correct -- the broken URL and a
    working canvas URL agree on origin and path -- so that prefix is kept. The
    result opens the runtime's canvas rather than the individual connector.
    """
    marker = "/nifi/"
    index = connector_url.find(marker)
    if index == -1:
        return None
    try:
        base = urlparse(connector_url[: index + len(marker)])
        if not base.scheme or not base.hostname:
            return None
        host = base.hostname
        # Snowflake reports the default port explicitly; its own UI omits it.
        port = base.port
    except ValueError:
        # urlparse raises on a malformed authority (an unclosed IPv6 bracket),
        # and .port raises on a non-numeric port. This value comes from
        # Snowflake rather than from us, so a surface change must cost this
        # connector its link, never the whole ingestion run.
        return None
    if port and not (base.scheme == "https" and port == 443):
        host = f"{host}:{port}"
    return f"{base.scheme}://{host}{base.path}"


def _url_shape(source_url: Optional[str]) -> str:
    """The subprotocol only -- never the whole URL.

    A JDBC URL routinely carries a host, and can carry a user and password in
    its properties. The report is operator-visible and persisted, so it gets
    just enough to tell an unrecognised spelling from a missing database:
    `jdbc:sqlserver://...`.
    """
    if not source_url:
        return "<absent>"
    scheme, separator, _ = source_url.partition("://")
    if separator:
        return f"{scheme}://..."
    # No "://" means an unrecognised layout -- a DSN-style `user:pass@host`, an
    # Oracle thin URL, a hand-rolled property string. Precisely the input whose
    # structure cannot be reasoned about, so NOTHING of it is echoed. An earlier
    # revision returned source_url[:16] here, which rendered
    # `user=admin;password=hunter2;...` as `user=admin;passw...` into a
    # persisted, operator-visible report. Sixteen arbitrary characters carry no
    # diagnostic value that a length does not.
    return f"<unrecognised, {len(source_url)} chars>"


@dataclasses.dataclass
class ConnectorTableLineage:
    """One replicated table: where it comes from and where it lands.

    The pairing is what makes this a type rather than two parallel lists. A
    connector replicating N tables produces N independent 1:1 edges; flattening
    them onto one DataJob's inlets/outlets says instead that every source feeds
    every destination, which is N*N edges DataHub will happily render.
    """

    source_schema: str
    source_table: str
    outlet: str
    # None when the upstream platform's identifier could not be composed --
    # counted and warned at the call site. The destination half still stands.
    inlet: Optional[str] = None


def _jdbc_database(source_url: str) -> Optional[str]:
    """The database named by a JDBC URL, or None if it does not carry one.

    Two shapes, because SQL Server does not use the other one. Postgres and
    MySQL put the database in the path (`jdbc:postgresql://h:5432/mydb`), while
    the SQL Server driver takes it as a semicolon property
    (`jdbc:sqlserver://h:1433;databaseName=mydb`) and leaves the path empty --
    so parsing only the path silently drops every OPENFLOW_SQLSERVER_CDC
    upstream, which is a supported connector type.
    """
    if not source_url.startswith(JDBC_PREFIX):
        # Not a JDBC URL at all. Stripping a prefix that is not there would
        # eat five characters and yield a plausible but wrong database name.
        return None
    remainder = source_url[len(JDBC_PREFIX) :]
    head, _, properties = remainder.partition(";")
    for prop in properties.split(";"):
        key, sep, value = prop.partition("=")
        if sep and key.strip().lower() in JDBC_DATABASE_PROPERTIES:
            return value.strip() or None
    return urlparse(head).path.lstrip("/") or None


def _parse_table_names(raw: str) -> Tuple[List[Tuple[str, str]], List[str]]:
    # Returns (parsed, unparseable). The second element exists so the caller can
    # report dropped entries: an entry with no schema qualifier silently vanishing
    # from lineage is indistinguishable from a connector that legitimately has no
    # source tables, and losing one table out of ten is exactly the kind of
    # partial-lineage failure nothing else in the pipeline would surface.
    tables: List[Tuple[str, str]] = []
    unparseable: List[str] = []
    for entry in raw.split(","):
        cleaned = entry.strip().replace('"', "")
        if not cleaned:
            continue
        if "." not in cleaned:
            unparseable.append(cleaned)
            continue
        schema, _, table = cleaned.rpartition(".")
        tables.append((schema, table))
    return tables, unparseable


def decode_config_payload(content: bytes) -> Dict[str, Any]:
    """The downloaded config bytes as a JSON object, or raise.

    Separated from the download so the decision it makes is reachable by a test
    without a stage, a temporary directory or a GET. It was inline first, and a
    mutation pass showed the isinstance check could be deleted with the whole
    suite still green -- the only test covering it faked the download and so
    never ran this code at all.

    Valid JSON is not necessarily a config. Raising on a non-object routes an
    array/string/number payload into the caller's counted-and-warned path,
    costing this connector its lineage and nothing else. Returning it instead
    would reach parse_connector_config, whose .get() would raise AttributeError
    out of get_workunits_internal and abort the WHOLE run -- every other
    connector's metadata lost to one malformed file.
    """
    # Whether a staged file arrives gzip-compressed depends on how it was
    # staged, not on anything this connector controls. Detected via the magic
    # bytes rather than a ".gz" suffix, which is a naming convention GET
    # applies rather than a guarantee.
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"expected a JSON object, got {type(parsed).__name__}")
    return parsed


def parse_connector_config(config_json: Dict[str, Any]) -> OpenflowLineage:
    # Iterate EVERY section. Descending into configuration[0] only finds the
    # destination on connectors that happen to list it first.
    lineage = OpenflowLineage()
    sections = config_json.get("configuration")
    for section in sections if isinstance(sections, list) else []:
        # `configuration` is an array of objects in every config seen, but this
        # file is written by Openflow and read here without a schema, so a
        # differently-shaped one must degrade to no lineage rather than take
        # the run down with it.
        if not isinstance(section, dict):
            continue
        name = section.get("name")
        properties = section.get("properties")
        if not isinstance(properties, dict):
            properties = {}
        if name == SECTION_SOURCE:
            source_url: Optional[str] = None
            for candidate_key in PROP_SOURCE_URL_CANDIDATES:
                source_url = property_value(properties, candidate_key)
                if source_url:
                    break
            if source_url:
                lineage.source_url = source_url
                lineage.source_database = _jdbc_database(source_url)
            else:
                lineage.unrecognised_source_url_keys = sorted(properties)
        elif name == SECTION_REPLICATION:
            names = property_value(properties, PROP_INCLUDED_TABLE_NAMES)
            if names:
                lineage.source_tables, lineage.unparseable_tables = _parse_table_names(
                    names
                )
            lineage.table_pattern = property_value(
                properties, PROP_INCLUDED_TABLE_PATTERN
            )
        elif name == SECTION_DESTINATION:
            lineage.destination_database = property_value(
                properties, PROP_DESTINATION_DATABASE
            )
            lineage.schema_strategy = property_value(properties, PROP_SCHEMA_STRATEGY)
    return lineage


def destination_identifier(
    destination_database: str,
    source_schema: str,
    source_table: str,
    schema_strategy: Optional[str],
) -> Optional[str]:
    # Only SOURCE_SCHEMA is implemented. Prefix/Suffix/Pattern strategies exist;
    # guessing one produces a well-formed URN naming a table that is not there,
    # which no layer reports as an error.
    #
    # This is the single formula for the destination's dotted identifier: the
    # caller feeds the returned string straight into the URN (after case
    # folding) rather than recomputing it, so a future Prefix/Suffix strategy
    # only ever needs a change here.
    if schema_strategy != SCHEMA_STRATEGY_SOURCE_SCHEMA:
        return None
    return f"{destination_database}.{source_schema}.{source_table}"


# --- Ownership --------------------------------------------------------------

# Snowflake's OWNER is the name of a ROLE, never a user, so it maps to a
# corpGroup urn. The rest of the Snowflake family settles this the same way
# (snowflake_tasks.py, snowflake_pipes.py, snowflake_stages.py).
#
# TECHNICAL_OWNER on every surface: the role that owns the Snowflake object is
# who operates it, and using one type across containers, the DataFlow and the
# DataJob keeps a single source field from showing up as two different
# ownership kinds in the UI.
OWNERSHIP_TYPE = OwnershipTypeClass.TECHNICAL_OWNER


def _owner_group_urn(owner: Optional[str]) -> Optional[str]:
    return make_group_urn(owner) if owner else None


def _owner_classes(owner: Optional[str]) -> Optional[List[OwnerClass]]:
    # Returns None -- not [] -- when there is no owner, so the SDK v2 entities
    # skip the aspect entirely. An OwnershipClass with an empty owners list
    # would overwrite owners a user had set in DataHub by hand.
    #
    # An explicit OwnerClass, never a bare string: `owners=["MY_ROLE"]` is
    # routed through make_user_urn by HasOwnership._parse_owner_class
    # (sdk/_shared.py), which would silently emit urn:li:corpuser:MY_ROLE.
    urn = _owner_group_urn(owner)
    if urn is None:
        return None
    return [OwnerClass(owner=urn, type=OWNERSHIP_TYPE)]


# --- Connector DataFlow / DataJob -------------------------------------------


def _connector_properties(connector: OpenflowConnector) -> Dict[str, str]:
    properties: Dict[str, str] = {}
    if connector.connector_id:
        properties["connector_id"] = connector.connector_id
    if connector.connector_definition:
        properties["connector_definition"] = connector.connector_definition
    if connector.default_version:
        properties["default_version"] = connector.default_version
    if connector.status:
        properties["status"] = connector.status
    if connector.runtime_name:
        properties["runtime"] = connector.runtime_name
    return properties


def build_connector_flow(
    connector: OpenflowConnector,
    platform_instance: Optional[str],
    env: str,
    parent_container: Optional[OpenflowRuntimeKey] = None,
    external_url: Optional[str] = None,
) -> DataFlow:
    # Keyed on the COMPOSITE <runtime_name>/<connector_name> (connector.key), not on
    # CONNECTOR_ID and not on the bare name. Three measured facts force this:
    #   - SHOW OPENFLOW CONNECTORS returns no id column, so CONNECTOR_ID is not
    #     available for every connector and cannot be the identity.
    #   - The ACCOUNT_USAGE views lag ~20 min, so an id-keyed URN would change
    #     identity once the view caught up, producing two entities for one connector.
    #   - Snowsight allows several connectors to share a display name, so the bare
    #     name collides across runtimes.
    # CONNECTOR_ID is carried in custom properties instead.
    # Measured against the entity's OWN urn rather than a character budget, so
    # this cannot drift from however the SDK composes it.
    return _fitted(
        lambda name: _flow_with_name(
            connector, name, platform_instance, env, parent_container, external_url
        ),
        connector.key,
        _FLOW_URN_BUDGET,
    )


def _flow_with_name(
    connector: OpenflowConnector,
    name: str,
    platform_instance: Optional[str],
    env: str,
    parent_container: Optional[OpenflowRuntimeKey],
    external_url: Optional[str],
) -> DataFlow:
    return DataFlow(
        name=name,
        platform=PLATFORM,
        platform_instance=platform_instance,
        env=env,
        display_name=connector.display_name or connector.name,
        subtype=DataFlowSubTypes.OPENFLOW_CONNECTOR,
        custom_properties=_connector_properties(connector),
        owners=_owner_classes(connector.owner),
        external_url=external_url,
        # `unset`, not None: the SDK treats None as "this entity has no parent"
        # and writes an EMPTY browsePathsV2, which then suppresses the one
        # auto_browse_path_v2 would otherwise derive. `unset` leaves both
        # aspects off the flow entirely.
        parent_container=parent_container if parent_container is not None else unset,
    )


# java.net.URLEncoder leaves only these unencoded; everything else becomes %XX
# per UTF-8 byte, and a space becomes "+". Python's quote_plus does NOT agree:
# it passes "~" through where Java writes %7E, and encodes "*" where Java does
# not. The "~" direction is the dangerous one -- it UNDER-measures, so a urn
# could pass this check and still be rejected by GMS.
_JAVA_URLENCODER_SAFE = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-*_"
)


def encoded_urn_len(urn: object) -> int:
    """The length GMS measures: java.net.URLEncoder.encode(urn).length()."""
    return sum(
        1 if char in _JAVA_URLENCODER_SAFE or char == " " else 3 * len(char.encode())
        for char in str(urn)
    )


def urn_fits(urn: object, budget: int = _MAX_URN_BYTES) -> bool:
    """Whether GMS will accept this URN's length.

    The limit is on the URL-ENCODED urn, not on any component of it -- see
    UrnValidationUtil.URN_NUM_BYTES_LIMIT. That distinction is the whole point:
    an earlier guard here bounded the job NAME to 200 characters and was
    measured as correct against that number, while the DataJob urn it produced
    was 573 bytes, because the urn embeds connector.key TWICE (once on its own,
    once inside the flow urn it nests under). GMS rejects the aspect and the
    table's lineage is lost. Bound the thing the server actually measures.
    """
    return encoded_urn_len(urn) <= budget


EntityT = TypeVar("EntityT", DataFlow, DataJob)


def _fitted(
    build: Callable[[str], EntityT], readable: str, budget: int = _MAX_URN_BYTES
) -> EntityT:
    """The entity built from `readable`, shortened until its urn fits.

    Shortening by CHARACTERS is not enough, which is the trap this replaces: the
    limit is on the URL-ENCODED urn, and one CJK character encodes to nine bytes
    and one emoji to twelve. An 80-character prefix of a CJK name is 720 bytes of
    urn on its own, so a single shorten-and-return produced a 1550-byte DataJob
    urn -- rejected by GMS, and the table's lineage lost. Measured, not reasoned.

    So each candidate is built and measured, and the ladder ends at the digest
    alone, which is 16 ASCII characters and therefore always fits. hashlib
    rather than the builtin hash: the builtin is salted per process, so the id
    would name a different entity on every run.
    """
    entity = build(readable)
    if urn_fits(entity.urn, budget):
        return entity
    digest = hashlib.md5(readable.encode("utf-8")).hexdigest()[:16]
    for prefix in _SHORTENED_PREFIXES:
        # "-" rather than "~": both encoders leave it alone, so the separator
        # cannot be the thing that makes the measurement disagree with GMS.
        entity = build(f"{readable[:prefix]}-{digest}" if prefix else digest)
        if urn_fits(entity.urn, budget):
            return entity
    return entity


def _table_job_name(connector: OpenflowConnector, pair: ConnectorTableLineage) -> str:
    return f"{connector.key}/{pair.source_schema}.{pair.source_table}"


def build_connector_table_job(
    connector: OpenflowConnector, flow: DataFlow, pair: ConnectorTableLineage
) -> DataJob:
    """One DataJob per replicated table, carrying that table's edge alone."""
    # The job urn nests the flow urn, so it carries connector.key twice and can
    # exceed the cap while the flow's own urn is comfortably inside it. The flow
    # handed in here is already fitted, so only the job half can still overflow.
    return _fitted(
        lambda name: _job_with_name(connector, flow, pair, name),
        _table_job_name(connector, pair),
    )


def _job_with_name(
    connector: OpenflowConnector,
    flow: DataFlow,
    pair: ConnectorTableLineage,
    name: str,
) -> DataJob:
    return DataJob(
        name=name,
        flow=flow,
        display_name=f"{pair.source_schema}.{pair.source_table}",
        subtype=DataJobSubTypes.OPENFLOW_CONNECTOR_SYNC,
        inlets=[pair.inlet] if pair.inlet else [],
        outlets=[pair.outlet],
        owners=_owner_classes(connector.owner),
    )


# --- Source -----------------------------------------------------------------


# id is the PLATFORM id, not the recipe type: entities are emitted on
# urn:li:dataPlatform:openflow (PLATFORM above), and data-platforms.yaml seeds
# that same name. The recipe type stays "snowflake-openflow" via the entry point.
@platform_name("Snowflake Openflow", id="openflow")
@config_class(SnowflakeOpenflowSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.OPENFLOW_DEPLOYMENT,
        SourceCapabilityModifier.OPENFLOW_RUNTIME,
    ],
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Derived from each connector's own configuration; disable with "
    "`include_table_lineage: false`",
)
@capability(SourceCapability.OWNERSHIP, "Extracted from each object's OWNER")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion, using DELETED_ON from the "
    "ACCOUNT_USAGE views",
    supported=True,
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, SNOWFLAKE_SOURCE_HINT, supported=False)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Not supported: Openflow publishes no column-level mapping. Neither the "
    "connector's configuration nor the OPENFLOW_* ACCOUNT_USAGE views carry a "
    "column-to-column relation, and the configuration selects source tables by "
    "name or pattern, so lineage is derivable only at table grain.",
    supported=False,
)
@capability(SourceCapability.DATA_PROFILING, SNOWFLAKE_SOURCE_HINT, supported=False)
@capability(
    SourceCapability.USAGE_STATS,
    "Not supported: OPENFLOW_USAGE_HISTORY reports credit consumption, not dataset "
    "usage.",
    supported=False,
)
@capability(
    SourceCapability.TAGS,
    "Not supported: the Openflow object views expose no tag column.",
    supported=False,
)
@capability(
    SourceCapability.DOMAINS,
    "Not supported: domains are assigned in DataHub rather than sourced from Openflow.",
    supported=False,
)
class SnowflakeOpenflowSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Snowflake Openflow deployments, runtimes and connectors.

    Openflow objects are enumerated with ``SHOW OPENFLOW ...`` and mapped as:
    a deployment becomes a Container, a runtime a Container nested under it, and
    each connector a DataFlow. A connector IS the pipeline, so the DataJobs
    inside it are the tables it replicates -- one per table, which is what keeps
    each table's lineage on its own edge. Ownership comes from the Snowflake
    object OWNER, which is a role, so it maps to a corpGroup.

    Lineage is derived from **configuration**, not from observed runs. Each
    connector's ``config.json`` is fetched from its version stage and parsed for
    the source connection URL, the replicated table list and the destination
    database plus schema strategy. That yields table-level edges from the
    upstream platform's datasets to the Snowflake tables the connector writes.
    Because the edges are config-derived, anything the configuration does not
    state -- a table *pattern* instead of an explicit list, an unimplemented
    destination schema strategy, an unrecognised connection-URL property -- is
    reported as a warning and counted rather than guessed, so partial lineage
    is never silent.
    """

    def __init__(
        self, config: SnowflakeOpenflowSourceConfig, ctx: PipelineContext
    ) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.platform = PLATFORM
        # Set once per run by _decide_url_lookup, before the connector loop.
        self._fetch_connector_urls = True
        self.report: SnowflakeOpenflowReport = SnowflakeOpenflowReport()
        self.connection: SnowflakeConnection = config.connection.get_connection()

        # Stale entity removal needs NO wiring here — AutoStaleEntityRemovalProcessor
        # (api/source.py) enables itself from self.state_provider (always set by
        # StatefulIngestionSourceBase) and this report being a
        # StaleEntityRemovalSourceReport.

        self._warn_if_platform_instance_casing_is_ambiguous()
        self._warn_if_upstream_folding_is_unverifiable()

    def _warn_if_platform_instance_casing_is_ambiguous(self) -> None:
        # Warn, do not reject. Which casing is correct depends on how the OTHER
        # recipe is written, which this source cannot observe, so there is no value
        # that is right in every case.
        #
        # AutoLowercaseUrnsProcessor.should_enable gates on the key being PRESENT in
        # the raw recipe (`return bool(recipe_value)`, where an absent key is None),
        # not on the parsed config. So a `snowflake` recipe has three geometries:
        #   key absent  -> the processor does NOT run, but SnowflakeIdentifierConfig
        #                  still folds the identifier, so the instance stays verbatim
        #                  -- e.g. PROD_SF.db.schema.table. This is the DEFAULT, and
        #                  it is the geometry THIS source produces.
        #   key = true  -> the processor runs and folds the whole name, instance too
        #                  -- prod_sf.db.schema.table.
        #   key = false -> nothing is folded.
        # An uppercase instance therefore matches the default recipe and mismatches
        # the explicit-flag one. An earlier revision raised a ValueError telling the
        # operator to lowercase; that silently un-joined them from the DEFAULT
        # geometry, which is the likelier one.
        instance = self.config.snowflake_platform_instance
        if not instance or instance == instance.lower():
            return
        self.report.warning(
            title="Snowflake platform instance case may not match the snowflake source",
            message="This source folds the destination identifier but leaves the "
            "platform_instance prefix verbatim, matching a `snowflake` recipe that "
            "does NOT set convert_urns_to_lowercase (its default). A `snowflake` "
            "recipe that sets the key to true folds the prefix as well, and these "
            "URNs will not match it. Lowercase the instance on both sides if that "
            "is the recipe you run.",
            context=f"snowflake_platform_instance={instance!r}",
        )

    def _warn_if_upstream_folding_is_unverifiable(self) -> None:
        # The mirror of _warn_if_platform_instance_casing_is_ambiguous, for the
        # upstream side. Same shape of problem and the same answer: warn, never
        # guess, because the correct value depends on a recipe this source cannot
        # observe.
        #
        # source_convert_urns_to_lowercase is only correct to set when the UPSTREAM
        # recipe spells convert_urns_to_lowercase out. postgres, mysql and mssql all
        # default it to False, and an absent key leaves AutoLowercaseUrnsProcessor
        # off for that source too -- so the operator has to know which of those two
        # their upstream recipe is, and nothing here can check it. A mismatch emits a
        # well-formed URN naming a dataset that does not exist, which renders in the
        # UI exactly like a live one, so it must be said out loud rather than
        # discovered.
        #
        # Only warn when a coordinate was actually chosen: on the default recipe
        # there is nothing to mismatch, and a warning every run would be noise.
        #
        # source_env cannot be tested for None -- default_source_env_to_env fills it
        # from `env` during validation, so it is always set. Compare it to `env`
        # instead, which is what "the operator chose an upstream env" actually means.
        if not self.config.include_table_lineage:
            return
        configured = (
            self.config.source_platform_instance is not None
            or self.config.source_convert_urns_to_lowercase
            or self.config.source_env != self.config.env
        )
        if not configured:
            return
        # info, not warning. There is no state in which this source CAN verify the
        # coordinates, so a warning here can never be cleared -- a correctly
        # configured deployment would finish every run "with warnings" and nothing
        # the operator does would fix it. An unclearable warning is noise, and this
        # connector has four conditional warnings whose credibility it would spend.
        # Its twin returns early unless a real ambiguity exists; this one cannot, so
        # it drops a severity level instead.
        self.report.info(
            title="Upstream coordinates cannot be verified from this source",
            message="Upstream lineage URNs are built from source_platform_instance, "
            "source_env and source_convert_urns_to_lowercase, which must match the "
            "recipe that ingests the upstream system. This source cannot read that "
            "recipe, so a mismatch is not detectable here and produces lineage "
            "pointing at datasets that do not exist. Set "
            "source_convert_urns_to_lowercase to true only if that recipe sets "
            "convert_urns_to_lowercase explicitly; postgres, mysql and mssql all "
            "default it to false.",
            context=(
                f"source_platform_instance="
                f"{self.config.source_platform_instance!r}, "
                f"source_env={self.config.source_env!r}, "
                f"source_convert_urns_to_lowercase="
                f"{self.config.source_convert_urns_to_lowercase!r}"
            ),
        )

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "SnowflakeOpenflowSource":
        config = SnowflakeOpenflowSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(
        self,
    ) -> List[Union[str, Type[WorkunitProcessor]]]:
        # This source emits dataset URNs for two different platforms, and only one
        # of them may be folded. The destination Snowflake URNs are already folded
        # in-source by SnowflakeIdentifierBuilder.snowflake_identifier() from the
        # parsed convert_urns_to_lowercase, so the pipeline-level pass adds nothing
        # for them. What it does add is damage: it folds every dataset URN in the
        # stream, including the postgres/mysql/mssql upstream inlets that
        # _lineage_for_connector builds verbatim to match what those platforms'
        # own sources wrote. Those sources default convert_urns_to_lowercase to
        # False (LowerCaseDatasetUrnConfigMixin), so a folded inlet joins to
        # nothing for any case-preserving upstream. lowercase_dataset_urns()
        # offers no per-platform or per-aspect exemption, so the only way to keep
        # the inlets verbatim is to keep the processor off this source entirely.
        return [AutoLowercaseUrnsProcessor]

    def get_report(self) -> SnowflakeOpenflowReport:
        return self.report

    def close(self) -> None:
        # finally, not sequence: StatefulIngestionSourceBase.close() is what
        # prepares the checkpoint commit. If the connection teardown raises,
        # skipping it would leave the next run reconciling against a stale
        # checkpoint -- and this source declares DELETION_DETECTION, so that
        # soft-deletes or resurrects entities. The sibling snowflake_v2 closes
        # the base first for the same reason.
        try:
            self.connection.close()
        finally:
            super().close()

    @staticmethod
    def test_connection(config_dict: Dict[str, Any]) -> TestConnectionReport:
        report = TestConnectionReport()
        try:
            # Config parse and connect share one handler: neither is the
            # visibility probe below, and either failing here means there is
            # no connection to attribute a capability failure to.
            config = SnowflakeOpenflowSourceConfig.model_validate(config_dict)
            connection = config.connection.get_connection()
        except Exception as exc:
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(exc)
            )
            return report
        report.basic_connectivity = CapabilityReport(capable=True)
        try:
            # A successful connection says nothing about Openflow visibility,
            # which is granted per object. Probe it separately so a role missing
            # MONITOR is reported here rather than as an empty ingestion. The
            # probe itself erroring (a permission error, a transient failure) is
            # caught below rather than escaping, and reported distinctly from
            # the zero-rows case: both are "not capable", but only the latter
            # names MONITOR.
            rows = list(connection.query(SnowflakeOpenflowQuery.show_deployments()))
            report.capability_report = {
                SourceCapability.CONTAINERS: CapabilityReport(
                    capable=bool(rows),
                    failure_reason=None
                    if rows
                    else "No Openflow deployments are visible to this role. Grant "
                    "MONITOR on the deployments and runtimes to ingest.",
                )
            }
        except Exception as exc:
            report.capability_report = {
                SourceCapability.CONTAINERS: CapabilityReport(
                    capable=False, failure_reason=str(exc)
                )
            }
        finally:
            connection.close()
        return report

    def _deployment_key(self, deployment: OpenflowDeployment) -> OpenflowDeploymentKey:
        return OpenflowDeploymentKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            deployment=deployment.key,
        )

    def _runtime_key(
        self, runtime: OpenflowRuntime, deployment: OpenflowDeployment
    ) -> OpenflowRuntimeKey:
        return OpenflowRuntimeKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            deployment=deployment.key,
            runtime=runtime.key,
        )

    def _query_rows(self, query: str) -> List[Dict[str, Any]]:
        return [dict(row) for row in self.connection.query(query)]

    def _fail_if_show_truncated(
        self, rows: List[Dict[str, Any]], object_type: str
    ) -> None:
        # SHOW returns at most _SHOW_ROW_CAP rows and says nothing when it
        # truncates -- the caller just sees a short inventory, and with stateful
        # ingestion everything past the cap reads as deleted. Detected by
        # equality with the cap rather than by paginating: this family's
        # name-cursor continuation is known to skip and duplicate rows under
        # concurrent create/rename, which is a worse failure than the one it
        # would close, and a realistic account sits far below the cap.
        #
        # The cap is Snowflake's documented SHOW limit; it is NOT separately
        # confirmed for the OPENFLOW command family. If the real cap is lower
        # this warning simply never fires -- it can miss a truncation, but it
        # cannot invent one.
        if len(rows) == _SHOW_ROW_CAP:
            self.report.num_show_results_at_row_cap += 1
            # failure, not warning, and the distinction is the whole point. This
            # is the one case where the source KNOWS its inventory is short, and
            # with stateful ingestion a short inventory is a deletion order:
            # every object past the cap is absent from this run's checkpoint and
            # gets soft-deleted. StaleEntityRemovalHandler skips soft-deletion
            # when the source has reported a failure -- see
            # stale_entity_removal_handler.py, "If the source already had a
            # failure, skip soft-deletion" -- so reporting one here is what
            # actually prevents the deletion, rather than merely narrating it.
            self.report.failure(
                title="Inventory is truncated",
                message=(
                    f"SHOW returned exactly {_SHOW_ROW_CAP} rows, which is the "
                    "row cap, so there are almost certainly more objects than "
                    "were read. This is reported as a failure so that stale-"
                    "entity removal is skipped: otherwise every object past the "
                    "cap would be soft-deleted on the strength of an inventory "
                    "this source already knows is incomplete."
                ),
                context=object_type,
            )

    def _query_rows_with_retry(self, query: str) -> List[Dict[str, Any]]:
        """A read-only query, retried on transient failure.

        The shared connection retries only queries whose text contains
        ACCOUNT_USAGE *and* that fail with a permission error, so SHOW OPENFLOW
        matches neither and the load-bearing inventory fetch would otherwise be
        the one unprotected call in this source.

        Deliberately a separate method rather than retry inside _query_rows.
        The stage GET goes through _query_rows too, and its retry has to
        recreate the temporary directory it downloads into -- see
        _download_connector_config, where a partial file from a failed attempt
        must not be visible to the next one. Retrying below that level would
        re-run the GET inside the same directory and defeat it. So each caller
        retries at the layer where its own invariants hold, and no call is
        wrapped twice.
        """
        return self._retrying()(self._query_rows, query)

    # Instance state, set in __init__ like every other attribute here. The
    # class-level default exists ONLY because the unit-test harnesses build a
    # source via object.__new__ to avoid opening a real Snowflake connection,
    # so __init__ never runs for them. Removing it costs an identical
    # assignment in five separate test helpers and buys no behaviour: the
    # value is an immutable bool, never mutated through the class.
    _fetch_connector_urls: bool = True

    @cached_property
    def _filtered_parent_runtimes(self) -> Set[Optional[str]]:
        """Runtimes whose DEPLOYMENT the operator filtered out.

        Per-instance and lazily created for the same reason as _canvas_urls: a
        mutable class-level default would be shared by every source, including
        the test harnesses that build one via object.__new__.
        """
        return set()

    @cached_property
    def _canvas_urls(self) -> Dict[Tuple[str, str, str], str]:
        """Canvas URL per runtime, for this run only.

        NOT a class-level default like `_fetch_connector_urls`. That one is a
        bool, so every write rebinds and instances cannot interfere; a dict is
        mutated in place, so a class-level one would be shared by every source
        -- and by every test built via object.__new__, which is how that was
        caught. cached_property gives per-instance state without depending on
        __init__, which those same harnesses skip.
        """
        return {}

    def _decide_url_lookup(self, connector_count: int, runtime_count: int) -> None:
        """Resolve the tri-state URL option, once per run, into one boolean.

        `None` means auto: fetch unless the account is large enough that the
        extra round trip per connector would dominate. `True`/`False` are the
        operator's explicit choice and are honoured at any size.
        """
        choice = self.config.include_connector_external_url
        if choice is not None:
            self._fetch_connector_urls = choice
            return
        # Gated on RUNTIMES, not connectors: with the per-runtime cache the
        # DESCRIBE count is one per distinct runtime, so a 5000-connector
        # account spread over 20 runtimes costs 20 queries and must not be
        # degraded. Only an account with a pathological number of near-empty
        # runtimes reaches the bound.
        if runtime_count <= _MAX_RUNTIMES_FOR_URL_LOOKUP:
            return
        self._fetch_connector_urls = False
        self.report.num_connector_urls_skipped_for_scale = connector_count
        # message stays a literal so warnings aggregate; the varying counts go
        # in context, which is what every other warning in this file does.
        self.report.warning(
            title="Connector external links skipped",
            message="This account has more distinct Openflow runtimes than "
            "the threshold at which the per-runtime DESCRIBE needed for each "
            "external link would dominate the run, so no external links are "
            "emitted. Set include_connector_external_url: true to fetch them "
            "anyway.",
            context=f"{runtime_count} distinct runtimes across "
            f"{connector_count} connectors, threshold "
            f"{_MAX_RUNTIMES_FOR_URL_LOOKUP} runtimes",
        )

    def _retrying(self) -> Retrying:
        # Shared by both per-connector network calls. They fail the same way --
        # a transient blip on either drops that connector's contribution for the
        # whole run, since neither is revisited.
        return Retrying(
            retry=retry_if_exception_type(_RETRYABLE_SNOWFLAKE_ERRORS),
            stop=stop_after_attempt(_RETRY_MAX_ATTEMPTS),
            wait=wait_exponential(multiplier=_RETRY_BACKOFF_MULTIPLIER, max=4),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def _read_connector_url(self, connector: OpenflowConnector) -> Optional[str]:
        """The connector's NiFi canvas deep link, or None.

        A missing link degrades the flow by one aspect field; it never costs
        lineage. So every failure here is counted and, at worst, warned -- it
        must not abort the connector the way a lineage failure would.
        """
        if not self._fetch_connector_urls:
            return None
        # connector.location is the scope this DESCRIBE's answer is valid in:
        # runtime names are scoped to their deployment, so the schema has to be
        # part of the key or one deployment's connectors get another's canvas
        # host. The gate that budgets these queries counts the same thing.
        cache_key = connector.location
        cached = self._canvas_urls.get(cache_key) if cache_key else None
        if cached is not None:
            return cached
        fqn = connector.fqn
        if fqn is None:
            # Both surfaces carry DATABASE_NAME and SCHEMA_NAME -- measured;
            # an earlier version of this comment claimed only SHOW did -- so a
            # connector reaches here only when neither supplied them, which in
            # practice means a row too sparse to address. Counted rather than
            # warned: for a dropped connector this is the expected steady
            # state, not a fault.
            self.report.num_connectors_without_fqn += 1
            return None
        try:
            rows = self._retrying()(
                self._query_rows, SnowflakeOpenflowQuery.describe_connector(fqn)
            )
        except Exception as exc:
            self.report.num_connector_urls_failed += 1
            self.report.warning(
                title="Could not read connector URL",
                message="DESCRIBE OPENFLOW CONNECTOR failed, so this connector "
                "has no external link. Everything else about it is unaffected. "
                "Set include_connector_external_url: false to skip these queries.",
                context=connector.key,
                exc=exc,
            )
            return None
        # Both remaining branches mean DESCRIBE answered but not with what its
        # contract promises -- an empty result, or a row without the column.
        # Neither is an operator mistake, so neither is silent: they are the
        # shape a Snowflake-side surface change would take, and a silently
        # absent link is indistinguishable from a connector that simply has
        # none.
        reported = get_str(rows[0], COL_CONNECTOR_URL) if rows else None
        url = _canvas_url(reported) if reported else None
        if url is None:
            self.report.num_connector_urls_failed += 1
            self.report.warning(
                title="Connector URL missing from DESCRIBE",
                message="DESCRIBE OPENFLOW CONNECTOR succeeded but returned "
                "no CONNECTOR_URL, so this connector has no external link. "
                "Everything else about it is unaffected.",
                context=connector.key,
            )
        if url is not None:
            # Successes only. Caching a transient DESCRIBE failure against this
            # runtime would deny the link to every sibling connector processed
            # afterwards, trading N-plus-one for a correctness regression.
            if cache_key is not None:
                self._canvas_urls[cache_key] = url
        return url

    def _read_connector_config(
        self, connector: OpenflowConnector
    ) -> Optional[Dict[str, Any]]:
        if not connector.version_location_uri:
            # Counted and warned, not silent. SHOW does not carry this column, so a
            # connector reaches here when the history side could not supply it --
            # including the case the SHOW-authority rule newly creates: a live
            # connector whose only history row is closed (what the view's ~20-minute
            # lag produces just after a drop and re-create) now contributes nothing,
            # so the URI is absent and lineage would vanish with nothing to see.
            self.report.num_connectors_without_config_uri += 1
            self.report.warning(
                title="Connector has no config location",
                message="No version_location_uri is available for this connector, so "
                "its configuration cannot be read and no lineage is derived for it. "
                "The ACCOUNT_USAGE views lag by roughly 20 minutes, so a connector "
                "created or re-created very recently may resolve on the next run.",
                context=connector.key,
            )
            return None
        try:
            # One bounded retry per connector, around the download only. A
            # single transient blip would otherwise drop this connector's
            # lineage for the whole run: the failure is caught below, counted,
            # and never revisited.
            return self._retrying()(
                self._download_connector_config, connector.version_location_uri
            )
        except Exception as exc:
            self.report.num_config_reads_failed += 1
            self.report.warning(
                title="Could not read connector configuration",
                message="Lineage for this connector is skipped. This can mean the "
                "role lacks READ on the connector's version stage, the download "
                "failed, or the file was not valid JSON.",
                context=connector.key,
                exc=exc,
            )
            return None

    def _download_connector_config(self, version_location_uri: str) -> Dict[str, Any]:
        # One attempt. Each retry gets its OWN temporary directory, so a
        # partial file left behind by a failed GET can never be picked up as
        # the config by the attempt that follows it.
        with tempfile.TemporaryDirectory(
            prefix="openflow-connector-config-"
        ) as local_dir:
            # GET, not `SELECT $1 FROM stage`: SELECT parses the file under
            # Snowflake's default CSV file format, so $1 is only the text up
            # to the first comma. Confirmed against a live connector's
            # config.json (2921 bytes): $1 silently returned 24 bytes. An
            # inline FILE_FORMAT=>(TYPE=JSON) argument is rejected as
            # non-constant, and a named file format is DDL a read-only
            # metadata role should not need. GET has no such assumption --
            # it downloads the file whole.
            #
            # GET's result rows (filename/size/status) are discarded; they're
            # an audit trail, not the content. The file is read back from
            # `local_dir` below. Everything GET wrote there -- the config
            # included -- is removed the moment this `with` block exits,
            # success or failure, because it is a `tempfile.TemporaryDirectory`
            # rather than a path this method chooses and cleans up itself.
            # Nothing sensitive is at risk regardless: the config carries
            # SECRET_REFERENCE placeholders rather than literal secret
            # values (confirmed against a live connector's file), never
            # resolved or logged anywhere in this method.
            self._query_rows(
                SnowflakeOpenflowQuery.get_stage_file_to_local(
                    version_location_uri, CONFIG_FILENAME, local_dir
                )
            )
            downloaded = list(pathlib.Path(local_dir).iterdir())
            if not downloaded:
                raise RuntimeError("GET reported no error but produced no local file")
            return decode_config_payload(downloaded[0].read_bytes())

    def _edge_within_urn_limits(
        self, pair: ConnectorTableLineage, connector: OpenflowConnector
    ) -> Optional[ConnectorTableLineage]:
        """The edge with any over-long endpoint removed, or None if unusable.

        A dataset urn here is FOREIGN -- this source does not create those
        datasets, it points at what the warehouse ingestion emitted -- so
        unlike a connector name it cannot be shortened: a shortened urn joins
        to nothing. Three 255-character Snowflake identifiers already make an
        839-byte urn, and CJK identifiers reach the thousands, so the only
        honest options are to skip and say so, or to emit an aspect the server
        discards.

        The outlet IS the edge, so an over-long one costs the whole job. An
        over-long inlet costs only the upstream half, which is the same
        degradation the existing inlet-skipped counter already describes.
        """
        if not urn_fits(pair.outlet):
            self.report.num_urns_too_long += 1
            self.report.warning(
                title="Lineage edge skipped: destination urn too long",
                message=(
                    "The destination dataset's urn exceeds what DataHub "
                    "accepts, and it cannot be shortened without pointing at a "
                    "dataset that does not exist. The edge is skipped rather "
                    "than emitted for the server to discard."
                ),
                context=f"{connector.key}: {encoded_urn_len(pair.outlet)} bytes",
            )
            return None
        if pair.inlet is not None and not urn_fits(pair.inlet):
            self.report.num_upstream_inlets_skipped += 1
            self.report.warning(
                title="Upstream dropped: source urn too long",
                message=(
                    "The upstream dataset's urn exceeds what DataHub accepts, "
                    "so the downstream half of this edge is emitted without it."
                ),
                context=f"{connector.key}: {encoded_urn_len(pair.inlet)} bytes",
            )
            return dataclasses.replace(pair, inlet=None)
        return pair

    def _urn_is_emittable(self, urn: object, key: str, kind: str) -> bool:
        """Whether this urn is short enough for GMS, reported if it is not.

        _fitted shortens until the urn fits and, if even the digest-only
        candidate does not, returns it anyway -- there is nothing shorter it
        can try. That last case used to return silently, in a source that
        counts or warns on every other kind of loss, so an aspect GMS discards
        looked identical to one it stored. Config validation makes it
        unreachable for the known cause (an oversized platform_instance), which
        is exactly why it must say something if it is ever reached: it would
        mean a cause nobody has thought of.
        """
        if urn_fits(urn):
            return True
        self.report.num_urns_too_long += 1
        self.report.warning(
            title="Entity skipped: urn too long",
            message=(
                "Even the shortest id this source can generate produced a urn "
                "over the limit DataHub accepts, so the entity is skipped "
                "rather than emitted for the server to discard. Something "
                "other than the connector name is consuming the budget."
            ),
            context=f"{kind} for {key}: {encoded_urn_len(urn)} bytes",
        )
        return False

    def _lineage_for_connector(
        self, connector: OpenflowConnector
    ) -> List[ConnectorTableLineage]:
        config_json = self._read_connector_config(connector)
        if config_json is None:
            return []
        lineage = parse_connector_config(config_json)
        if lineage.unparseable_tables:
            # Reuse num_lineage_edges_skipped rather than adding a counter: the
            # operator-visible fact is the same, an edge we could not build.
            self.report.num_lineage_edges_skipped += len(lineage.unparseable_tables)
            self.report.warning(
                title="Unparseable source table name",
                message="These entries carried no schema qualifier, so no upstream "
                "table could be derived and their lineage is omitted.",
                context=f"{connector.key}: {lineage.unparseable_tables}",
            )
        if lineage.unrecognised_source_url_keys:
            self.report.warning(
                title="Source connection URL property not recognised",
                message="The connector's Source section carried none of the known "
                "connection-URL property names, so no upstream dataset could be "
                "derived. Downstream lineage is still emitted.",
                context=f"{connector.key}: {lineage.unrecognised_source_url_keys}",
            )
        if not lineage.destination_database:
            # The only branch here that used to return empty-handed in silence,
            # while every sibling counts or warns. It also fires BEFORE the
            # table-pattern check, so it shadowed that counter. If Snowflake
            # renames the destination section or the property becomes a secret
            # reference, every connector in the account loses lineage at once --
            # this is what makes that visible rather than a clean empty report.
            self.report.num_connectors_without_destination_database += 1
            self.report.warning(
                title="Connector has no destination database",
                message="The connector's configuration did not name a "
                "destination Snowflake database, so no lineage is derived for "
                "it. If this affects every connector at once, the property "
                "names this source reads have most likely changed.",
                context=connector.key,
            )
            return []
        if not lineage.source_tables:
            if lineage.unparseable_tables:
                # The configuration DID name tables; they just carried no schema
                # qualifier, which is already counted and warned above. Falling
                # through to the branch below would tell the operator the
                # connector "listed neither table names nor a table pattern",
                # which is false and sends them looking at the wrong property.
                pass
            elif lineage.table_pattern:
                # A pattern explains the emptiness: the tables are chosen at
                # run time and the configuration cannot enumerate them. Counted
                # silently -- this is the documented steady state, not a fault.
                self.report.num_connectors_without_enumerable_tables += 1
            else:
                # No tables AND no pattern. Nothing in the configuration says
                # which tables this connector replicates, which is what a
                # renamed property looks like -- and if the name changed, every
                # connector in the account loses lineage at once.
                self.report.num_connectors_without_table_configuration += 1
                self.report.warning(
                    title="Connector names no tables to replicate",
                    message="The connector's configuration listed neither "
                    "table names nor a table pattern, so no lineage is derived "
                    "for it. If this affects every connector at once, the "
                    "property names this source reads have most likely changed.",
                    context=connector.key,
                )
            return []

        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=self.config.get_snowflake_identifier_config(),
            structured_reporter=self.report,
        )
        upstream = CONNECTOR_DEFINITION_PLATFORM.get(
            connector.connector_definition or ""
        )
        if upstream is None and lineage.source_tables:
            # Guessing a name for an unmapped definition is what produces a
            # well-formed URN naming a dataset that cannot exist, which nothing
            # downstream reports. Warn instead; destination lineage still flows.
            self.report.warning(
                title="Unsupported connector definition for upstream lineage",
                message="No upstream platform and dataset-name shape are known for "
                "this connector definition, so no upstream dataset could be derived. "
                "Downstream lineage is still emitted.",
                context=f"{connector.key}: {connector.connector_definition!r}",
            )
        pairs: List[ConnectorTableLineage] = []
        for source_schema, source_table in lineage.source_tables:
            destination = destination_identifier(
                lineage.destination_database,
                source_schema,
                source_table,
                lineage.schema_strategy,
            )
            if destination is None:
                self.report.num_lineage_edges_skipped += 1
                self.report.warning(
                    title="Unrecognised destination schema strategy",
                    message="This Destination Schema Strategy is not implemented, so "
                    "the destination table cannot be derived. Lineage is skipped "
                    "rather than guessed.",
                    context=f"{connector.key}: strategy={lineage.schema_strategy!r}",
                )
                continue
            # `destination` (from destination_identifier, above) is the single
            # formula for the destination's dotted identifier -- it is fed
            # straight into the URN rather than recomputed via
            # get_dataset_identifier's own db/schema/table formula, so that
            # adding a Prefix/Suffix strategy later only ever needs a change
            # in one place.
            # Bound before the upstream branch, which is skipped entirely for an
            # unmapped connector definition -- the destination half still stands.
            inlet: Optional[str] = None
            outlet = identifiers.gen_dataset_urn(
                identifiers.snowflake_identifier(destination)
            )
            if upstream is not None:
                # `upstream_identifier` is the single formula for the upstream's
                # dotted name, and it is tier-aware: which of source_database /
                # source_schema it consumes depends on the platform, so the name
                # is never recomposed here.
                upstream_name = upstream_identifier(
                    upstream,
                    lineage.source_database,
                    source_schema,
                    source_table,
                )
                if upstream_name is not None:
                    source_instance = self.config.source_platform_instance
                    if self.config.source_convert_urns_to_lowercase:
                        # Fold the identifier AND the platform_instance prefix.
                        #
                        # An earlier revision folded only the identifier, reasoning
                        # that this matched the upstream source's own in-source
                        # folding. It does not, and there is no upstream shape in
                        # which it would. postgres/mysql/mssql all default
                        # convert_urns_to_lowercase to False, so this flag is only
                        # correct to set when the upstream recipe spells the key out
                        # explicitly -- and an explicitly present key is exactly what
                        # AutoLowercaseUrnsProcessor.should_enable gates on, so that
                        # recipe ALSO gets the pipeline-level pass. That pass calls
                        # lowercase_dataset_urn, which rebuilds the urn with
                        # `name.lower()` over the WHOLE name, and the name composed by
                        # make_dataset_urn_with_platform_instance is
                        # `<instance>.<identifier>`. Folding half of it emitted
                        # `PG_Prod.db.schema.table` where that recipe wrote
                        # `pg_prod.db.schema.table` -- a well-formed urn joining to
                        # nothing, which renders identically to a real one.
                        upstream_name = upstream_name.lower()
                        if source_instance:
                            source_instance = source_instance.lower()
                    inlet = make_dataset_urn_with_platform_instance(
                        platform=upstream.platform,
                        name=upstream_name,
                        platform_instance=source_instance,
                        # default_source_env_to_env guarantees source_env is set;
                        # the fallback keeps that guarantee visible to mypy.
                        env=self.config.source_env or self.config.env,
                    )
                else:
                    # The upstream half is the reason to connect Openflow to a
                    # source system at all, so losing it must not look like a
                    # complete edge. _jdbc_database returns None when the URL is
                    # not JDBC, names no database, or spells the database in a
                    # property this does not recognise -- all invisible until now,
                    # because the old unconditional prefix strip produced a
                    # wrong-but-present value instead.
                    inlet = None
                    self.report.num_upstream_inlets_skipped += 1
                    self.report.warning(
                        title="Upstream dataset could not be identified",
                        message="The connector's source URL did not yield the "
                        "database name this upstream platform needs, so the edge "
                        "is emitted with its destination only. If the source URL "
                        "spells the database in a way this connector does not "
                        "recognise, the upstream half of every table it "
                        "replicates is missing.",
                        context=f"{connector.key}: source URL of the form "
                        f"{_url_shape(lineage.source_url)!r}",
                    )
            pairs.append(
                ConnectorTableLineage(
                    source_schema=source_schema,
                    source_table=source_table,
                    outlet=outlet,
                    inlet=inlet,
                )
            )
            self.report.num_lineage_edges += 1
        return pairs

    def _paged_history(
        self, builder: Callable[[Optional[str]], str], object_type: str
    ) -> List[Dict[str, Any]]:
        # Cursor pagination on CREATED_ON. Three things here are deliberate; the
        # first two are guards against defects demonstrated before this was written.
        rows: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        pages = 0
        while True:
            page = self._query_rows_with_retry(builder(cursor))
            if not page:
                break
            rows.extend(page)
            pages += 1
            if len(page) < SnowflakeOpenflowQuery.PAGE_SIZE:
                break

            # Through get_col like every other column read in this package. This
            # was the one place reading an exact uppercase key, in a module whose
            # accessor exists precisely because the views' casing is not
            # guaranteed -- and it is the read that decides whether pages 2..N
            # are fetched at all, so a miss here silently truncates the whole
            # history to one page while blaming the data for being NULL.
            next_created_on = get_col(page[-1], COL_CREATED_ON)
            if next_created_on is None:
                # Guard 1: an absent or NULL CREATED_ON on the page boundary would
                # make the cursor the literal string "None", and the next predicate
                # `WHERE CREATED_ON >= 'None'` is nonsense rather than an error.
                self.report.warning(
                    title="Cannot paginate past a missing CREATED_ON",
                    message="A full page ended with a row whose CREATED_ON is "
                    "NULL or absent from the view, so the cursor cannot advance. "
                    "Results may be incomplete.",
                )
                break
            next_cursor = str(next_created_on)
            if next_cursor == cursor:
                # Guard 2: every row on a full page shares one timestamp, so the
                # cursor cannot advance without skipping the whole tie group.
                # Stopping beats looping forever on the same page.
                self.report.warning(
                    title="Pagination stalled on identical timestamps",
                    message="A full page of rows shares a single CREATED_ON, so the "
                    "cursor cannot advance. Results may be incomplete.",
                )
                break
            cursor = next_cursor

        if pages > 1:
            # Informational, not a warning. These queries page over CUMULATIVE
            # LIFECYCLE rows -- DELETED_ON is deliberately unfiltered, because the
            # deleted rows are the deletion-detection signal -- so an account with
            # even modest churn exceeds PAGE_SIZE within the view's retention
            # window while holding only a handful of live objects. Multi-page is
            # therefore the ordinary case, not a scale alarm. An earlier revision
            # of this comment argued the opposite from the live-object count and
            # called the branch unreachable; that reasoning weighed the wrong
            # population. The boundary-row loss it warned about is closed by the
            # inclusive cursor in snowflake_openflow_query.py.
            self.report.num_history_pages_beyond_first += pages - 1
            if pages > _HISTORY_PAGES_BEFORE_WARNING:
                self.report.warning(
                    title="History view is unusually large",
                    # Constant message, varying part in context -- three views
                    # with three different page counts must aggregate to one
                    # structured-log entry, not three. Same rule as
                    # EMPTY_INVENTORY_MESSAGE in the report module.
                    message=LARGE_HISTORY_MESSAGE,
                    context=f"{object_type}: {pages} pages of "
                    f"{SnowflakeOpenflowQuery.PAGE_SIZE}",
                )
        return rows

    def _parse_rows(
        self,
        model: Type[RowModel],
        rows: List[Dict[str, Any]],
        object_type: str,
        surface: str,
    ) -> List[RowModel]:
        """Rows parsed into models, counting and reporting the ones that cannot be.

        from_row returns None when the column carrying the object's identity is
        absent. Dropping those silently is how a healthy object disappears: it is
        then missing from the stale-entity checkpoint, so the next run
        SOFT-DELETES it, and the operator sees a green run with no warnings. A
        total parse failure is already caught downstream by the empty-inventory
        report; a partial one is invisible without this.
        """
        parsed: List[RowModel] = []
        dropped = 0
        unparseable_shapes: Set[str] = set()
        for row in rows:
            item = model.from_row(row)
            if item is None:
                dropped += 1
                continue
            # A CREATED_ON that is PRESENT but did not parse is the dangerous
            # case, and it is only visible here, where the raw row and the model
            # are both in hand: get_datetime collapses "absent" and
            # "unparseable" into the same None. Unparsed rows sort at the epoch,
            # epochs tie, and a tie resolves CLOSED over OPEN -- so a format
            # change alone can report objects deleted.
            raw_created_on = get_col(row, COL_CREATED_ON)
            if raw_created_on is not None and item.created_at is None:
                unparseable_shapes.add(timestamp_shape(str(raw_created_on)))
            parsed.append(item)
        if unparseable_shapes:
            self.report.num_unparseable_timestamps += len(unparseable_shapes)
            self.report.warning(
                title="Timestamp rendering not understood",
                message=(
                    "CREATED_ON was present but could not be parsed, so those "
                    "rows cannot be ordered against each other. Ordering decides "
                    "which lifecycle row wins, and an unordered row is resolved "
                    "as deleted, so affected objects may be reported deleted. "
                    "Most likely TIMESTAMP_OUTPUT_FORMAT was changed for this "
                    "session, user or account."
                ),
                context=f"{object_type} ({surface}): {sorted(unparseable_shapes)}",
            )
        if dropped:
            self.report.num_rows_missing_identity += dropped
            self.report.warning(
                title="Rows skipped: identity column missing",
                message=(
                    "Rows were dropped because the column carrying the object's "
                    "identity was absent, most likely a renamed or unexpectedly "
                    "cased Snowflake column. With stateful ingestion enabled the "
                    "affected objects are treated as deleted."
                ),
                context=f"{object_type} ({surface}): {dropped} row(s)",
            )
        return parsed

    def _fetch_inventory(
        self,
        *,
        model: Type[RowModel],
        show_query: str,
        history_query: Callable[[Optional[str]], str],
        object_type: str,
        pattern: AllowDenyPattern,
        on_dropped: Callable[[str], None],
    ) -> List[RowModel]:
        """SHOW + paged history, merged, filtered to live, then pattern-filtered.

        One algorithm for all three object types. It was three copies, which is
        three places for a fix to the deleted_on filter or the mixed-lifecycle
        accounting to land and two chances to miss one.

        RowModel is a value-constrained TypeVar, so mypy re-checks this body
        once per member rather than erasing to a common base -- the three
        models share no base class, only the shape used here.
        """
        show_rows = self._query_rows_with_retry(show_query)
        self._fail_if_show_truncated(show_rows, object_type)
        show = self._parse_rows(model, show_rows, object_type, "SHOW")
        history = self._parse_rows(
            model,
            self._paged_history(history_query, object_type),
            object_type,
            "ACCOUNT_USAGE",
        )
        merged, mixed_keys = merge_show_and_history(show, history)
        self.report.num_keys_with_mixed_lifecycle_rows += mixed_keys
        live = [row for row in merged if row.deleted_on is None]
        treated_as_deleted = len(merged) - len(live)
        if treated_as_deleted:
            # A history-only key is EITHER genuinely deleted OR invisible to
            # SHOW for privilege reasons -- merge_show_and_history's own
            # comment says so, and the two are indistinguishable here. With
            # DELETION_DETECTION on, 'this run declared N objects deleted' is
            # the one number worth checking before the checkpoint commits.
            self.report.num_objects_treated_as_deleted += treated_as_deleted
            # info, not warning: deletions are the ordinary case for a source
            # that declares DELETION_DETECTION, and an account that deletes
            # anything would otherwise carry a warning it can never clear --
            # which is how warnings stop being read. The number is what matters.
            self.report.info(
                title="Objects reported deleted",
                message=(
                    "These objects were absent from SHOW and carried a "
                    "DELETED_ON in the history view, so they are treated as "
                    "deleted and, with stateful ingestion enabled, "
                    "soft-deleted. A lost MONITOR grant looks identical to a "
                    "deletion here, so check the count is what you expect."
                ),
                context=f"{object_type}: {treated_as_deleted}",
            )
        if not live:
            self.report.report_empty_inventory(object_type)
        return [
            row
            for row in live
            if self._allowed(pattern, row.name or row.key, on_dropped)
        ]

    def _fetch_deployments(self) -> List[OpenflowDeployment]:
        return self._fetch_inventory(
            model=OpenflowDeployment,
            show_query=SnowflakeOpenflowQuery.show_deployments(),
            history_query=SnowflakeOpenflowQuery.deployment_history,
            object_type="deployments",
            pattern=self.config.deployment_pattern,
            on_dropped=self.report.report_dropped_deployment,
        )

    def _fetch_runtimes(self) -> List[OpenflowRuntime]:
        return self._fetch_inventory(
            model=OpenflowRuntime,
            show_query=SnowflakeOpenflowQuery.show_runtimes(),
            history_query=SnowflakeOpenflowQuery.runtime_history,
            object_type="runtimes",
            pattern=self.config.runtime_pattern,
            on_dropped=self.report.report_dropped_runtime,
        )

    def _fetch_connectors(self) -> List[OpenflowConnector]:
        # Gen 1 connectors are not SQL objects at all, so this surface sees only
        # Gen 2. An account running Gen 1 exclusively looks empty here, and the
        # count of omitted Gen 1 connectors is not observable.
        return self._fetch_inventory(
            model=OpenflowConnector,
            show_query=SnowflakeOpenflowQuery.show_connectors(),
            history_query=SnowflakeOpenflowQuery.connector_history,
            object_type="connectors",
            pattern=self.config.connector_pattern,
            on_dropped=self.report.report_dropped_connector,
        )

    @staticmethod
    def _allowed(
        pattern: AllowDenyPattern, name: str, on_dropped: Callable[[str], None]
    ) -> bool:
        if pattern.allowed(name):
            return True
        on_dropped(name)
        return False

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        deployments = self._fetch_deployments()
        runtimes = self._fetch_runtimes()
        # Keyed on the name only when there IS one: an unnamed deployment would
        # otherwise occupy the None key, and a runtime whose deployment_name is
        # also None would match it and nest under an unrelated parent. The
        # runtime map below guards the same way, for the same reason.
        # No ambiguity guard here, unlike the runtime map below, and the
        # asymmetry is deliberate: SHOW OPENFLOW RUNTIMES and SHOW OPENFLOW
        # CONNECTORS both return database_name/schema_name while SHOW OPENFLOW
        # DEPLOYMENTS returns neither, so deployments are account-scoped and
        # their names are unique. Two schemas can each hold a runtime called
        # `default`; two deployments cannot share a name.
        by_deployment_name = {
            deployment.name: deployment for deployment in deployments if deployment.name
        }

        # Parents before children so auto_browse_path_v2 can build browse paths.
        for deployment in deployments:
            self.report.num_deployments += 1
            yield from gen_containers(
                container_key=self._deployment_key(deployment),
                name=deployment.display_name or deployment.name or deployment.key,
                sub_types=[GenericContainerSubTypes.OPENFLOW_DEPLOYMENT],
                owner_urn=_owner_group_urn(deployment.owner),
                ownership_type=OWNERSHIP_TYPE,
                extra_properties=self._deployment_properties(deployment),
            )
            if deployment.owner:
                self.report.num_owners_emitted += 1

        # Populated as runtime containers are emitted, then read by the connector
        # loop below, so a connector's DataFlow can only ever point at a container
        # this run actually emitted. Keyed on the runtime NAME because that is what
        # a connector row carries (SHOW's `runtime` / the view's RUNTIME_NAME) --
        # the opaque RUNTIME_KEY that keys the container is not on connector rows.
        runtime_keys_by_name: Dict[str, OpenflowRuntimeKey] = {}
        # Runtime names are scoped to their DEPLOYMENT, not to the account, so two
        # deployments may each hold a runtime called `default`. A connector row
        # carries only the runtime NAME, never a deployment, so such a name cannot
        # be resolved to one runtime and any choice would be a guess. Names seen
        # more than once are recorded here and removed from the map, turning a
        # silent mis-nesting into a counted miss -- the same trade-off the case
        # comment below already makes, applied to the likelier collision.
        ambiguous_runtime_names: Set[str] = set()

        for runtime in runtimes:
            parent_deployment = (
                by_deployment_name.get(runtime.deployment_name)
                if runtime.deployment_name
                else None
            )
            if parent_deployment is None:
                if (
                    runtime.deployment_name
                    and not self.config.deployment_pattern.allowed(
                        runtime.deployment_name
                    )
                ):
                    # The operator excluded this deployment, so its runtimes
                    # arriving un-nested is the requested outcome -- not a
                    # missing grant. Telling them to fix a grant that is
                    # already correct produces a warning they cannot ever
                    # clear, which is how a warning stops being read. Same
                    # waiver shape as _report_connector_without_runtime_parent
                    # applies one level down for runtime_pattern.
                    self._filtered_parent_runtimes.add(runtime.name)
                    # Counted as well as waived. Cascading a deployment
                    # exclusion to its runtimes is intended, but an operator
                    # who adds one deny rule and watches a set of runtimes
                    # disappear needs a number that says so.
                    self.report.report_dropped_runtime(runtime.key)
                else:
                    self.report.num_runtimes_without_deployment_parent += 1
                    self.report.warning(
                        title="Runtime skipped: no visible parent deployment",
                        message="The runtime is SKIPPED ENTIRELY rather than "
                        "emitted without a parent: its container URN embeds the "
                        "deployment key, so there is no un-nested URN to emit "
                        "that would not duplicate the runtime once the "
                        "deployment becomes visible. With stateful ingestion a "
                        "previously-ingested copy is therefore soft-deleted. "
                        "Grant MONITOR on the deployment.",
                        context=runtime.key,
                    )
                continue
            self.report.num_runtimes += 1
            runtime_key = self._runtime_key(runtime, parent_deployment)
            if runtime.name:
                if runtime.name in runtime_keys_by_name:
                    ambiguous_runtime_names.add(runtime.name)
                    del runtime_keys_by_name[runtime.name]
                elif runtime.name not in ambiguous_runtime_names:
                    runtime_keys_by_name[runtime.name] = runtime_key
            yield from gen_containers(
                container_key=runtime_key,
                name=runtime.display_name or runtime.name or runtime.key,
                sub_types=[GenericContainerSubTypes.OPENFLOW_RUNTIME],
                parent_container_key=self._deployment_key(parent_deployment),
                owner_urn=_owner_group_urn(runtime.owner),
                ownership_type=OWNERSHIP_TYPE,
                extra_properties=self._runtime_properties(runtime),
            )
            if runtime.owner:
                self.report.num_owners_emitted += 1

        connectors = self._fetch_connectors()
        self._decide_url_lookup(
            len(connectors),
            # The DESCRIBE is issued once per location, so that -- not the
            # bare runtime name -- is what the budget has to count. Counting
            # names would under-estimate whenever one runtime name appears in
            # more than one schema, and could never over-estimate.
            len({connector.location for connector in connectors if connector.location}),
        )
        for connector in connectors:
            self.report.num_connectors += 1
            # A miss leaves the flow un-nested rather than dropped: SHOW OPENFLOW
            # CONNECTORS is account-wide, while runtimes are both privilege-filtered
            # and runtime_pattern-filtered, so a connector can legitimately name a
            # runtime this run never emitted.
            #
            # Matched EXACTLY, case included, and deliberately so. Both surfaces
            # report the identifier as Snowflake stored it rather than re-casing
            # it: a runtime created under a quoted, mixed-case name comes back in
            # that same mixed case from SHOW OPENFLOW RUNTIMES (`name`), SHOW
            # OPENFLOW CONNECTORS (`runtime`) and OPENFLOW_CONNECTOR_HISTORY
            # (`RUNTIME_NAME`) alike -- measured on a live account, where a fold
            # by either surface would have shown up on exactly such a name. The
            # only lowercasing observed anywhere is in the derived RUNTIME_KEY
            # slug, which this lookup does not use. So a case-insensitive fold
            # added "just in case" would buy nothing and risk quietly nesting a
            # connector under the wrong one of two runtimes whose names differ
            # only in case (which quoted identifiers permit) -- worse than a
            # miss, now that the miss is counted.
            parent_runtime_key = runtime_keys_by_name.get(connector.runtime_name)
            if connector.runtime_name in ambiguous_runtime_names:
                self.report.num_connectors_with_ambiguous_runtime += 1
                self.report.warning(
                    title="Runtime name is not unique across deployments",
                    message="More than one deployment exposes a runtime with this "
                    "name, and a connector row carries no deployment, so the "
                    "connector cannot be attributed to one of them. It is emitted "
                    "without a parent runtime rather than nested under a guess.",
                    context=connector.runtime_name,
                )
            elif parent_runtime_key is None:
                self._report_connector_without_runtime_parent(connector)
            flow = build_connector_flow(
                connector,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                parent_container=parent_runtime_key,
                external_url=self._read_connector_url(connector),
            )
            if not self._urn_is_emittable(flow.urn, connector.key, "DataFlow"):
                continue
            yield from flow.as_workunits()
            if connector.owner:
                self.report.num_owners_emitted += 1
            # No connector-level DataJob. The DataFlow above already IS the
            # connector -- identical name, properties, ownership and external
            # link -- so an anchor job duplicated it as a task inside its own
            # pipeline, edgeless and sharing the per-table jobs' subtype.
            # fivetran keeps one because it hangs run history (DPIs) on it;
            # this source emits none, so the anchor carried nothing.
            if self.config.include_table_lineage:
                for pair in self._lineage_for_connector(connector):
                    # One job per replicated table, so each edge keeps the 1:1
                    # pairing this connector's configuration actually states.
                    emittable = self._edge_within_urn_limits(pair, connector)
                    if emittable is None:
                        continue
                    job = build_connector_table_job(connector, flow, emittable)
                    if not self._urn_is_emittable(job.urn, connector.key, "DataJob"):
                        continue
                    yield from job.as_workunits()
                    self.report.num_table_jobs += 1
                    if connector.owner:
                        # Per-table jobs carry the connector's owner too, so
                        # the counter has to see them or it under-reports the
                        # aspects its name promises.
                        self.report.num_owners_emitted += 1

    def _report_connector_without_runtime_parent(
        self, connector: OpenflowConnector
    ) -> None:
        self.report.num_connectors_without_runtime_parent += 1
        if connector.runtime_name in self._filtered_parent_runtimes:
            # Its runtime was skipped because the operator filtered that
            # runtime's DEPLOYMENT. Same waiver as the runtime_pattern case
            # below: requested, so counted rather than warned.
            return
        if not self.config.runtime_pattern.allowed(connector.runtime_name):
            # The operator asked for this runtime to be skipped, so its
            # connectors arriving un-nested is the requested outcome. Warning
            # on it every run is how a warning stops being read.
            #
            # The pattern is re-evaluated here rather than checking membership
            # of report.filtered_runtimes: that is a LossyList, which keeps only
            # the first handful of names, so an account filtering more runtimes
            # than the list holds would start warning about deliberately
            # filtered ones.
            return
        self.report.warning(
            title="Connector with no visible parent runtime",
            message="No runtime container was emitted for this connector's "
            "runtime, so the connector is not nested under it. The runtime is "
            "likely not visible to this role -- grant MONITOR on it.",
            context=f"{connector.key}: runtime={connector.runtime_name!r}",
        )

    @staticmethod
    def _deployment_properties(deployment: OpenflowDeployment) -> Dict[str, str]:
        properties = {"deployment_key": deployment.key}
        if deployment.status:
            properties["status"] = deployment.status
        return properties

    @staticmethod
    def _runtime_properties(runtime: OpenflowRuntime) -> Dict[str, str]:
        properties = {"runtime_key": runtime.key}
        if runtime.status:
            properties["status"] = runtime.status
        if runtime.execute_as_role:
            properties["execute_as_role"] = runtime.execute_as_role
        if runtime.object_database and runtime.object_schema:
            # The runtime OBJECT's own location. Explicitly not a data destination.
            properties["object_location"] = (
                f"{runtime.object_database}.{runtime.object_schema}"
            )
        return properties
