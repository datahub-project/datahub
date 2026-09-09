import dataclasses
import enum
import gzip
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
    Sequence,
    Set,
    Tuple,
    Type,
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
    OpenflowConnector,
    OpenflowDeployment,
    OpenflowRuntime,
    get_str,
    merge_show_and_history,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_query import (
    SnowflakeOpenflowQuery,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_report import (
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
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass
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
_MAX_RUNTIMES_FOR_URL_LOOKUP = 500

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
    base = urlparse(connector_url[: index + len(marker)])
    if not base.scheme or not base.hostname:
        return None
    # Snowflake reports the default port explicitly; its own UI omits it.
    host = base.hostname
    if base.port and not (base.scheme == "https" and base.port == 443):
        host = f"{host}:{base.port}"
    return f"{base.scheme}://{host}{base.path}"


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


def parse_connector_config(config_json: Dict[str, Any]) -> OpenflowLineage:
    # Iterate EVERY section. Descending into configuration[0] only finds the
    # destination on connectors that happen to list it first.
    lineage = OpenflowLineage()
    for section in config_json.get("configuration") or []:
        name = section.get("name")
        properties = section.get("properties") or {}
        if name == SECTION_SOURCE:
            source_url: Optional[str] = None
            for candidate_key in PROP_SOURCE_URL_CANDIDATES:
                source_url = property_value(properties, candidate_key)
                if source_url:
                    break
            if source_url:
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
    return DataFlow(
        name=connector.key,
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


def build_connector_job(
    connector: OpenflowConnector,
    flow: DataFlow,
    inlets: Sequence[str],
    outlets: Sequence[str],
) -> DataJob:
    return DataJob(
        name=connector.key,
        flow=flow,
        display_name=connector.display_name or connector.name,
        subtype=DataJobSubTypes.OPENFLOW_CONNECTOR_SYNC,
        custom_properties=_connector_properties(connector),
        inlets=list(inlets),
        outlets=list(outlets),
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
    "`include_openflow_lineage: false`",
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
    each connector a DataFlow holding a single DataJob. Ownership comes from the
    Snowflake object OWNER, which is a role, so it maps to a corpGroup.

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
        if not self.config.include_openflow_lineage:
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

    # Instance state, set in __init__ like every other attribute here. The
    # class-level default exists ONLY because the unit-test harnesses build a
    # source via object.__new__ to avoid opening a real Snowflake connection,
    # so __init__ never runs for them. Removing it costs an identical
    # assignment in five separate test helpers and buys no behaviour: the
    # value is an immutable bool, never mutated through the class.
    _fetch_connector_urls: bool = True

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
        # Keyed on the connector's schema PLUS the runtime name, never the
        # name alone. This file documents twice that runtime names are scoped
        # to their deployment rather than the account, so two deployments may
        # each hold a runtime called `default`; a bare-name key would hand one
        # deployment's connectors the other's canvas host. The schema is the
        # same scope the DESCRIBE itself is issued in, so it cannot be less
        # precise than the query whose answer it caches. A connector missing
        # either part is not cached at all rather than cached under a partial
        # key -- it also cannot be DESCRIBEd, so it returns below anyway.
        # Narrowed to a fully-populated tuple before use, so mypy needs no
        # suppression: a connector missing any part is simply not cached (it
        # also cannot be DESCRIBEd, and returns below).
        database, schema, runtime = (
            connector.database_name,
            connector.schema_name,
            connector.runtime_name,
        )
        cache_key = (
            (database, schema, runtime) if database and schema and runtime else None
        )
        cached = self._canvas_urls.get(cache_key) if cache_key else None
        if cached is not None:
            return cached
        fqn = connector.fqn
        if fqn is None:
            # Only SHOW carries DATABASE_NAME / SCHEMA_NAME, so a connector
            # known solely from the history view cannot be addressed by
            # DESCRIBE. Counted rather than warned: for a dropped connector
            # this is the expected steady state, not a fault.
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
            content = downloaded[0].read_bytes()
            # Whether a staged file arrives gzip-compressed depends on how
            # it was staged (Snowflake's AUTO_COMPRESS behaviour), not on
            # anything this connector controls. Detected via the gzip magic
            # bytes rather than trusting a ".gz" filename suffix, since the
            # suffix is a naming convention GET applies, not a guarantee.
            if content[:2] == b"\x1f\x8b":
                content = gzip.decompress(content)
            return json.loads(content)

    def _lineage_for_connector(
        self, connector: OpenflowConnector
    ) -> Tuple[List[str], List[str]]:
        config_json = self._read_connector_config(connector)
        if config_json is None:
            return [], []
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
            return [], []
        if lineage.table_pattern and not lineage.source_tables:
            self.report.num_connectors_without_enumerable_tables += 1
            return [], []

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
        inlets: List[str] = []
        outlets: List[str] = []
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
            outlets.append(
                identifiers.gen_dataset_urn(
                    identifiers.snowflake_identifier(destination)
                )
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
                    inlets.append(
                        make_dataset_urn_with_platform_instance(
                            platform=upstream.platform,
                            name=upstream_name,
                            platform_instance=source_instance,
                            # default_source_env_to_env guarantees source_env is set;
                            # the fallback keeps that guarantee visible to mypy.
                            env=self.config.source_env or self.config.env,
                        )
                    )
            self.report.num_lineage_edges += 1
        return inlets, outlets

    def _paged_history(
        self, builder: Callable[[Optional[str]], str]
    ) -> List[Dict[str, Any]]:
        # Cursor pagination on CREATED_ON. Three things here are deliberate; the
        # first two are guards against defects demonstrated before this was written.
        rows: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        pages = 0
        while True:
            page = self._query_rows(builder(cursor))
            if not page:
                break
            rows.extend(page)
            pages += 1
            if len(page) < SnowflakeOpenflowQuery.PAGE_SIZE:
                break

            next_created_on = page[-1].get("CREATED_ON")
            if next_created_on is None:
                # Guard 1: a NULL CREATED_ON on the page boundary would make the
                # cursor the literal string "None", and the next predicate
                # `WHERE CREATED_ON >= 'None'` is nonsense rather than an error.
                self.report.warning(
                    title="Cannot paginate past a NULL CREATED_ON",
                    message="A full page ended with a row whose CREATED_ON is NULL, "
                    "so the cursor cannot advance. Results may be incomplete.",
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
        return rows

    def _fetch_deployments(self) -> List[OpenflowDeployment]:
        show = [
            OpenflowDeployment.from_row(row)
            for row in self._query_rows(SnowflakeOpenflowQuery.show_deployments())
        ]
        history = [
            OpenflowDeployment.from_row(row)
            for row in self._paged_history(SnowflakeOpenflowQuery.deployment_history)
        ]
        merged, mixed_keys = merge_show_and_history(
            [row for row in show if row], [row for row in history if row]
        )
        self.report.num_keys_with_mixed_lifecycle_rows += mixed_keys
        live = [row for row in merged if row.deleted_on is None]
        if not live:
            self.report.report_empty_inventory("deployments")
        return [
            row
            for row in live
            if self._allowed(
                self.config.deployment_pattern,
                row.name or row.key,
                self.report.report_dropped_deployment,
            )
        ]

    def _fetch_runtimes(self) -> List[OpenflowRuntime]:
        show = [
            OpenflowRuntime.from_row(row)
            for row in self._query_rows(SnowflakeOpenflowQuery.show_runtimes())
        ]
        history = [
            OpenflowRuntime.from_row(row)
            for row in self._paged_history(SnowflakeOpenflowQuery.runtime_history)
        ]
        merged, mixed_keys = merge_show_and_history(
            [row for row in show if row], [row for row in history if row]
        )
        self.report.num_keys_with_mixed_lifecycle_rows += mixed_keys
        live = [row for row in merged if row.deleted_on is None]
        if not live:
            self.report.report_empty_inventory("runtimes")
        return [
            row
            for row in live
            if self._allowed(
                self.config.runtime_pattern,
                row.name or row.key,
                self.report.report_dropped_runtime,
            )
        ]

    def _fetch_connectors(self) -> List[OpenflowConnector]:
        show = [
            OpenflowConnector.from_row(row)
            for row in self._query_rows(SnowflakeOpenflowQuery.show_connectors())
        ]
        history = [
            OpenflowConnector.from_row(row)
            for row in self._paged_history(SnowflakeOpenflowQuery.connector_history)
        ]
        merged, mixed_keys = merge_show_and_history(
            [row for row in show if row], [row for row in history if row]
        )
        self.report.num_keys_with_mixed_lifecycle_rows += mixed_keys
        live = [row for row in merged if row.deleted_on is None]
        if not live:
            # Gen 1 connectors are not SQL objects at all, so this surface sees
            # only Gen 2. An account running Gen 1 exclusively looks empty here
            # and the count of omitted Gen 1 connectors is not observable.
            self.report.report_empty_inventory("connectors")
        return [
            row
            for row in live
            if self._allowed(
                self.config.connector_pattern,
                row.name or row.key,
                self.report.report_dropped_connector,
            )
        ]

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
                self.report.warning(
                    title="Runtime with no visible parent deployment",
                    message="The runtime's deployment is not visible to this role, so "
                    "the runtime container cannot be nested. Grant MONITOR on the "
                    "deployment.",
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
            len({connector.runtime_name for connector in connectors}),
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
            yield from flow.as_workunits()
            if connector.owner:
                self.report.num_owners_emitted += 1
            inlets: List[str] = []
            outlets: List[str] = []
            if self.config.include_openflow_lineage:
                inlets, outlets = self._lineage_for_connector(connector)
            # The job is emitted regardless of whether lineage was found, so run
            # history and connector metadata have a stable anchor.
            job = build_connector_job(connector, flow, inlets=inlets, outlets=outlets)
            yield from job.as_workunits()
            if connector.owner:
                self.report.num_owners_emitted += 1

    def _report_connector_without_runtime_parent(
        self, connector: OpenflowConnector
    ) -> None:
        self.report.num_connectors_without_runtime_parent += 1
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
