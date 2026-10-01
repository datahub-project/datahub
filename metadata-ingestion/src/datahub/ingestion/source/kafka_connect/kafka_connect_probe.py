"""Metadata-only probe over the Kafka Connect REST API.

Connector configs on a Connect cluster hold credentials under names no hint
list anticipates -- JDBC URLs with ?password=, Mongo URIs with userinfo, plugin
keys like snowflake.private.key -- so this provider never returns a config
value unless its key is on DISCLOSED_CONFIG_KEYS. That is also why there is no
`api` passthrough: /connectors/{name}, /config, /tasks and ?expand=info all
return configs, and /status returns stack traces.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional

import requests
from typing_extensions import LiteralString

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeReadFailed
from datahub.ingestion.api.source import (
    StructuredLogCategory,
    StructuredLogLevel,
    StructuredLogs,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.kafka_connect.common import (
    CONNECTOR_CLASS,
    KAFKA_CONNECT_CONNECTOR_KIND,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.connector_registry import (
    ConnectorRegistry,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.metadata.schema_classes import DataJobInputOutputClass

# Ingestion's Connect calls pass no timeout, which a long run tolerates. A probe
# is a short diagnostic an agent waits on; a hung worker must fail it.
PROBE_REQUEST_TIMEOUT_SECONDS = 30

# Ingestion builds /connectors/{name} by plain interpolation, so these would
# change which endpoint a caller-supplied name reaches: "x/config" is a config
# read, "x?expand=info" returns every config at once.
_URL_SIGNIFICANT = ("/", "?", "#", "%")

# The package every reused ingestion step logs under.
_CONNECTOR_LOGGER = "datahub.ingestion.source.kafka_connect"

# Config keys whose VALUES the probe may return: what lineage inference reads,
# and nothing that authenticates. An allowlist on purpose -- Connect plugins name
# their secrets freely, so a denylist would admit the next plugin's credential
# by default. Deliberately absent:
#   connection.url / connection.uri -- carry passwords (JDBC ?password=, Mongo
#       userinfo); the resolved dataset URNs in connector_lineage answer the
#       "which database" question without them
#   query                           -- JDBC query mode's raw SQL; a WHERE literal
#       is a row value (the rule hex_probe applies to /cells)
#   *.hostname / *.host / *.port     -- environment detail, not needed to judge
#       lineage once URNs are shown
DISCLOSED_CONFIG_KEYS: FrozenSet[str] = frozenset(
    {
        "connector.class",
        "tasks.max",
        "mode",
        "topics",
        "topics.regex",
        "topic",
        "kafka.topic",
        "topic.prefix",
        "table.whitelist",
        "table.blacklist",
        "table.include.list",
        "table.exclude.list",
        "table.name.format",
        "schema.include.list",
        "schema.exclude.list",
        "database.include.list",
        "database.server.name",
        "database.dbname",
        "database.name",
        "database.names",
        "database.default.schema",
        "database",
        "db.name",
        "db.schema",
        "snowflake.database.name",
        "snowflake.schema.name",
        "snowflake.topic2table.map",
        "topic2table.map",
        "topic2TableMap",
        "project",
        "defaultDataset",
        "datasets",
        "topicsToTables",
        "sanitizeTopics",
        "s3.bucket.name",
        "topics.dir",
        "iceberg.tables",
        "iceberg.tables.dynamic-enabled",
        "transforms",
    }
)

# For `transforms.<alias>.<prop>`: the properties transform_plugins and the
# routers read to rename topics. Other SMT properties are not disclosed -- a
# custom SMT may take its own credential.
DISCLOSED_TRANSFORM_PROPERTIES: FrozenSet[str] = frozenset(
    {
        "type",
        "regex",
        "replacement",
        "topic.format",
        "timestamp.format",
        "route.by.field",
        "route.topic.regex",
        "route.topic.replacement",
    }
)


def disclosed_config(config: Dict[str, str]) -> Dict[str, str]:
    """The subset of a connector config the probe may return, sorted by key."""
    out: Dict[str, str] = {}
    for key, value in config.items():
        if key in DISCLOSED_CONFIG_KEYS:
            out[key] = value
            continue
        # split(".", 2): the property may itself be dotted ("topic.format").
        parts = key.split(".", 2)
        if (
            len(parts) == 3
            and parts[0] == "transforms"
            and parts[2] in DISCLOSED_TRANSFORM_PROPERTIES
        ):
            out[key] = value
    return dict(sorted(out.items()))


@dataclass
class _Resolved:
    manifest: Optional[ConnectorManifest]
    # Snapshotted before provided_configs substitution: a ${provider:path:key}
    # reference is not a secret, the value substituted into it may be.
    raw_config: Dict[str, str]
    emitted: bool


class _UnloggedStructuredLogs(StructuredLogs):
    """Report entries that are recorded but never written to the Python logger.

    Ingestion's report.warning() logs `message => context` to the console, and
    the reused steps put raw connector config into contexts -- the JDBC sink
    puts the whole connection.url there when it cannot parse the platform. The
    CLI's log masking only knows the recipe's secrets, not values read off the
    Connect cluster, so on a probe that line would reach stderr as-is. The probe
    result renders entries as `title: message` only, so nothing is lost by not
    logging them."""

    def report_log(
        self,
        level: StructuredLogLevel,
        message: LiteralString,
        title: Optional[LiteralString] = None,
        context: Optional[str] = None,
        exc: Optional[BaseException] = None,
        log: bool = False,
        stacklevel: int = 1,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        super().report_log(
            level,
            message,
            title=title,
            context=context,
            exc=exc,
            log=False,
            stacklevel=stacklevel,
            log_category=log_category,
        )


@dataclass
class _ProbeReport(KafkaConnectSourceReport):
    _structured_logs: StructuredLogs = field(default_factory=_UnloggedStructuredLogs)


class _TimeoutSession(requests.Session):
    """A Session that applies a default timeout; an explicit one still wins."""

    def __init__(self, timeout_seconds: float) -> None:
        super().__init__()
        self._timeout_seconds = timeout_seconds

    # Any: this forwards requests.Session.request's whole signature unchanged.
    def request(self, *args: Any, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self._timeout_seconds)
        return super().request(*args, **kwargs)


class KafkaConnectMetadataProbe:
    """Reads through an uninitialized KafkaConnectSource (see
    KafkaConnectSource.for_probe), so each command runs ingestion's own
    per-connector steps -- minus connector_patterns, which `probe filter` judges.
    """

    def __init__(self, source: KafkaConnectSource) -> None:
        self._source = source
        # Replaced rather than reused so the reused steps' report entries are
        # never logged; see _UnloggedStructuredLogs.
        self._source.report = _ProbeReport()
        self.warnings: List[str] = []
        self._saved_log_level: Optional[int] = None

    @classmethod
    def for_config(
        cls, config: KafkaConnectSourceConfig
    ) -> "KafkaConnectMetadataProbe":
        # The same factories ingestion's __init__ uses, so headers, auth and the
        # Kafka REST retry adapter match; only the timeout is the probe's own.
        # No request here: ingestion's test GET is replaced by the first command.
        session = KafkaConnectSource._create_connect_session(
            config, session=_TimeoutSession(PROBE_REQUEST_TIMEOUT_SECONDS)
        )
        kafka_session = KafkaConnectSource._create_kafka_session(
            session=_TimeoutSession(PROBE_REQUEST_TIMEOUT_SECONDS)
        )
        return cls(
            KafkaConnectSource.for_probe(
                config, session=session, kafka_session=kafka_session
            )
        )

    def __enter__(self) -> "KafkaConnectMetadataProbe":
        # The reused steps also log connector details directly (logger.info /
        # .warning with config values interpolated), and the CLI's masking cannot
        # recognise a credential it was never told about. Silenced for the
        # probe's lifetime only; the result's warnings and failures still carry
        # every degraded read via probe_report.
        connector_logger = logging.getLogger(_CONNECTOR_LOGGER)
        self._saved_log_level = connector_logger.level
        connector_logger.setLevel(logging.CRITICAL + 1)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._saved_log_level is not None:
            logging.getLogger(_CONNECTOR_LOGGER).setLevel(self._saved_log_level)
            self._saved_log_level = None
        # Not self._source.close(): that runs StatefulIngestionSourceBase.close()
        # on a shim that never ran its __init__.
        self._source.session.close()
        self._source.kafka_session.close()

    @property
    def probe_report(self) -> object:
        """Ingestion's report. The reused steps record every degraded read on it
        (runtime topics, tasks, Kafka REST), so exposing it is what keeps an
        empty result from reading as 'nothing here'."""
        return self._source.report

    @probe_method(kind=KAFKA_CONNECT_CONNECTOR_KIND, row_limit_param="limit")
    def connectors(self, limit: int = 200) -> List[str]:
        """Connectors on this Connect cluster, by the name connector_patterns is
        matched against, including ones it would exclude -- judge them with
        `probe filter --kind Connector`. A pattern is not the only reason a
        connector is skipped: an unsupported source connector is dropped too,
        which `probe run connector` reports as `emitted: false`. Names only."""
        return sorted(self._listed_names())[:limit]

    def _listed_names(self) -> List[str]:
        # The GET /connectors ingestion's endpoint discovery already makes, which
        # (unlike get_connectors_manifest's) raises on 401/5xx.
        payload: object = self._source._get_connector_names_for_endpoint_discovery()
        if isinstance(payload, list):
            return [str(name) for name in payload]
        raise ProbeReadFailed(
            f"GET /connectors returned {type(payload).__name__}, not a list of "
            f"connector names"
        )

    def _require_listed(self, connector: str) -> None:
        """Refuse a name before it becomes part of a URL.

        Listing membership is the real check -- only a name the cluster itself
        reports can reach a per-connector endpoint. The character check runs
        first so a crafted name costs no request at all."""
        if any(ch in connector for ch in _URL_SIGNIFICANT):
            raise ValueError(
                f"connector name '{connector}' contains URL syntax "
                f"({' '.join(_URL_SIGNIFICANT)}); ingestion addresses connectors "
                f"by unencoded name, so it could not read this one either"
            )
        if connector not in self._listed_names():
            raise ValueError(
                f"no connector named '{connector}' on this Connect cluster; "
                f"`probe run connectors` lists them"
            )

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _resolve(self, connector: str) -> _Resolved:
        """One connector through ingestion's own steps, minus connector_patterns.

        The fetch is the probe's own so a 401/5xx raises instead of degrading to
        a warning as ingestion's _get_connector_manifest does; the parse and the
        enrichment are ingestion's."""
        self._require_listed(connector)
        self._warn_unreproduced()
        source = self._source
        url = f"{source.config.connect_uri}/connectors/{connector}"
        response = source.session.get(url)
        if response.status_code == 404:
            raise ValueError(f"connector '{connector}' was listed but no longer exists")
        response.raise_for_status()
        manifest = source._parse_connector_manifest(connector, response.json())
        if manifest is None:
            # Ingestion drops it; the parse recorded why on the report.
            return _Resolved(manifest=None, raw_config={}, emitted=False)
        raw_config = dict(manifest.config)
        emitted = source._enrich_manifest(connector, manifest, url)
        return _Resolved(manifest=manifest, raw_config=raw_config, emitted=emitted)

    def _warn_unreproduced(self) -> None:
        """Say where the probe's answer can differ from ingestion's."""
        config = self._source.config
        if config.use_schema_resolver:
            self._warn(
                "use_schema_resolver is on (Confluent Cloud turns it on unless it "
                "is set to false), but the probe has no DataHub connection: table "
                "patterns are not expanded from DataHub and no column-level "
                "lineage is computed, so ingestion may emit more tables and "
                "column edges than shown"
            )
        if (
            config.confluent_catalog.enabled
            and config.confluent_catalog.include_lineage
        ):
            self._warn(
                "confluent_catalog.include_lineage is on: ingestion takes source "
                "connectors' topics from the Stream Catalog, and keeps an "
                "unsupported source connector the catalog resolves. The probe "
                "does not read the catalog and shows config-inferred lineage"
            )

    @probe_method()
    def connector(self, connector: str) -> Dict[str, object]:
        """One connector as ingestion would treat it: its type and class, the
        handler that infers its lineage (null when none does), whether ingestion
        emits it at all (`emitted: false` for an unsupported source connector,
        whatever connector_patterns says), its DataFlow URN, and counts of
        lineage edges, runtime topics and tasks. `name` is the string
        connector_patterns is matched against. `config_keys` lists every
        configured key NAME; `lineage_config` gives values only for keys lineage
        inference reads. Credentials, connection URLs and query text are never
        returned."""
        resolved = self._resolve(connector)
        manifest = resolved.manifest
        if manifest is None:
            return {"name": connector, "emitted": False}
        source = self._source
        handler = ConnectorRegistry.get_connector_for_manifest(
            manifest, source.config, source.report, None
        )
        return {
            "name": manifest.name,
            "type": manifest.type,
            "connector_class": resolved.raw_config.get(CONNECTOR_CLASS),
            "handled_by": type(handler).__name__ if handler else None,
            "platform": handler.get_platform() if handler else None,
            "emitted": resolved.emitted,
            # Only the URN: the aspect's customProperties is the flow property
            # bag, which is a per-connector denylist and not safe to return.
            "flow_urn": source.construct_flow_workunit(manifest).get_urn(),
            "lineage_edges": len(manifest.lineages),
            "runtime_topics": len(manifest.topic_names),
            "tasks": len(manifest.tasks),
            "config_keys": sorted(resolved.raw_config),
            "lineage_config": disclosed_config(resolved.raw_config),
        }

    @probe_method(
        kind=DatasetSubTypes.TOPIC, row_limit_param="limit", parent_params=("connector",)
    )
    def connector_topics(self, connector: str, limit: int = 500) -> List[str]:
        """Topics ingestion resolves for one connector from Connect's runtime
        /topics API, with stale topics dropped for a sink whose config names its
        topics. Empty, with a warning saying why, on Confluent Cloud (no such API;
        see `cluster_topics`) or when use_connect_topics_api is false. Nothing
        filters topics; judge the connector instead."""
        config = self._source.config
        if not config.use_connect_topics_api:
            self._require_listed(connector)
            self._warn(
                "use_connect_topics_api is false, so ingestion reads no runtime "
                "topics for any connector and infers lineage from config alone"
            )
            return []
        if self._source._is_confluent_cloud:
            self._require_listed(connector)
            self._warn(
                "Confluent Cloud has no per-connector topics API, so ingestion "
                "leaves this empty and infers topics from the connector config "
                "and the cluster topic list; see `connector_lineage` for the "
                "topics lineage names and `cluster_topics` for that list"
            )
            return []
        resolved = self._resolve(connector)
        if resolved.manifest is None:
            return []
        return list(resolved.manifest.topic_names)[:limit]

    @probe_method(kind=DatasetSubTypes.TOPIC, row_limit_param="limit")
    def cluster_topics(self, limit: int = 500) -> List[str]:
        """Confluent Cloud only: the non-internal topics on the Kafka cluster, from
        the Kafka REST v3 API, which ingestion uses to infer sink and regex-routed
        topics. Empty with a warning when the REST endpoint or its credentials
        are missing -- in which case ingestion's lineage for those connectors is
        config-inferred only. The listing is fully paged before the limit
        applies, as ingestion pages it."""
        if not self._source._is_confluent_cloud:
            raise ValueError(
                "cluster_topics applies to Confluent Cloud only; self-hosted "
                "ingestion reads each connector's runtime topics instead -- use "
                "connector_topics"
            )
        topics = self._source._get_all_topics_from_kafka_api()
        # None means unavailable, and the reason is already on the report.
        return sorted(topics)[:limit] if topics is not None else []

    @probe_method(row_limit_param="limit", parent_params=("connector",))
    def connector_lineage(
        self, connector: str, limit: int = 200
    ) -> List[Dict[str, object]]:
        """The DataJobs ingestion would emit for one connector, each with the
        dataset URNs it reads and writes -- after platform_instance_map,
        connect_to_platform_map, generic_connectors and
        convert_lineage_urns_to_lowercase, so a URN here is the one to compare
        against the upstream source's datasets. `fine_grained_edges` counts
        column-level edges. Shown even for a connector connector_patterns would
        exclude; empty, with the reason in warnings, for a connector ingestion
        does not emit."""
        resolved = self._resolve(connector)
        if resolved.manifest is None or not resolved.emitted:
            return []
        edges: List[Dict[str, object]] = []
        # construct_job_workunits is what ingestion emits, so the URNs are not
        # re-derived here and cannot drift from it.
        for wu in self._source.construct_job_workunits(resolved.manifest):
            mcp = wu.metadata
            if not (
                isinstance(mcp, MetadataChangeProposalWrapper)
                and isinstance(mcp.aspect, DataJobInputOutputClass)
            ):
                continue
            edges.append(
                {
                    "job": str(mcp.entityUrn),
                    "inputs": list(mcp.aspect.inputDatasets),
                    "outputs": list(mcp.aspect.outputDatasets),
                    "fine_grained_edges": len(mcp.aspect.fineGrainedLineages or []),
                }
            )
            if len(edges) >= limit:
                break
        return edges
