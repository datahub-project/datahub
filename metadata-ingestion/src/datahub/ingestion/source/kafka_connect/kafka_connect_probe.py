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
from typing import Any, List, Optional

import requests
from typing_extensions import LiteralString

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeReadFailed
from datahub.ingestion.api.source import (
    StructuredLogCategory,
    StructuredLogLevel,
    StructuredLogs,
)
from datahub.ingestion.source.kafka_connect.common import (
    KAFKA_CONNECT_CONNECTOR_KIND,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource

# Ingestion's Connect calls pass no timeout, which a long run tolerates. A probe
# is a short diagnostic an agent waits on; a hung worker must fail it.
PROBE_REQUEST_TIMEOUT_SECONDS = 30

# Ingestion builds /connectors/{name} by plain interpolation, so these would
# change which endpoint a caller-supplied name reaches: "x/config" is a config
# read, "x?expand=info" returns every config at once.
_URL_SIGNIFICANT = ("/", "?", "#", "%")

# The package every reused ingestion step logs under.
_CONNECTOR_LOGGER = "datahub.ingestion.source.kafka_connect"


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
