import logging
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from typing_extensions import LiteralString

from datahub.ingestion.source.kafka.kafka_config import FlinkLineageConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.constants import (
    FLINK_DEFAULT_PAGE_SIZE,
    FLINK_HOST_TEMPLATE,
    FLINK_KEY_COMPUTE_POOL,
    FLINK_KEY_DATA,
    FLINK_KEY_METADATA,
    FLINK_KEY_NAME,
    FLINK_KEY_NEXT,
    FLINK_KEY_PHASE,
    FLINK_KEY_PROPERTIES,
    FLINK_KEY_SPEC,
    FLINK_KEY_STATEMENT,
    FLINK_KEY_STATUS,
    FLINK_MAX_PAGES,
    FLINK_PAGE_SIZE_PARAM,
    FLINK_PHASE_RUNNING,
    FLINK_PROP_CURRENT_CATALOG,
    FLINK_PROP_CURRENT_DATABASE,
    FLINK_SQL_DIALECT,
    FLINK_STATEMENTS_PATH_TEMPLATE,
    PROP_STATE,
    StreamProcessingEngine,
)
from datahub.ingestion.source.kafka.stream_processing.flink_sql import (
    FlinkSqlParseError,
    parse_flink_statement,
    table_ref,
)
from datahub.ingestion.source.kafka.stream_processing.models import StreamProcessingJob
from datahub.utilities.dedup_list import deduplicate_list

logger = logging.getLogger(__name__)


class FlinkStatementsClient:
    def __init__(
        self,
        statements_url: str,
        credentials: Optional[Tuple[str, str]],
        timeout_seconds: int,
        report: KafkaSourceReport,
        page_size: int = FLINK_DEFAULT_PAGE_SIZE,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.statements_url = statements_url
        self.timeout_seconds = timeout_seconds
        self.report = report
        self.page_size = page_size
        self._owns_session = session is None
        self.session = session or requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        if credentials is not None:
            self.session.auth = credentials

    def list_statements(self) -> List[Dict[str, object]]:
        statements: List[Dict[str, object]] = []
        url: Optional[str] = self.statements_url
        params: Optional[Dict[str, int]] = {FLINK_PAGE_SIZE_PARAM: self.page_size}
        pages = 0
        while url and pages < FLINK_MAX_PAGES:
            page = self._fetch(url, params)
            pages += 1
            data = page.get(FLINK_KEY_DATA)
            if isinstance(data, list):
                statements.extend(item for item in data if isinstance(item, dict))
            url = _next_link(page)
            # The next link is a full URL that already carries pagination params.
            params = None
        if url:
            # Statements past the cap would look deleted; a failure keeps stale-entity
            # removal from soft-deleting their jobs.
            self.report.failure(
                title="Flink statements listing truncated",
                message="Stopped listing Confluent Cloud Flink statements at the page "
                "limit; lineage for later statements is missing from this run",
                context=f"pages={pages}, page_size={self.page_size}",
            )
        return statements

    def _fetch(self, url: str, params: Optional[Dict[str, int]]) -> Dict[str, object]:
        # Errors propagate: the caller reports them as a failure, which stops
        # stale-entity removal from treating every Flink job as deleted.
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(
                f"Unexpected Confluent Cloud Flink statements payload type: "
                f"{type(payload).__name__} from {url}"
            )
        return payload

    def close(self) -> None:
        if self._owns_session:
            self.session.close()


class FlinkLineageExtractor:
    """Turns Confluent Cloud Flink statements into topic-level jobs for one cluster.

    In Confluent Cloud Flink a catalog is an environment and a database is a Kafka
    cluster, so `catalog.database.table` names a topic in a specific cluster:
    https://docs.confluent.io/cloud/current/flink/overview.html
    Only tables that resolve to this recipe's environment and cluster become URNs;
    anything else is dropped and reported rather than mapped onto a same-named topic here.
    """

    def __init__(
        self,
        client: FlinkStatementsClient,
        report: KafkaSourceReport,
        config: FlinkLineageConfig,
    ) -> None:
        self.client = client
        self.report = report
        self.config = config
        self._catalogs = {
            name for name in (config.environment_id, config.environment_name) if name
        }
        self._databases = {
            name
            for name in (config.kafka_cluster_id, config.kafka_cluster_name)
            if name
        }

    def extract(self) -> List[StreamProcessingJob]:
        jobs: List[StreamProcessingJob] = []
        for statement in self.client.list_statements():
            try:
                job = self._build_job(statement)
            except Exception as e:
                # The statement may have produced a job last run; a failure keeps
                # stale-entity removal from soft-deleting it.
                self.report.failure(
                    title="Flink statement could not be read",
                    message="Unexpected error reading a Flink statement; other "
                    "statements are still emitted",
                    context=f"statement={statement.get(FLINK_KEY_NAME)}",
                    exc=e,
                )
                continue
            if job is not None:
                jobs.append(job)
        return jobs

    def _build_job(self, statement: Dict[str, object]) -> Optional[StreamProcessingJob]:
        spec = statement.get(FLINK_KEY_SPEC)
        if not isinstance(spec, dict):
            return None
        if self.config.compute_pool_id:
            if spec.get(FLINK_KEY_COMPUTE_POOL) != self.config.compute_pool_id:
                return None

        sql = spec.get(FLINK_KEY_STATEMENT)
        # sql/v1 puts the statement name at the top level, not under `metadata`.
        name = statement.get(FLINK_KEY_NAME)
        if not isinstance(sql, str) or not sql or not isinstance(name, str) or not name:
            return None

        phase = _phase(statement)
        if (
            phase != FLINK_PHASE_RUNNING
            and not self.config.include_non_running_statements
        ):
            self.report.stream_processing_statements_not_running.append(name)
            return None

        try:
            dml_statements = parse_flink_statement(sql)
        except FlinkSqlParseError as e:
            self._skip(
                name,
                "Skipping a Flink statement whose SQL could not be parsed",
                f"statement={name}",
                e,
            )
            return None
        if not dml_statements:
            # SELECT, DDL and SET statements move no data between topics.
            return None

        properties = spec.get(FLINK_KEY_PROPERTIES)
        properties = properties if isinstance(properties, dict) else {}
        default_catalog = _str_or_none(properties.get(FLINK_PROP_CURRENT_CATALOG))
        default_database = _str_or_none(properties.get(FLINK_PROP_CURRENT_DATABASE))

        outputs: List[str] = []
        inputs: List[str] = []
        elsewhere: List[str] = []
        for dml in dml_statements:
            for table, targets in [(t, outputs) for t in dml.outputs] + [
                (t, inputs) for t in dml.inputs
            ]:
                ref = table_ref(table)
                catalog = ref.catalog or default_catalog
                database = ref.database or default_database
                if catalog is None or database is None:
                    self._skip(
                        name,
                        "Skipping a Flink statement with a table whose environment or "
                        "Kafka cluster is not named and has no sql.current-catalog / "
                        "sql.current-database default",
                        f"statement={name}, table={ref.table}",
                    )
                    return None
                if catalog in self._catalogs and database in self._databases:
                    targets.append(ref.table)
                    # Unqualified, so the column-lineage parser maps it to the topic URN.
                    table.set("catalog", None)
                    table.set("db", None)
                else:
                    elsewhere.append(f"{catalog}.{database}.{ref.table}")

        outputs = _unique(outputs)
        if not outputs:
            # Writes to another cluster are that cluster's recipe's to emit.
            return None
        if elsewhere:
            self.report.warning(
                title="Flink lineage outside this cluster dropped",
                message="Dropping Flink lineage to tables in another environment or "
                "Kafka cluster; only this recipe's cluster is mapped to topic URNs",
                context=f"statement={name}, tables={sorted(set(elsewhere))}",
            )

        # Column lineage needs one statement whose every table is in this cluster.
        parse_query: Optional[str] = None
        if len(dml_statements) == 1 and not elsewhere:
            parse_query = dml_statements[0].expression.sql(dialect=FLINK_SQL_DIALECT)

        self.report.stream_processing_jobs_scanned += 1
        return StreamProcessingJob(
            engine=StreamProcessingEngine.FLINK,
            job_id=name,
            name=name,
            input_topics=_unique(inputs),
            output_topics=outputs,
            query=sql,
            parse_query=parse_query,
            sql_dialect=FLINK_SQL_DIALECT,
            custom_properties={PROP_STATE: phase} if phase else {},
        )

    def _skip(
        self,
        name: str,
        message: LiteralString,
        context: str,
        exc: Optional[BaseException] = None,
    ) -> None:
        self.report.stream_processing_statements_skipped.append(name)
        self.report.warning(
            title="Flink statement skipped", message=message, context=context, exc=exc
        )


def _phase(statement: Dict[str, object]) -> Optional[str]:
    status = statement.get(FLINK_KEY_STATUS)
    if isinstance(status, dict):
        phase = status.get(FLINK_KEY_PHASE)
        if isinstance(phase, str) and phase:
            return phase
    return None


def _str_or_none(value: object) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def build_flink_client(
    config: FlinkLineageConfig,
    report: KafkaSourceReport,
    session: Optional[requests.Session] = None,
) -> Optional[FlinkStatementsClient]:
    base_url = config.endpoint or FLINK_HOST_TEMPLATE.format(
        region=config.region, cloud=config.cloud
    )
    if not config.organization_id or not config.environment_id:
        return None
    path = FLINK_STATEMENTS_PATH_TEMPLATE.format(
        organization_id=config.organization_id,
        environment_id=config.environment_id,
    )
    credentials: Optional[Tuple[str, str]] = None
    if config.api_key is not None and config.api_secret is not None:
        credentials = (
            config.api_key.get_secret_value(),
            config.api_secret.get_secret_value(),
        )
    return FlinkStatementsClient(
        statements_url=f"{base_url}{path}",
        credentials=credentials,
        timeout_seconds=config.timeout_seconds,
        report=report,
        session=session,
    )


def _next_link(page: Dict[str, object]) -> Optional[str]:
    metadata = page.get(FLINK_KEY_METADATA)
    if isinstance(metadata, dict):
        nxt = metadata.get(FLINK_KEY_NEXT)
        if isinstance(nxt, str) and nxt:
            return nxt
    return None


def _unique(values: Iterable[str]) -> List[str]:
    return deduplicate_list(value for value in values if value)
