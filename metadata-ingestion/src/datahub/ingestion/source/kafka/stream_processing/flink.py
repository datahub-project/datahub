import logging
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlsplit

import requests
from pydantic import BaseModel, ValidationError
from sqlglot import exp
from typing_extensions import LiteralString

from datahub.ingestion.source.kafka.kafka_config import FlinkLineageConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.constants import (
    FLINK_COMPUTE_POOL_PARAM,
    FLINK_DEFAULT_PAGE_SIZE,
    FLINK_HOST_TEMPLATE,
    FLINK_LIVE_PHASES,
    FLINK_MAX_PAGES,
    FLINK_PAGE_SIZE_PARAM,
    FLINK_PROP_CURRENT_CATALOG,
    FLINK_PROP_CURRENT_DATABASE,
    FLINK_SQL_DIALECT,
    FLINK_STATEMENTS_PATH_TEMPLATE,
    PROP_COMPUTE_POOL,
    PROP_STATE,
)
from datahub.ingestion.source.kafka.stream_processing.flink_sql import (
    FlinkDmlStatement,
    FlinkParsedStatement,
    FlinkSqlParseError,
    parse_flink_statement,
    table_ref,
)
from datahub.ingestion.source.kafka.stream_processing.models import (
    FlinkJob,
    FlinkStatement,
    FlinkStatementsPage,
)
from datahub.utilities.dedup_list import deduplicate_list

logger = logging.getLogger(__name__)

# Stand-ins for "this recipe's environment / cluster", whichever alias names them.
_THIS_ENVIRONMENT = "\0environment"
_THIS_CLUSTER = "\0cluster"


class _ResolvedTables(BaseModel):
    inputs: List[str]
    outputs: List[str]
    # `catalog.database.table` of tables outside this recipe's cluster.
    elsewhere: Set[str]
    # Writes or reads a view or temporary table the statement creates, so its column
    # lineage would name tables that are not topics.
    touches_non_topic: bool


class FlinkListingError(Exception):
    pass


class FlinkStatementsClient:
    def __init__(
        self,
        statements_url: str,
        credentials: Tuple[str, str],
        timeout_seconds: int,
        report: KafkaSourceReport,
        page_size: int = FLINK_DEFAULT_PAGE_SIZE,
        session: Optional[requests.Session] = None,
        compute_pool_id: Optional[str] = None,
    ) -> None:
        self.statements_url = statements_url
        self.timeout_seconds = timeout_seconds
        self.report = report
        self.page_size = page_size
        self._owns_session = session is None
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.session.auth = credentials
        self._host = urlsplit(statements_url).netloc
        self.compute_pool_id = compute_pool_id

    def list_statements(self) -> List[object]:
        statements: List[object] = []
        url: Optional[str] = self.statements_url
        first_page_params: Dict[str, Union[str, int]] = {
            FLINK_PAGE_SIZE_PARAM: self.page_size
        }
        if self.compute_pool_id:
            first_page_params[FLINK_COMPUTE_POOL_PARAM] = self.compute_pool_id
        params: Optional[Dict[str, Union[str, int]]] = first_page_params
        pages = 0
        while url and pages < FLINK_MAX_PAGES:
            page = self._fetch(url, params)
            pages += 1
            statements.extend(page.data)
            url = page.metadata.next if page.metadata else None
            if url:
                self._check_next_link(url)
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

    def _check_next_link(self, url: str) -> None:
        # The session sends the API key with every request, so a next link may only
        # point back at the same HTTPS host.
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.netloc != self._host:
            raise FlinkListingError(
                f"Refusing to follow a Flink statements page link to another host or "
                f"over plain HTTP: {parts.scheme}://{parts.netloc}"
            )

    def _fetch(
        self, url: str, params: Optional[Dict[str, Union[str, int]]]
    ) -> FlinkStatementsPage:
        # Errors propagate: the caller reports them as a failure, which stops
        # stale-entity removal from treating every Flink job as deleted. Redirects
        # are not followed, so credentials never leave the configured host.
        response = self.session.get(
            url, params=params, timeout=self.timeout_seconds, allow_redirects=False
        )
        if response.is_redirect:
            raise FlinkListingError(
                f"Flink statements request was redirected (HTTP {response.status_code})"
            )
        response.raise_for_status()
        try:
            return FlinkStatementsPage.model_validate(response.json())
        except (ValueError, ValidationError) as e:
            raise FlinkListingError(
                "Unexpected Confluent Cloud Flink statements response"
            ) from e

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
        self._other_locations: Set[str] = set()
        self._statements_in_this_cluster = 0

    def extract(self) -> List[FlinkJob]:
        jobs: List[FlinkJob] = []
        for item in self.client.list_statements():
            self.report.flink_statements_listed += 1
            try:
                job = self._build_job(FlinkStatement.model_validate(item))
            except Exception as e:
                # The statement may have produced a job last run; a failure keeps
                # stale-entity removal from soft-deleting it.
                name = item.get("name") if isinstance(item, dict) else None
                self.report.failure(
                    title="Flink statement could not be read",
                    message="Unexpected error reading a Flink statement; other "
                    "statements are still emitted",
                    context=f"statement={name}",
                    exc=e,
                )
                continue
            if job is not None:
                jobs.append(job)
        if self._other_locations and not self._statements_in_this_cluster:
            self.report.warning(
                title="No Flink statement emitted for this Kafka cluster",
                message="No Flink statement was emitted for this Kafka cluster, while "
                "some write to other environments or clusters. If statements name the "
                "catalog or database by display name, set "
                "stream_processing_lineage.flink.environment_name / kafka_cluster_name.",
                context=f"other_locations_seen={sorted(self._other_locations)[:10]}",
            )
        return jobs

    def _build_job(self, statement: FlinkStatement) -> Optional[FlinkJob]:
        if not statement.name:
            return None
        name = statement.name
        spec = statement.spec
        # The listing is already filtered by pool; this also drops rows whose pool
        # cannot be told, before anything about them is reported.
        if self.config.compute_pool_id and (
            spec is None or spec.compute_pool_id != self.config.compute_pool_id
        ):
            return None
        if spec is None or not spec.statement:
            # It may have had a job last run; a failure keeps it from being deleted.
            self._fail(name, "Flink statement has no SQL text in the API response")
            return None

        phase = statement.status.phase if statement.status else None
        if (
            phase not in FLINK_LIVE_PHASES
            and not self.config.include_non_running_statements
        ):
            self.report.flink_statements_not_running.append(name)
            return None

        try:
            parsed = parse_flink_statement(spec.statement)
        except FlinkSqlParseError as e:
            self._fail(name, "Flink statement SQL could not be parsed", exc=e)
            return None
        if not parsed.dml:
            # SELECT, DDL and SET statements move no data between topics.
            return None

        resolved = self._resolve_tables(name, parsed, spec.properties)
        if resolved is None:
            return None
        inputs, outputs, elsewhere = (
            resolved.inputs,
            resolved.outputs,
            resolved.elsewhere,
        )
        if not outputs:
            if elsewhere:
                # Writes to another cluster are that cluster's recipe's to emit.
                self.report.flink_statements_other_cluster.append(name)
                self._other_locations.update(elsewhere)
            # Otherwise it only writes temporary tables, which are not topics.
            return None
        self._statements_in_this_cluster += 1
        if elsewhere:
            self.report.warning(
                title="Flink lineage outside this cluster dropped",
                message="Dropping Flink lineage to tables in another environment or "
                "Kafka cluster; only this recipe's cluster is mapped to topic URNs",
                context=f"statement={name}, tables={sorted(elsewhere)}",
            )

        properties = {PROP_STATE: phase} if phase else {}
        if spec.compute_pool_id:
            properties[PROP_COMPUTE_POOL] = spec.compute_pool_id
        return FlinkJob(
            name=name,
            input_topics=inputs,
            output_topics=outputs,
            query=spec.statement,
            parse_query=_column_lineage_query(parsed.dml)
            if not elsewhere and not resolved.touches_non_topic
            else None,
            custom_properties=properties,
        )

    def _resolve_tables(
        self,
        name: str,
        parsed: FlinkParsedStatement,
        properties: Dict[str, str],
    ) -> Optional["_ResolvedTables"]:
        """Topics this statement reads and writes in this cluster, plus other places.

        Views and temporary tables the statement creates are left out wherever they
        are written or read. None when a table has no catalog or database to place it.
        """
        default_catalog = properties.get(FLINK_PROP_CURRENT_CATALOG) or None
        default_database = properties.get(FLINK_PROP_CURRENT_DATABASE) or None

        def location(table: exp.Table) -> Tuple[Optional[str], Optional[str], str]:
            ref = table_ref(table)
            return (
                ref.catalog or default_catalog,
                ref.database or default_database,
                ref.table,
            )

        def canonical(
            place: Tuple[Optional[str], Optional[str], str],
        ) -> Tuple[Optional[str], Optional[str], str]:
            # The environment and cluster may be named by id or display name.
            catalog, database, table = place
            return (
                _THIS_ENVIRONMENT if catalog in self._catalogs else catalog,
                _THIS_CLUSTER if database in self._databases else database,
                table,
            )

        non_topics = {canonical(location(table)) for table in parsed.created_non_topics}
        inputs: List[str] = []
        outputs: List[str] = []
        elsewhere: Set[str] = set()
        touches_non_topic = False
        for dml in parsed.dml:
            for table, targets in [(dml.output, outputs)] + [
                (t, inputs) for t in dml.inputs
            ]:
                catalog, database, topic = location(table)
                if canonical((catalog, database, topic)) in non_topics:
                    touches_non_topic = True
                    continue
                if catalog is None or database is None:
                    self._fail(
                        name,
                        "Flink statement has a table whose environment or Kafka "
                        "cluster is not named and has no sql.current-catalog / "
                        "sql.current-database default",
                        detail=f"table={topic}",
                    )
                    return None
                if catalog in self._catalogs and database in self._databases:
                    targets.append(topic)
                else:
                    elsewhere.add(f"{catalog}.{database}.{topic}")
        return _ResolvedTables(
            inputs=deduplicate_list(inputs),
            outputs=deduplicate_list(outputs),
            elsewhere=elsewhere,
            touches_non_topic=touches_non_topic,
        )

    def _fail(
        self,
        name: str,
        message: LiteralString,
        detail: Optional[str] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        # A failure, not a warning: the statement may have had a job last run, and
        # stale-entity removal would otherwise soft-delete it.
        self.report.flink_statements_skipped.append(name)
        self.report.failure(
            title="Flink statement skipped",
            message=message,
            context=f"statement={name}, {detail}" if detail else f"statement={name}",
            exc=exc,
        )


def _column_lineage_query(dml_statements: List[FlinkDmlStatement]) -> Optional[str]:
    # Column lineage needs one statement; its tables are all in this cluster, so they
    # are written as bare topic names for the parser. The parsed tree is left as is.
    if len(dml_statements) != 1:
        return None
    expression = dml_statements[0].expression.copy()
    for table in expression.find_all(exp.Table):
        table.set("catalog", None)
        table.set("db", None)
    return expression.sql(dialect=FLINK_SQL_DIALECT)


def build_flink_client(
    config: FlinkLineageConfig,
    report: KafkaSourceReport,
    session: Optional[requests.Session] = None,
) -> FlinkStatementsClient:
    base_url = config.endpoint or FLINK_HOST_TEMPLATE.format(
        region=config.region, cloud=config.cloud
    )
    path = FLINK_STATEMENTS_PATH_TEMPLATE.format(
        organization_id=config.organization_id,
        environment_id=config.environment_id,
    )
    if config.api_key is None or config.api_secret is None:
        # The config validator requires both halves of the key when Flink is enabled.
        raise ValueError("stream_processing_lineage.flink needs api_key and api_secret")
    return FlinkStatementsClient(
        statements_url=f"{base_url.rstrip('/')}{path}",
        credentials=(
            config.api_key.get_secret_value(),
            config.api_secret.get_secret_value(),
        ),
        timeout_seconds=config.timeout_seconds,
        report=report,
        session=session,
        compute_pool_id=config.compute_pool_id,
    )
