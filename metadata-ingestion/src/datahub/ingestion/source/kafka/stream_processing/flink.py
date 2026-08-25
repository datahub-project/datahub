import logging
from typing import Dict, Iterable, List, Optional, Tuple

import requests

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
    FLINK_KEY_SPEC,
    FLINK_KEY_STATEMENT,
    FLINK_KEY_STATUS,
    FLINK_MAX_PAGES,
    FLINK_PAGE_SIZE_PARAM,
    FLINK_SQL_DIALECT,
    FLINK_STATEMENTS_PATH_TEMPLATE,
    FROM_JOIN_RE,
    INSERT_INTO_RE,
    PROP_STATE,
    StreamProcessingEngine,
    last_identifier_segment,
    quote_sql_identifier,
    rewrite_table_identifiers,
)
from datahub.ingestion.source.kafka.stream_processing.models import StreamProcessingJob

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
            if page is None:
                break
            data = page.get(FLINK_KEY_DATA)
            if isinstance(data, list):
                statements.extend(item for item in data if isinstance(item, dict))
            url = _next_link(page)
            # The next link is a full URL that already carries pagination params.
            params = None
        return statements

    def _fetch(
        self, url: str, params: Optional[Dict[str, int]]
    ) -> Optional[Dict[str, object]]:
        try:
            response = self.session.get(
                url, params=params, timeout=self.timeout_seconds
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            self.report.warning(
                message="Failed to list Confluent Cloud Flink statements",
                context=f"url={url}",
                exc=e,
                log=False,
            )
            return None
        if not isinstance(payload, dict):
            self.report.warning(
                message="Unexpected Confluent Cloud Flink statements response",
                context=f"url={url}",
                log=False,
            )
            return None
        return payload

    def close(self) -> None:
        self.session.close()


class FlinkLineageExtractor:
    def __init__(
        self,
        client: FlinkStatementsClient,
        report: KafkaSourceReport,
        compute_pool_id: Optional[str] = None,
    ) -> None:
        self.client = client
        self.report = report
        self.compute_pool_id = compute_pool_id

    def extract(self) -> List[StreamProcessingJob]:
        jobs: List[StreamProcessingJob] = []
        for statement in self.client.list_statements():
            job = self._build_job(statement)
            if job is not None:
                jobs.append(job)
        return jobs

    def _build_job(self, statement: Dict[str, object]) -> Optional[StreamProcessingJob]:
        spec = statement.get(FLINK_KEY_SPEC)
        if not isinstance(spec, dict):
            return None
        if self.compute_pool_id:
            pool = spec.get(FLINK_KEY_COMPUTE_POOL)
            if pool != self.compute_pool_id:
                return None

        sql = spec.get(FLINK_KEY_STATEMENT)
        name = statement.get(FLINK_KEY_NAME)
        if not isinstance(sql, str) or not sql or not isinstance(name, str) or not name:
            return None

        output_topics = _unique(
            last_identifier_segment(m.group(1)) for m in INSERT_INTO_RE.finditer(sql)
        )
        if not output_topics:
            # Only INSERT INTO statements move data between topics; skip DDL/queries.
            return None
        input_topics = _unique(
            last_identifier_segment(m.group(1)) for m in FROM_JOIN_RE.finditer(sql)
        )

        self.report.stream_processing_jobs_scanned += 1
        return StreamProcessingJob(
            engine=StreamProcessingEngine.FLINK,
            job_id=name,
            name=name,
            input_topics=input_topics,
            output_topics=output_topics,
            query=sql,
            parse_query=_collapse_identifiers(sql),
            sql_dialect=FLINK_SQL_DIALECT,
            custom_properties=_status_properties(statement),
        )


def _collapse_identifiers(sql: str) -> str:
    # Flink references topics as `catalog`.`database`.`table`; the SQL parser would treat
    # the 3-part name as db.schema.table and never match a topic URN. Reduce each
    # INSERT/FROM/JOIN identifier to its final segment (the topic) for column parsing.
    def replace_ident(identifier: str) -> str:
        return quote_sql_identifier(last_identifier_segment(identifier))

    return rewrite_table_identifiers(sql, replace_ident)


def _status_properties(statement: Dict[str, object]) -> Dict[str, str]:
    status = statement.get(FLINK_KEY_STATUS)
    if isinstance(status, dict):
        phase = status.get(FLINK_KEY_PHASE)
        if isinstance(phase, str) and phase:
            return {PROP_STATE: phase}
    return {}


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
    seen = set()
    result: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result
