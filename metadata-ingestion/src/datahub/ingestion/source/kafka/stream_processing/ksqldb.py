import logging
from typing import Dict, List, Optional, Tuple

import requests

from datahub.ingestion.source.kafka.kafka_config import KsqlDBLineageConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.constants import (
    FROM_JOIN_RE,
    KSQL_ENDPOINT_PATH,
    KSQL_KEY_ID,
    KSQL_KEY_NAME,
    KSQL_KEY_QUERIES,
    KSQL_KEY_QUERY_STRING,
    KSQL_KEY_QUERY_TYPE,
    KSQL_KEY_SINK_TOPICS,
    KSQL_KEY_STREAMS,
    KSQL_KEY_TABLES,
    KSQL_KEY_TOPIC,
    KSQL_MEDIA_TYPE,
    KSQL_SQL_DIALECT,
    KSQL_STMT_LIST_STREAMS,
    KSQL_STMT_LIST_TABLES,
    KSQL_STMT_SHOW_QUERIES,
    StreamProcessingEngine,
    last_identifier_segment,
    quote_sql_identifier,
    rewrite_table_identifiers,
)
from datahub.ingestion.source.kafka.stream_processing.models import StreamProcessingJob

logger = logging.getLogger(__name__)

# ksqlDB persistent queries (CSAS/CTAS/INSERT INTO) are the ones that read topics and
# write a sink topic; push/pull queries are transient and have no durable lineage.
_PERSISTENT_QUERY_TYPE = "PERSISTENT"


class KsqlDbClient:
    def __init__(
        self,
        endpoint: str,
        credentials: Optional[Tuple[str, str]],
        timeout_seconds: int,
        report: KafkaSourceReport,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.url = f"{endpoint}{KSQL_ENDPOINT_PATH}"
        self.timeout_seconds = timeout_seconds
        self.report = report
        self.session = session or requests.Session()
        self.session.headers.update(
            {"Accept": KSQL_MEDIA_TYPE, "Content-Type": KSQL_MEDIA_TYPE}
        )
        if credentials is not None:
            self.session.auth = credentials

    def execute(self, statement: str) -> List[Dict[str, object]]:
        try:
            response = self.session.post(
                self.url,
                json={"ksql": statement},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            self.report.warning(
                message="Failed to query ksqlDB for stream-processing lineage",
                context=f"statement={statement}",
                exc=e,
                log=False,
            )
            return []
        if not isinstance(payload, list):
            self.report.warning(
                message="Unexpected ksqlDB response (expected a list)",
                context=f"statement={statement}",
                log=False,
            )
            return []
        return [item for item in payload if isinstance(item, dict)]

    def close(self) -> None:
        self.session.close()


class KsqlDBLineageExtractor:
    def __init__(self, client: KsqlDbClient, report: KafkaSourceReport) -> None:
        self.client = client
        self.report = report

    def extract(self) -> List[StreamProcessingJob]:
        name_to_topic = self._entity_topic_map()
        substitution = _NameToTopicSubstitution(name_to_topic)

        jobs: List[StreamProcessingJob] = []
        for result in self.client.execute(KSQL_STMT_SHOW_QUERIES):
            for query in _as_list(result.get(KSQL_KEY_QUERIES)):
                job = self._build_job(query, name_to_topic, substitution)
                if job is not None:
                    jobs.append(job)
        return jobs

    def _entity_topic_map(self) -> Dict[str, str]:
        # ksqlDB stream/table names are case-insensitive (stored upper-cased); key the
        # map by upper-case so FROM/JOIN identifiers resolve regardless of source casing.
        mapping: Dict[str, str] = {}
        for statement, key in (
            (KSQL_STMT_LIST_STREAMS, KSQL_KEY_STREAMS),
            (KSQL_STMT_LIST_TABLES, KSQL_KEY_TABLES),
        ):
            for result in self.client.execute(statement):
                for entity in _as_list(result.get(key)):
                    name = entity.get(KSQL_KEY_NAME)
                    topic = entity.get(KSQL_KEY_TOPIC)
                    if isinstance(name, str) and isinstance(topic, str) and topic:
                        mapping[name.upper()] = topic
        return mapping

    def _build_job(
        self,
        query: Dict[str, object],
        name_to_topic: Dict[str, str],
        substitution: "_NameToTopicSubstitution",
    ) -> Optional[StreamProcessingJob]:
        query_type = query.get(KSQL_KEY_QUERY_TYPE)
        if isinstance(query_type, str) and query_type.upper() != _PERSISTENT_QUERY_TYPE:
            return None

        query_id = query.get(KSQL_KEY_ID)
        query_string = query.get(KSQL_KEY_QUERY_STRING)
        if not isinstance(query_id, str) or not query_id:
            return None
        if not isinstance(query_string, str) or not query_string:
            return None

        output_topics = [
            topic for topic in _as_str_list(query.get(KSQL_KEY_SINK_TOPICS)) if topic
        ]
        input_topics = self._source_topics(query_string, name_to_topic)
        if not output_topics and not input_topics:
            return None

        self.report.stream_processing_jobs_scanned += 1
        return StreamProcessingJob(
            engine=StreamProcessingEngine.KSQLDB,
            job_id=query_id,
            name=query_id,
            input_topics=input_topics,
            output_topics=output_topics,
            query=query_string,
            parse_query=substitution.apply(query_string),
            sql_dialect=KSQL_SQL_DIALECT,
        )

    def _source_topics(
        self, query_string: str, name_to_topic: Dict[str, str]
    ) -> List[str]:
        topics: List[str] = []
        seen = set()
        for match in FROM_JOIN_RE.finditer(query_string):
            name = last_identifier_segment(match.group(1)).upper()
            topic = name_to_topic.get(name)
            if topic and topic not in seen:
                seen.add(topic)
                topics.append(topic)
        return topics


class _NameToTopicSubstitution:
    # Rewrites ksqlDB stream/table identifiers in a query to their backing topic names
    # so the SQL parser resolves lineage against the same topic dataset URNs we emit.
    # Only CREATE / INSERT / FROM / JOIN identifiers are rewritten so SELECT columns
    # that share a stream name are left alone.
    def __init__(self, name_to_topic: Dict[str, str]) -> None:
        self._name_to_topic = name_to_topic

    def apply(self, query_string: str) -> Optional[str]:
        if not self._name_to_topic:
            return None

        def replace_ident(identifier: str) -> str:
            topic = self._name_to_topic.get(last_identifier_segment(identifier).upper())
            if not topic:
                return identifier
            return quote_sql_identifier(topic)

        return rewrite_table_identifiers(query_string, replace_ident)


def build_ksqldb_client(
    config: KsqlDBLineageConfig,
    report: KafkaSourceReport,
    session: Optional[requests.Session] = None,
) -> Optional[KsqlDbClient]:
    if not config.endpoint:
        return None
    credentials: Optional[Tuple[str, str]] = None
    if config.api_key is not None and config.api_secret is not None:
        credentials = (
            config.api_key.get_secret_value(),
            config.api_secret.get_secret_value(),
        )
    return KsqlDbClient(
        endpoint=config.endpoint,
        credentials=credentials,
        timeout_seconds=config.timeout_seconds,
        report=report,
        session=session,
    )


def _as_list(value: object) -> List[Dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _as_str_list(value: object) -> List[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]
