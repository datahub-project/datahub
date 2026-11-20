import logging
from typing import Any, Dict, Iterable, List, Optional

from opensearchpy.helpers import scan

from datahub.ingestion.source.sql_queries.config import (
    ElasticsearchQuerySourceConfig,
)
from datahub.ingestion.source.sql_queries.readers.mapped import (
    MappedQueryLogReader,
    lookup_path,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport

logger = logging.getLogger(__name__)

Hit = Dict[str, Any]


class ElasticsearchQueryLogReader(MappedQueryLogReader[Hit]):
    """Reads query records from an Elasticsearch/OpenSearch index.

    Documents with no SQL in the query field are skipped, so an index that mixes
    query records with other audit events can be read directly.
    """

    def __init__(
        self, config: ElasticsearchQuerySourceConfig, report: SqlQueriesSourceReport
    ) -> None:
        super().__init__(config, report)
        self.es_config = config

    def build_query(self) -> Dict[str, Any]:
        mappings = self.es_config.field_mappings
        filters: List[Dict[str, Any]] = [{"exists": {"field": mappings.query}}]
        if self.es_config.query_filter:
            filters.append(self.es_config.query_filter)
        query: Dict[str, Any] = {"query": {"bool": {"filter": filters}}}
        # The aggregator resolves session temp tables from earlier statements, so
        # records must arrive in ascending timestamp order. _doc is a stable
        # tiebreaker so records sharing a timestamp keep a deterministic order.
        if mappings.timestamp is not None:
            query["sort"] = [{mappings.timestamp: {"order": "asc"}}, {"_doc": "asc"}]
        return query

    def _read_records(self) -> Iterable[Hit]:
        config = self.es_config
        client = config.connection.get_client(
            self.report,
            timeout=config.request_timeout_seconds,
            retry_on_timeout=True,
            max_retries=config.max_retries,
        )
        # preserve_order keeps the scroll in sort order, at the cost of the
        # parallel-scroll optimisation, so it's only used when there is a sort.
        # The scroll keepalive must outlast downstream processing of one scroll
        # page (batch_size * matching shards); a slower page still fails, and is
        # reported. PIT + search_after would remove that limit.
        yield from scan(
            client,
            index=config.index,
            query=self.build_query(),
            size=config.batch_size,
            preserve_order=config.field_mappings.timestamp is not None,
            scroll=config.scroll_keepalive,
            request_timeout=config.request_timeout_seconds,
        )

    def _lookup(self, record: Hit, path: str) -> Any:
        return lookup_path(record.get("_source") or {}, path)

    def _context_of(self, record: Hit) -> Optional[str]:
        # _id alone isn't unique when index is a pattern.
        return f"{record.get('_index')}/{record.get('_id')}"
