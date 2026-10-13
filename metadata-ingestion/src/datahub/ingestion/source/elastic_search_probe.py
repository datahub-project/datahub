from typing import Dict, Iterator, List, Mapping, Sequence
from urllib.parse import quote

from opensearchpy import OpenSearch
from opensearchpy.exceptions import TransportError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, take
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.elastic_search import (
    ElasticsearchSourceConfig,
    ElasticToSchemaFieldConverter,
    create_elasticsearch_client,
)

# Indices named per `GET <a>,<b>,...` request: few round trips, and a request
# path kept under common proxy request-line limits (8 KiB) once URL-encoded,
# which can triple a non-ASCII name's length.
_INDICES_PER_REQUEST = 25
_MAX_ENCODED_PATH = 4096


def _batches(names: Sequence[str]) -> Iterator[List[str]]:
    batch: List[str] = []
    length = 0
    for name in names:
        encoded = len(quote(name, safe="")) + 1
        if batch and (
            len(batch) >= _INDICES_PER_REQUEST or length + encoded > _MAX_ENCODED_PATH
        ):
            yield batch
            batch, length = [], 0
        batch.append(name)
        length += encoded
    if batch:
        yield batch


def _mapped_fields(mappings: object) -> int:
    """How many schema fields ingestion would emit for these mappings, counted
    by ingestion's own converter."""
    if not isinstance(mappings, dict):
        return 0
    return sum(1 for _ in ElasticToSchemaFieldConverter.get_schema_fields(mappings))


def _section(body: object, *path: str) -> object:
    for key in path:
        if not isinstance(body, dict):
            return None
        body = body.get(key)
    return body


class ElasticsearchMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over Elasticsearch or OpenSearch: index and index
    template names with what decides whether ingestion emits them, read with
    the client ingestion builds. Never documents or search results."""

    # opensearch-py logs every failed request with its URL, the cluster's
    # host and index names, which carry no credential shape to scrub.
    silenced_loggers = ("opensearch",)

    def __init__(self, config: ElasticsearchSourceConfig) -> None:
        self._config = config

    @classmethod
    def for_config(
        cls, config: ElasticsearchSourceConfig
    ) -> "ElasticsearchMetadataProbe":
        return cls(config)

    def _client(self) -> OpenSearch:
        return self._open_once(
            "client",
            lambda: create_elasticsearch_client(self._config),
            close=lambda client: client.close(),
        )

    def _index_bodies(self, names: Sequence[str]) -> Dict[str, Mapping[str, object]]:
        bodies: Dict[str, Mapping[str, object]] = {}
        for batch in _batches(names):
            # Named explicitly, as ingestion fetches each index, so a hidden
            # index the listing returned is read too.
            bodies.update(self._client().indices.get(index=",".join(batch)))
        return bodies

    @probe_method(kind=DatasetSubTypes.ELASTIC_INDEX, row_limit_param="limit")
    def indices(self, limit: int = 200) -> List[Dict[str, object]]:
        """Indices as ingestion lists them (`GET _alias`), including ones
        index_pattern denies: a denied index is reported, not hidden. A data
        stream's backing indices are listed by their own names, which is what
        index_pattern is matched against; `data_stream` names the stream
        ingestion emits them as. `mapped_fields` counts the schema fields
        ingestion would emit; an index with none is not emitted. Metadata
        only: never documents."""
        names = take(sorted(self._client().indices.get_alias()), limit)
        bodies = self._index_bodies(names)
        records: List[Dict[str, object]] = []
        for name in names:
            body = bodies.get(name, {})
            record: Dict[str, object] = {
                "name": name,
                "mapped_fields": _mapped_fields(_section(body, "mappings")),
            }
            data_stream = _section(body, "data_stream")
            if isinstance(data_stream, str):
                record["data_stream"] = data_stream
            records.append(record)
        return records

    @probe_method(kind=DatasetSubTypes.ELASTIC_INDEX_TEMPLATE, row_limit_param="limit")
    def index_templates(self, limit: int = 200) -> List[Dict[str, object]]:
        """Legacy and composable index templates, including ones
        index_template_pattern denies. Ingestion reads them only when
        ingest_index_templates is set, which `probe filter` applies.
        `mapped_fields` is as for `indices`. Metadata only."""
        return take(self._template_records(), limit)

    def _template_records(self) -> Iterator[Dict[str, object]]:
        # Lazy, so mappings past the limit are never converted.
        legacy = self._client().indices.get_template()
        for name in sorted(legacy):
            yield {
                "name": name,
                "template_type": "legacy",
                "mapped_fields": _mapped_fields(_section(legacy[name], "mappings")),
            }
        for template in self._composable_templates():
            name = template.get("name")
            if isinstance(name, str) and name:
                yield {
                    "name": name,
                    "template_type": "composable",
                    "mapped_fields": _mapped_fields(
                        _section(template, "index_template", "template", "mappings")
                    ),
                }

    def _composable_templates(self) -> List[Mapping[str, object]]:
        try:
            response = self._client().indices.get_index_template()
        except TransportError as exc:
            # Ingestion catches any failure here, a lost connection included,
            # logs it and carries on with legacy templates only (servers
            # before 7.8 have no composable templates), so the probe does
            # too, and says so.
            status = exc.status_code if isinstance(exc.status_code, int) else None
            self._warn(
                f"composable index templates could not be listed"
                f"{f' (HTTP {status})' if status else ''}; ingestion skips them "
                f"the same way, so only legacy templates are shown"
            )
            return []
        listed = _section(response, "index_templates")
        if not isinstance(listed, list):
            return []
        return sorted(
            (t for t in listed if isinstance(t, dict)),
            key=lambda t: str(t.get("name")),
        )
