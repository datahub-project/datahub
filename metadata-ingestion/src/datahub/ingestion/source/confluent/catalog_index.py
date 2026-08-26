from typing import Generic, List, Optional, Type, TypeVar

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.confluent.models import (
    CatalogEntityType,
    NameIndex,
    index_by_name,
)

CatalogReportType = TypeVar("CatalogReportType", bound=SourceReport)


class CatalogIndex(Generic[CatalogEntityType, CatalogReportType]):
    """Lazy, name-indexed view over one Confluent Stream Catalog entity type.

    Every catalog-backed source repeats the same boilerplate: fetch a single entity
    type once, remember whether the read was complete, index it by name (reporting
    empty and duplicate names), and close the client. That lives here so each source
    only supplies its entity type, query, and any entity-specific behaviour.

    Subclasses pass the query/root key/model/label to `__init__` and may override
    `_filter` (narrow the fetched entities), `_warn_ambiguous` (duplicate-name
    wording) and `_record_indexed` (bump the source's report counter).
    """

    def __init__(
        self,
        config: ConfluentStreamCatalogConfig,
        report: CatalogReportType,
        *,
        query: str,
        root_key: str,
        model: Type[CatalogEntityType],
        entity_label: str,
        client: Optional[ConfluentStreamCatalogClient] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.client = client or ConfluentStreamCatalogClient(config, report)
        self._query = query
        self._root_key = root_key
        self._model = model
        self._entity_label = entity_label
        self._index: Optional[NameIndex[CatalogEntityType]] = None
        self._complete = True

    def is_complete(self) -> bool:
        self._ensure_indexed()
        return self._complete

    def close(self) -> None:
        self.client.close()

    def _get(self, name: str) -> Optional[CatalogEntityType]:
        return self._ensure_indexed().get(name)

    def _ensure_indexed(self) -> NameIndex[CatalogEntityType]:
        if self._index is None:
            result = self.client.fetch_entities(
                self._query, self._root_key, self._model
            )
            self._complete = result.complete
            entities = self._filter(list(result.entities))
            index = index_by_name(entities)
            index.report_issues(self.report, self._entity_label)
            for name, candidates in index.ambiguous.items():
                self._warn_ambiguous(name, candidates)
            self._record_indexed(len(index.by_name))
            self._index = index
        return self._index

    def _filter(self, entities: List[CatalogEntityType]) -> List[CatalogEntityType]:
        return entities

    def _warn_ambiguous(self, name: str, candidates: List[CatalogEntityType]) -> None:
        self.report.warning(
            message="Skipping Stream Catalog metadata for a name that the catalog "
            "reports more than once in this environment.",
            context=f"entity={self._entity_label}, name={name}",
        )

    def _record_indexed(self, count: int) -> None:
        pass
