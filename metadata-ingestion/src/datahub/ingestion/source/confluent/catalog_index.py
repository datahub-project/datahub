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
CatalogConfigType = TypeVar("CatalogConfigType", bound=ConfluentStreamCatalogConfig)


class CatalogIndex(Generic[CatalogEntityType, CatalogReportType, CatalogConfigType]):
    """Fetches one Stream Catalog entity type once and indexes it by name.

    The kafka and kafka-connect sources both need this; subclasses override `_filter`,
    `_warn_ambiguous` and `_record_indexed` for their entity type.
    """

    def __init__(
        self,
        config: CatalogConfigType,
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
            context=f"{self._entity_label}={name}",
        )

    def _record_indexed(self, count: int) -> None:
        pass
