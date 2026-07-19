from typing import List, Optional, Protocol, Union

from datahub.ingestion.source.hightouch.config import PlatformDetail
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator


class GetSource(Protocol):
    def __call__(self, source_id: str) -> Optional[HightouchSourceConnection]: ...


class GetModel(Protocol):
    def __call__(self, model_id: str) -> Optional[HightouchModel]: ...


class GetDestination(Protocol):
    def __call__(self, destination_id: str) -> Optional[HightouchDestination]: ...


class GetPlatformForSource(Protocol):
    def __call__(self, source: HightouchSourceConnection) -> PlatformDetail: ...


class GetPlatformForDestination(Protocol):
    def __call__(self, destination: HightouchDestination) -> PlatformDetail: ...


class GetAggregatorForPlatform(Protocol):
    def __call__(
        self, source_platform: PlatformDetail
    ) -> Optional[SqlParsingAggregator]: ...


class ExtractTableUrns(Protocol):
    def __call__(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> List[str]: ...


class GetOutletUrnForSync(Protocol):
    def __call__(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Union[str, DatasetUrn, None]: ...
