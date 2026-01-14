from typing import TYPE_CHECKING, Dict, Optional, Union

from datahub.ingestion.source.hightouch.config import PlatformDetail
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
)
from datahub.metadata.urns import DatasetUrn

if TYPE_CHECKING:
    from datahub.ingestion.source.hightouch.hightouch import HightouchSource


class HightouchUrnBuilder:
    def __init__(self, source: "HightouchSource"):
        self.source = source
        self._platform_detail_cache: Dict[str, PlatformDetail] = {}

    def _get_cached_source_details(
        self, source: HightouchSourceConnection
    ) -> PlatformDetail:
        if source.id not in self._platform_detail_cache:
            self._platform_detail_cache[source.id] = (
                self.source._get_platform_for_source(source)
            )
        return self._platform_detail_cache[source.id]

    def _get_cached_destination_details(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        cache_key = f"dest_{destination.id}"
        if cache_key not in self._platform_detail_cache:
            self._platform_detail_cache[cache_key] = (
                self.source._get_platform_for_destination(destination)
            )
        return self._platform_detail_cache[cache_key]

    def make_model_urn(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection] = None,
    ) -> Union[str, DatasetUrn]:
        return DatasetUrn.create_from_ids(
            platform_id=HIGHTOUCH_PLATFORM,
            table_name=model.slug,
            env=self.source.config.env,
            platform_instance=self.source.config.platform_instance,
        )

    def make_upstream_table_urn(
        self, table_name: str, source: HightouchSourceConnection
    ) -> Union[str, DatasetUrn]:
        source_details = self._get_cached_source_details(source)

        if source_details.database and "." not in table_name:
            table_name = f"{source_details.database}.{table_name}"

        return DatasetUrn.create_from_ids(
            platform_id=source_details.platform or source.type.lower(),
            table_name=table_name,
            env=source_details.env,
            platform_instance=source_details.platform_instance,
        )

    def make_destination_urn(
        self, table_name: str, destination: HightouchDestination
    ) -> Union[str, DatasetUrn]:
        dest_details = self._get_cached_destination_details(destination)

        if dest_details.database:
            table_name = f"{dest_details.database.lower()}.{table_name}"

        return DatasetUrn.create_from_ids(
            platform_id=dest_details.platform or destination.type.lower(),
            table_name=table_name,
            env=dest_details.env,
            platform_instance=dest_details.platform_instance,
        )
