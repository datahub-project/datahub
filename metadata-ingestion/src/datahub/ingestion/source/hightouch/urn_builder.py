from typing import Dict, Optional, Union

from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.constants import (
    HIGHTOUCH_PLATFORM,
    SOURCE_CONFIG_KEY_DATABASE,
    SOURCE_CONFIG_KEY_SCHEMA,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.protocols import (
    GetPlatformForDestination,
    GetPlatformForSource,
)
from datahub.metadata.urns import DatasetUrn


class HightouchUrnBuilder:
    def __init__(
        self,
        config: HightouchSourceConfig,
        get_platform_for_source: GetPlatformForSource,
        get_platform_for_destination: GetPlatformForDestination,
    ):
        self.config = config
        self.get_platform_for_source = get_platform_for_source
        self.get_platform_for_destination = get_platform_for_destination
        self._platform_detail_cache: Dict[str, PlatformDetail] = {}

    def _get_cached_source_details(
        self, source: HightouchSourceConnection
    ) -> PlatformDetail:
        cache_key = f"source_{source.id}"
        if cache_key not in self._platform_detail_cache:
            self._platform_detail_cache[cache_key] = self.get_platform_for_source(
                source
            )
        return self._platform_detail_cache[cache_key]

    def _get_cached_destination_details(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        cache_key = f"dest_{destination.id}"
        if cache_key not in self._platform_detail_cache:
            self._platform_detail_cache[cache_key] = self.get_platform_for_destination(
                destination
            )
        return self._platform_detail_cache[cache_key]

    def _resolve_source_database(
        self, source: HightouchSourceConnection, source_details: PlatformDetail
    ) -> str:
        # Single source of truth for a source's database: prefer the operator-
        # configured PlatformDetail.database, falling back to the raw connection
        # blob. This keeps qualified_table_name and make_upstream_table_urn from
        # disagreeing on which database to use.
        if source_details.database:
            return source_details.database
        configuration = source.configuration or {}
        return configuration.get(SOURCE_CONFIG_KEY_DATABASE, "")

    def qualified_table_name(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> str:
        # Qualify with database/schema (honoring include_schema_in_urn) so the name
        # matches the URN produced by the upstream platform's own connector.
        table_name = model.name
        configuration = source.configuration or {}
        schema = configuration.get(SOURCE_CONFIG_KEY_SCHEMA, "")

        source_details = self._get_cached_source_details(source)
        database = self._resolve_source_database(source, source_details)

        if source_details.include_schema_in_urn and schema:
            parts = [part for part in (database, schema, table_name) if part]
            return ".".join(parts)
        if database and "." not in table_name:
            return f"{database}.{table_name}"
        return table_name

    def make_model_urn(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection] = None,
    ) -> Union[str, DatasetUrn]:
        return DatasetUrn.create_from_ids(
            platform_id=HIGHTOUCH_PLATFORM,
            table_name=model.slug,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

    def make_upstream_table_urn(
        self, table_name: str, source: HightouchSourceConnection
    ) -> Union[str, DatasetUrn]:
        source_details = self._get_cached_source_details(source)
        database = self._resolve_source_database(source, source_details)

        if database and "." not in table_name:
            table_name = f"{database}.{table_name}"

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
