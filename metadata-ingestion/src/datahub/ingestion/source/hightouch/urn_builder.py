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

        if source_details.include_schema_in_urn:
            if schema:
                parts = [part for part in (database, schema, table_name) if part]
                return ".".join(parts)
            # Qualification is requested but no schema is configured: preserve any
            # schema already embedded in the model name (e.g. "schema.table")
            # instead of stripping it, and only prepend the database when the name
            # is not already database-qualified.
            if database and not table_name.startswith(f"{database}."):
                return f"{database}.{table_name}"
            return table_name

        # include_schema_in_urn is false: drop any existing schema/database
        # qualification down to the bare table name so the option actually removes
        # the schema segment, then prepend the configured database.
        bare_table = table_name.split(".")[-1]
        if database:
            return f"{database}.{bare_table}"
        return bare_table

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
            env=source_details.env or self.config.env,
            platform_instance=source_details.platform_instance,
        )

    def normalize_parsed_upstream_urn(
        self, urn: Union[str, DatasetUrn], source: HightouchSourceConnection
    ) -> str:
        # The SQL parser builds upstream URNs straight from the query text, so it
        # always keeps the schema segment. When include_schema_in_urn is false the
        # connector's own source URNs drop the schema, so a parsed upstream would not
        # match the entity the source platform emits. Re-run the parsed table name
        # through the same schema-omission rule (bare table re-qualified with the
        # configured database) so both sides agree. When the flag is true the parser
        # output already matches, so return it unchanged.
        source_details = self._get_cached_source_details(source)
        if source_details.include_schema_in_urn:
            return str(urn)
        try:
            parsed = DatasetUrn.from_string(str(urn))
        except Exception:
            return str(urn)
        database = self._resolve_source_database(source, source_details)
        bare_table = parsed.name.split(".")[-1]
        name = f"{database}.{bare_table}" if database else bare_table
        return str(self.make_upstream_table_urn(name, source))

    def make_destination_urn(
        self, table_name: str, destination: HightouchDestination
    ) -> Union[str, DatasetUrn]:
        dest_details = self._get_cached_destination_details(destination)

        if dest_details.database:
            table_name = f"{dest_details.database.lower()}.{table_name}"

        return DatasetUrn.create_from_ids(
            platform_id=dest_details.platform or destination.type.lower(),
            table_name=table_name,
            env=dest_details.env or self.config.env,
            platform_instance=dest_details.platform_instance,
        )
