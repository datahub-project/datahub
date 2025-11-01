import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import make_domain_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import gen_data_product
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeMarketplaceAccessEvent,
    SnowflakeMarketplaceListing,
    SnowflakeMarketplacePurchase,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    DatasetUsageStatistics,
    DatasetUserUsageCounts,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import TimeWindowSize

logger = logging.getLogger(__name__)


class SnowflakeMarketplaceHandler(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        identifiers: SnowflakeIdentifierBuilder,
    ) -> None:
        self.config = config
        self.report = report
        self.connection = connection
        self.identifiers = identifiers

        # Cache for marketplace data
        self._marketplace_listings: Dict[str, SnowflakeMarketplaceListing] = {}
        self._marketplace_purchases: Dict[str, SnowflakeMarketplacePurchase] = {}

    def get_marketplace_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Generate work units for marketplace data"""

        # First, load marketplace data
        self._load_marketplace_data()

        # 1. Create Data Products for marketplace listings
        if self.config.include_marketplace_listings:
            yield from self._create_marketplace_data_products()

        # 2. Enhance purchased datasets with marketplace metadata
        if self.config.include_marketplace_purchases:
            yield from self._enhance_purchased_datasets()

        # 3. Create lineage between listings and purchased data
        if (
            self.config.include_marketplace_listings
            and self.config.include_marketplace_purchases
        ):
            yield from self._create_marketplace_lineage()

        # 4. Add marketplace usage statistics
        if self.config.include_marketplace_usage:
            yield from self._create_marketplace_usage_statistics()

    def _resolve_domain_for_listing(self, listing_name: str) -> Optional[str]:
        """Resolve domain URN for a marketplace listing based on regex patterns"""
        for domain_urn, pattern in self.config.marketplace_domain.items():
            if pattern.allowed(listing_name):
                # Normalize the domain URN
                if not domain_urn.startswith("urn:li:domain:"):
                    domain_urn = make_domain_urn(domain_urn)
                return domain_urn
        return None

    def _load_marketplace_data(self) -> None:
        """Load marketplace listings and purchases"""

        if self.config.include_marketplace_listings:
            self._load_marketplace_listings()

        if self.config.include_marketplace_purchases:
            self._load_marketplace_purchases()

    def _load_marketplace_listings(self) -> None:
        """Load marketplace listings from Snowflake"""
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_listings())

            for row in cur:
                listing = SnowflakeMarketplaceListing(
                    listing_global_name=row["LISTING_GLOBAL_NAME"],
                    listing_display_name=row["LISTING_DISPLAY_NAME"],
                    provider_name=row["PROVIDER_NAME"],
                    category=row.get("CATEGORY"),
                    description=row.get("DESCRIPTION"),
                    listing_created_on=row.get("LISTING_CREATED_ON"),
                    listing_updated_on=row.get("LISTING_UPDATED_ON"),
                    is_personal_data=row.get("IS_PERSONAL_DATA", False),
                    is_free=row.get("IS_FREE", False),
                )

                if self.config.marketplace_listing_pattern.allowed(
                    listing.listing_global_name
                ):
                    self._marketplace_listings[listing.listing_global_name] = listing
                    self.report.report_marketplace_listing_scanned()
                else:
                    self.report.report_dropped(listing.listing_global_name)

        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.structured_reporter.warning(
                    "Failed to get marketplace listings - insufficient permissions",
                    context="Make sure the role has access to SNOWFLAKE.ACCOUNT_USAGE.MARKETPLACE_LISTINGS",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get marketplace listings",
                    exc=e,
                )

    def _load_marketplace_purchases(self) -> None:
        """Load marketplace purchases from Snowflake"""
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_purchases())

            for row in cur:
                purchase = SnowflakeMarketplacePurchase(
                    listing_global_name=row["LISTING_GLOBAL_NAME"],
                    listing_display_name=row["LISTING_DISPLAY_NAME"],
                    provider_name=row["PROVIDER_NAME"],
                    purchase_date=row["PURCHASE_DATE"],
                    database_name=row["DATABASE_NAME"],
                    is_auto_fulfill=row.get("IS_AUTO_FULFILL", False),
                    purchase_status=row["PURCHASE_STATUS"],
                )

                self._marketplace_purchases[purchase.database_name] = purchase
                self.report.report_marketplace_purchase_scanned()

        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.structured_reporter.warning(
                    "Failed to get marketplace purchases - insufficient permissions",
                    context="Make sure the role has access to SNOWFLAKE.ACCOUNT_USAGE.MARKETPLACE_PURCHASES",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get marketplace purchases",
                    exc=e,
                )

    def _create_marketplace_data_products(self) -> Iterable[MetadataWorkUnit]:
        """Create Data Product entities for marketplace listings"""

        for listing in self._marketplace_listings.values():
            data_product_key = self.identifiers.gen_marketplace_data_product_key(
                listing.listing_global_name
            )

            # Create custom properties dictionary
            custom_properties = {
                "marketplace_listing": "true",
                "provider_name": listing.provider_name,
                "category": listing.category or "",
                "is_personal_data": str(listing.is_personal_data),
                "is_free": str(listing.is_free),
                "listing_global_name": listing.listing_global_name,
            }

            # Add dates if available
            if listing.listing_created_on:
                custom_properties["listing_created_on"] = (
                    listing.listing_created_on.isoformat()
                )
            if listing.listing_updated_on:
                custom_properties["listing_updated_on"] = (
                    listing.listing_updated_on.isoformat()
                )

            # Resolve domain for this listing
            domain_urn = self._resolve_domain_for_listing(listing.listing_display_name)

            # Use the gen_data_product function from mcp_builder
            yield from gen_data_product(
                data_product_key=data_product_key,
                name=listing.listing_display_name,
                description=listing.description,
                custom_properties=custom_properties,
                domain_urn=domain_urn,
            )

            self.report.report_marketplace_data_product_created()

    def _enhance_purchased_datasets(self) -> Iterable[MetadataWorkUnit]:
        """Enhance regular dataset metadata with marketplace purchase info"""

        for purchase in self._marketplace_purchases.values():
            # Get the database URN using existing identifier logic
            dataset_identifier = self.identifiers.snowflake_identifier(
                purchase.database_name
            )
            database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

            # Create marketplace-enhanced properties
            marketplace_properties = {
                "marketplace_purchase": "true",
                "marketplace_listing_global_name": purchase.listing_global_name,
                "marketplace_provider": purchase.provider_name,
                "purchase_date": purchase.purchase_date.isoformat(),
                "purchase_status": purchase.purchase_status,
                "is_auto_fulfill": str(purchase.is_auto_fulfill),
            }

            # If we have the corresponding listing, add the data product URN
            if purchase.listing_global_name in self._marketplace_listings:
                data_product_urn = self.identifiers.gen_marketplace_data_product_urn(
                    purchase.listing_global_name
                )
                marketplace_properties["data_product_source"] = data_product_urn

            # Enhance the dataset properties
            enhanced_properties = DatasetProperties(
                name=purchase.listing_display_name,
                qualifiedName=f"{purchase.database_name}",
                customProperties=marketplace_properties,
                description=f"Marketplace dataset from {purchase.provider_name}",
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=database_urn, aspect=enhanced_properties
            ).as_workunit()

            self.report.report_marketplace_dataset_enhanced()

    def _create_marketplace_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Create lineage from marketplace listings to purchased datasets"""

        for purchase in self._marketplace_purchases.values():
            if purchase.listing_global_name not in self._marketplace_listings:
                continue

            # Generate URNs using existing identifier logic
            data_product_urn = self.identifiers.gen_marketplace_data_product_urn(
                purchase.listing_global_name
            )

            dataset_identifier = self.identifiers.snowflake_identifier(
                purchase.database_name
            )
            database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

            # Create upstream lineage
            upstream_lineage = UpstreamLineage(
                upstreams=[
                    Upstream(
                        dataset=data_product_urn,
                        type=DatasetLineageType.COPY,
                    )
                ]
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=database_urn, aspect=upstream_lineage
            ).as_workunit()

    def _create_marketplace_usage_statistics(self) -> Iterable[MetadataWorkUnit]:
        """Create marketplace usage statistics"""

        start_time_millis = int(self.config.start_time.timestamp() * 1000)
        end_time_millis = int(self.config.end_time.timestamp() * 1000)

        try:
            cur = self.connection.query(
                SnowflakeQuery.marketplace_listing_access_history(
                    start_time_millis, end_time_millis
                )
            )

            # Group usage events by database and time bucket
            usage_by_database: Dict[str, List[SnowflakeMarketplaceAccessEvent]] = (
                defaultdict(list)
            )

            for row in cur:
                event = SnowflakeMarketplaceAccessEvent(
                    event_timestamp=row["EVENT_TIMESTAMP"],
                    listing_global_name=row["LISTING_GLOBAL_NAME"],
                    listing_display_name=row["LISTING_DISPLAY_NAME"],
                    user_name=row["USER_NAME"],
                    database_name=row["DATABASE_NAME"],
                    schema_name=row.get("SCHEMA_NAME"),
                    table_name=row.get("TABLE_NAME"),
                    query_id=row.get("QUERY_ID"),
                    bytes_accessed=row.get("BYTES_ACCESSED"),
                    rows_accessed=row.get("ROWS_ACCESSED"),
                )

                usage_by_database[event.database_name].append(event)
                self.report.marketplace_usage_events_processed += 1

            # Create usage statistics for each database
            for database_name, events in usage_by_database.items():
                if database_name not in self._marketplace_purchases:
                    continue

                dataset_identifier = self.identifiers.snowflake_identifier(
                    database_name
                )
                database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

                # Aggregate usage data
                unique_users = set(event.user_name for event in events)
                total_queries = len(
                    set(event.query_id for event in events if event.query_id)
                )

                # Create user usage counts
                user_counts = []
                user_query_counts: Dict[str, int] = defaultdict(int)
                for event in events:
                    if event.query_id:
                        user_query_counts[event.user_name] += 1

                for user_name, query_count in user_query_counts.items():
                    user_email = None
                    if self.config.email_domain:
                        user_email = f"{user_name}@{self.config.email_domain}".lower()

                    if user_email and self.config.user_email_pattern.allowed(
                        user_email
                    ):
                        user_counts.append(
                            DatasetUserUsageCounts(
                                user=make_user_urn(
                                    self.identifiers.get_user_identifier(
                                        user_name, user_email
                                    )
                                ),
                                count=query_count,
                                userEmail=user_email,
                            )
                        )

                # Create usage statistics
                usage_stats = DatasetUsageStatistics(
                    timestampMillis=start_time_millis,
                    eventGranularity=TimeWindowSize(
                        unit=self.config.bucket_duration, multiple=1
                    ),
                    totalSqlQueries=total_queries,
                    uniqueUserCount=len(unique_users),
                    userCounts=sorted(user_counts, key=lambda x: x.user),
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=database_urn, aspect=usage_stats
                ).as_workunit()

        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.structured_reporter.warning(
                    "Failed to get marketplace usage statistics - insufficient permissions",
                    context="Make sure the role has access to SNOWFLAKE.ORGANIZATION_USAGE.MARKETPLACE_LISTING_ACCESS_HISTORY",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get marketplace usage statistics",
                    exc=e,
                )
