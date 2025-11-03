import json
import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set

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
        """Load internal marketplace listings from Snowflake using SHOW AVAILABLE LISTINGS"""
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_listings())

            for row in cur:
                # SHOW AVAILABLE LISTINGS returns: name, created_on, listing_global_name, title, description, provider, category
                listing = SnowflakeMarketplaceListing(
                    name=row.get("name", ""),
                    listing_global_name=row.get("listing_global_name", ""),
                    title=row.get("title", ""),
                    provider=row.get("provider", ""),
                    category=row.get("category"),
                    description=row.get("description"),
                    created_on=row.get("created_on"),
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
                    context="Make sure the role has privileges to run SHOW AVAILABLE LISTINGS",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get marketplace listings",
                    exc=e,
                )

    def _load_marketplace_purchases(self) -> None:
        """Load databases created from internal marketplace listings"""
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_purchases())

            for row in cur:
                purchase = SnowflakeMarketplacePurchase(
                    database_name=row["DATABASE_NAME"],
                    purchase_date=row["PURCHASE_DATE"],
                    owner=row["OWNER"],
                    comment=row.get("COMMENT"),
                )

                self._marketplace_purchases[purchase.database_name] = purchase
                self.report.report_marketplace_purchase_scanned()

        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.structured_reporter.warning(
                    "Failed to get marketplace purchases - insufficient permissions",
                    context="Make sure the role has access to SNOWFLAKE.ACCOUNT_USAGE.DATABASES",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get marketplace purchases",
                    exc=e,
                )

    def _create_marketplace_data_products(self) -> Iterable[MetadataWorkUnit]:
        """Create Data Product entities for internal marketplace listings"""

        for listing in self._marketplace_listings.values():
            data_product_key = self.identifiers.gen_marketplace_data_product_key(
                listing.listing_global_name
            )

            # Create custom properties dictionary
            custom_properties = {
                "marketplace_listing": "true",
                "marketplace_type": "internal",  # Internal/private data exchange
                "provider": listing.provider,
                "category": listing.category or "",
                "listing_global_name": listing.listing_global_name,
                "listing_name": listing.name,
            }

            # Add dates if available
            if listing.created_on:
                custom_properties["listing_created_on"] = listing.created_on.isoformat()

            # Resolve domain for this listing
            domain_urn = self._resolve_domain_for_listing(listing.title)

            # Use the gen_data_product function from mcp_builder
            yield from gen_data_product(
                data_product_key=data_product_key,
                name=listing.title,
                description=listing.description
                or f"Internal marketplace listing from {listing.provider}",
                custom_properties=custom_properties,
                domain_urn=domain_urn,
            )

            self.report.report_marketplace_data_product_created()

    def _find_listing_for_purchase(
        self, purchase: SnowflakeMarketplacePurchase
    ) -> Optional[str]:
        """
        Heuristically find the listing_global_name for a purchased database.
        This is needed since the ORIGIN column doesn't exist in SNOWFLAKE.ACCOUNT_USAGE.DATABASES.

        Strategy: Check if any listing_global_name appears in the database comment.
        """
        if not purchase.comment:
            return None

        # Try to find any listing whose global name appears in the comment
        for listing_global_name in self._marketplace_listings:
            if listing_global_name.lower() in purchase.comment.lower():
                return listing_global_name

        return None

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
                "database_type": "IMPORTED_DATABASE",
                "owner": purchase.owner,
                "purchase_date": purchase.purchase_date.isoformat(),
            }

            if purchase.comment:
                marketplace_properties["comment"] = purchase.comment

            # Try to match with known listings by searching comment
            listing_global_name = self._find_listing_for_purchase(purchase)

            if (
                listing_global_name
                and listing_global_name in self._marketplace_listings
            ):
                data_product_urn = self.identifiers.gen_marketplace_data_product_urn(
                    listing_global_name
                )
                marketplace_properties["data_product_source"] = data_product_urn
                marketplace_properties["marketplace_listing_global_name"] = (
                    listing_global_name
                )

                listing = self._marketplace_listings[listing_global_name]
                marketplace_properties["marketplace_provider"] = listing.provider
                purchase_status = "INSTALLED"
            else:
                purchase_status = "UNKNOWN_LISTING"

            marketplace_properties["purchase_status"] = purchase_status

            # Enhance the dataset properties
            enhanced_properties = DatasetProperties(
                name=purchase.database_name,
                qualifiedName=purchase.database_name.upper(),
                customProperties=marketplace_properties,
                description=purchase.comment
                or "Database imported from internal marketplace",
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=database_urn, aspect=enhanced_properties
            ).as_workunit()

            self.report.report_marketplace_dataset_enhanced()

    def _create_marketplace_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Create lineage from marketplace listings to purchased datasets"""

        for purchase in self._marketplace_purchases.values():
            # Try to match with a known listing
            listing_global_name = self._find_listing_for_purchase(purchase)

            if (
                not listing_global_name
                or listing_global_name not in self._marketplace_listings
            ):
                continue

            # Generate URNs using existing identifier logic
            data_product_urn = self.identifiers.gen_marketplace_data_product_urn(
                listing_global_name
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

    def _parse_share_objects(self, share_objects_str: str) -> List[str]:
        """
        Parse SHARE_OBJECTS_ACCESSED JSON to extract database names.

        SHARE_OBJECTS_ACCESSED is a JSON array of objects like:
        [{"objectName": "DB.SCHEMA.TABLE", "objectDomain": "Table", ...}, ...]

        We extract the database names (first part of objectName).
        """
        try:
            objects = (
                json.loads(share_objects_str)
                if isinstance(share_objects_str, str)
                else share_objects_str
            )
            databases: Set[str] = set()

            if not isinstance(objects, list):
                return []

            for obj in objects:
                if isinstance(obj, dict):
                    # Extract database from objectName (format: "DB"."SCHEMA"."TABLE")
                    object_name = obj.get("objectName", "")
                    if object_name:
                        parts = object_name.split(".")
                        if parts:
                            # Remove quotes and get database name
                            databases.add(parts[0].strip('"').strip("'"))

            return list(databases)
        except (json.JSONDecodeError, AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse share objects: {e}")
            return []

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

            # Group usage events by listing and database
            usage_by_listing_db: Dict[
                str, Dict[str, List[SnowflakeMarketplaceAccessEvent]]
            ] = defaultdict(lambda: defaultdict(list))

            for row in cur:
                # Parse share objects to extract database names
                databases = self._parse_share_objects(
                    row.get("SHARE_OBJECTS_ACCESSED", "[]")
                )

                # Parse the share objects for the event
                share_objects_raw = row.get("SHARE_OBJECTS_ACCESSED", "[]")
                share_objects: List[Dict[str, Any]] = (
                    json.loads(share_objects_raw)
                    if isinstance(share_objects_raw, str)
                    else share_objects_raw
                )

                event = SnowflakeMarketplaceAccessEvent(
                    event_timestamp=row["EVENT_TIMESTAMP"],
                    listing_global_name=row["LISTING_GLOBAL_NAME"],
                    user_name=row["USER_NAME"],
                    query_id=row["QUERY_ID"],
                    share_name=row["SHARE_NAME"],
                    share_objects_accessed=share_objects,
                )

                # Group by listing and database
                listing_name = event.listing_global_name
                for db_name in databases:
                    usage_by_listing_db[listing_name][db_name].append(event)
                    self.report.marketplace_usage_events_processed += 1

            # Create usage statistics for each database accessed via marketplace
            for listing_name, db_events in usage_by_listing_db.items():
                # Check if this listing is in our known listings
                if listing_name not in self._marketplace_listings:
                    logger.debug(f"Skipping usage for unknown listing: {listing_name}")
                    continue

                for database_name, events in db_events.items():
                    # Check if this database is a known purchase
                    if database_name not in self._marketplace_purchases:
                        logger.debug(
                            f"Skipping usage for unknown database: {database_name}"
                        )
                        continue

                    dataset_identifier = self.identifiers.snowflake_identifier(
                        database_name
                    )
                    database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

                    # Aggregate usage data
                    unique_users: Set[str] = set(event.user_name for event in events)
                    total_queries = len(set(event.query_id for event in events))

                    # Create user usage counts
                    user_counts = []
                    user_query_counts: Dict[str, int] = defaultdict(int)
                    for event in events:
                        user_query_counts[event.user_name] += 1

                    for user_name, query_count in user_query_counts.items():
                        user_email = None
                        if self.config.email_domain:
                            user_email = (
                                f"{user_name}@{self.config.email_domain}".lower()
                            )

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
                    context="Make sure the role has access to SNOWFLAKE.DATA_SHARING_USAGE.LISTING_ACCESS_HISTORY",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get marketplace usage statistics",
                    exc=e,
                )
