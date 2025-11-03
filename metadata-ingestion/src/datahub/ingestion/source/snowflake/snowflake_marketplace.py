import json
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_domain_urn, make_group_urn, make_user_urn
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
    SnowflakeProviderShare,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    OwnershipTypeClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import DataTypeUrn, EntityTypeUrn, StructuredPropertyUrn

if TYPE_CHECKING:
    from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


class SnowflakeMarketplaceHandler(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        identifiers: SnowflakeIdentifierBuilder,
        domain_registry: Optional["DomainRegistry"] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.connection = connection
        self.identifiers = identifiers
        self.domain_registry = domain_registry

        # Cache for marketplace data
        self._marketplace_listings: Dict[str, SnowflakeMarketplaceListing] = {}
        self._marketplace_purchases: Dict[str, SnowflakeMarketplacePurchase] = {}
        self._provider_shares: Dict[
            str, SnowflakeProviderShare
        ] = {}  # For provider mode

    def get_marketplace_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Generate work units for marketplace data"""
        if not self.config.include_internal_marketplace:
            return

        # 0. Create structured property definitions if needed
        if self.config.marketplace_properties_as_structured_properties:
            yield from self._create_marketplace_structured_property_definitions()

        # First, load marketplace data
        self._load_marketplace_data()

        # 1. Create Data Products for marketplace listings
        yield from self._create_marketplace_data_products()

        # 2. Enhance purchased datasets with marketplace metadata
        yield from self._enhance_purchased_datasets()

        # 3. Add marketplace usage statistics
        # Note: Marketplace usage is tracked independently from general usage_stats
        yield from self._create_marketplace_usage_statistics()

    def _create_marketplace_structured_property_definitions(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create structured property definitions for marketplace custom properties.

        These are created once and define the schema for marketplace properties that
        can be attached to Data Products and Datasets.
        """
        from datahub.emitter.mce_builder import get_sys_time

        # Define marketplace structured properties
        marketplace_properties = [
            {
                "id": "snowflake.marketplace.provider",
                "display_name": "Marketplace Provider",
                "description": "The provider/publisher of the internal marketplace listing",
                "value_type": "string",
            },
            {
                "id": "snowflake.marketplace.category",
                "display_name": "Marketplace Category",
                "description": "The category of the internal marketplace listing",
                "value_type": "string",
            },
            {
                "id": "snowflake.marketplace.listing_name",
                "display_name": "Marketplace Listing Name",
                "description": "The internal name of the marketplace listing",
                "value_type": "string",
            },
            {
                "id": "snowflake.marketplace.listing_global_name",
                "display_name": "Marketplace Listing Global Name",
                "description": "The globally unique identifier for the marketplace listing",
                "value_type": "string",
            },
            {
                "id": "snowflake.marketplace.listing_created_on",
                "display_name": "Marketplace Listing Created Date",
                "description": "The date when the marketplace listing was created",
                "value_type": "date",
            },
            {
                "id": "snowflake.marketplace.type",
                "display_name": "Marketplace Type",
                "description": "The type of marketplace (internal for private data sharing)",
                "value_type": "string",
            },
        ]

        for prop_def in marketplace_properties:
            urn = StructuredPropertyUrn(prop_def["id"]).urn()

            # Map value_type to DataHub type URNs
            type_mapping = {
                "string": "datahub.string",
                "date": "datahub.date",
            }
            value_type_urn = DataTypeUrn(type_mapping[prop_def["value_type"]]).urn()

            aspect = StructuredPropertyDefinition(
                qualifiedName=prop_def["id"],
                displayName=prop_def["display_name"],
                description=prop_def["description"],
                valueType=value_type_urn,
                entityTypes=[
                    EntityTypeUrn("datahub.dataProduct").urn(),
                    EntityTypeUrn("datahub.dataset").urn(),
                ],
                lastModified=AuditStamp(
                    time=get_sys_time(), actor="urn:li:corpuser:datahub"
                ),
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=aspect,
                changeType=ChangeTypeClass.CREATE,
                headers={"If-None-Match": "*"},  # Only create if doesn't exist
            ).as_workunit()

    def _resolve_domain_for_listing(
        self, listing: SnowflakeMarketplaceListing, purchased_databases: List[str]
    ) -> Optional[str]:
        """
        Resolve domain URN for a marketplace listing based on purchased database names.

        Uses the existing `domain` config pattern which matches against database names.
        For example: {'Finance': {'allow': ['^FINANCE_.*', '^FIN_.*']}}

        Args:
            listing: The marketplace listing
            purchased_databases: List of database names purchased from this listing

        Returns:
            Domain URN if a match is found, None otherwise
        """
        if not self.domain_registry or not self.config.domain:
            return None

        # Try to match any of the purchased database names against domain patterns
        for db_name in purchased_databases:
            for domain_name, pattern in self.config.domain.items():
                if pattern.allowed(db_name):
                    # Use domain registry to resolve domain name to URN
                    domain_urn = self.domain_registry.get_domain_urn(domain_name)
                    return make_domain_urn(domain_urn)

        return None

    def _load_marketplace_data(self) -> None:
        """Load marketplace listings and purchases/provider shares based on mode"""
        self._load_marketplace_listings()  # Always load listings

        mode = self.config.marketplace_mode
        if mode in ["consumer", "both"]:
            self._load_marketplace_purchases()  # Load imported databases
        if mode in ["provider", "both"]:
            self._load_provider_shares()  # Load OUTBOUND shares

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

                if self.config.internal_marketplace_listing_pattern.allowed(
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

    def _load_provider_shares(self) -> None:
        """Load OUTBOUND shares for provider mode (databases you're sharing)"""
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_shares())

            for row in cur:
                # Filter for OUTBOUND shares only
                if row.get("kind") != "OUTBOUND":
                    continue

                # Only track shares associated with marketplace listings
                listing_global_name = row.get("listing_global_name")
                if not listing_global_name:
                    continue

                # Check if this listing is in our filtered set
                if listing_global_name not in self._marketplace_listings:
                    continue

                provider_share = SnowflakeProviderShare(
                    share_name=row["name"],
                    source_database=row.get("database_name", ""),
                    listing_global_name=listing_global_name,
                    created_on=row.get("created_on"),
                    owner=row.get("owner"),
                    comment=row.get("comment"),
                )

                # Index by listing_global_name for easy lookup
                self._provider_shares[listing_global_name] = provider_share
                logger.info(
                    f"Found provider share {provider_share.share_name} for listing {listing_global_name}"
                )

        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.structured_reporter.warning(
                    "Failed to get provider shares - insufficient permissions",
                    context="Make sure the role has privileges to run SHOW SHARES",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    "Failed to get provider shares",
                    exc=e,
                )

    def _normalize_owner_urn(self, owner: str) -> str:
        """Return a valid corpuser/corpGroup URN from a flexible owner string."""
        if not owner:
            return owner
        val = owner.strip()
        if val.startswith("urn:li:corpuser:") or val.startswith("urn:li:corpGroup:"):
            return val
        # Support explicit prefixes
        if val.lower().startswith("user:"):
            return make_user_urn(val.split(":", 1)[1])
        if val.lower().startswith("group:"):
            return make_group_urn(val.split(":", 1)[1])
        # Default to user
        return make_user_urn(val)

    def _resolve_owners_for_listing(
        self, listing: SnowflakeMarketplaceListing
    ) -> List[str]:
        """Determine owner URNs for a given listing based on config mappings."""
        owners: List[str] = []
        title = listing.title or ""
        global_name = listing.listing_global_name or ""
        provider = listing.provider or ""

        # Listing-based patterns (match title or global name)
        try:
            # Match against listing title, global name, or provider
            for (
                pattern,
                owner_vals,
            ) in self.config.internal_marketplace_owner_patterns.items():
                if (
                    re.search(pattern, title, flags=re.IGNORECASE)
                    or re.search(pattern, global_name, flags=re.IGNORECASE)
                    or re.search(pattern, provider, flags=re.IGNORECASE)
                ):
                    owners.extend(self._normalize_owner_urn(o) for o in owner_vals)
        except Exception as e:
            logger.debug(f"Owner mapping failed for listing {global_name}: {e}")

        # De-duplicate while preserving order
        deduped: List[str] = []
        seen: Set[str] = set()
        for o in owners:
            if o and o not in seen:
                deduped.append(o)
                seen.add(o)
        return deduped

    def _enrich_listing_custom_properties(
        self, listing: SnowflakeMarketplaceListing, props: Dict[str, str]
    ) -> Dict[str, Any]:
        """Optionally enrich custom properties via DESCRIBE AVAILABLE LISTING (best-effort).

        Returns a dict with optional keys:
          - owners: List[str] of owner identifiers derived from details
          - external_url: Optional[str] preferred documentation URL
        """
        result: Dict[str, Any] = {"owners": [], "external_url": None}
        if not self.config.fetch_internal_marketplace_listing_details:
            return result
        try:
            query = f"DESCRIBE AVAILABLE LISTING '{listing.listing_global_name}'"
            cur = self.connection.query(query)
            for row in cur:
                # Common DESCRIBE pattern: PROPERTY / VALUE columns
                if "property" in row and "value" in row:
                    key = str(row["property"]).strip()
                    val = row.get("value")
                    if key and val is not None:
                        norm_key = "listing_detail_" + key.lower().replace(
                            " ", "_"
                        ).replace("-", "_")
                        props[norm_key] = str(val)
                else:
                    # Fallback: add all primitive columns
                    for k, v in row.items():
                        if v is None:
                            continue
                        norm_key = "listing_detail_" + str(k).lower().replace(" ", "_")
                        props[norm_key] = str(v)

            # After collection, map recognized fields from props
            def first(*keys: str) -> Optional[str]:
                for k in keys:
                    v = props.get(k)
                    if v:
                        return v
                return None

            documentation_url = first(
                "listing_detail_documentation_url", "listing_detail_docs_url"
            )
            quickstart_url = first(
                "listing_detail_quickstart_url",
                "listing_detail_quick_start_url",
                "listing_detail_quickstart",
            )
            support_contact = first(
                "listing_detail_contact",
                "listing_detail_support_contact",
                "listing_detail_support_name",
            )
            support_email = first(
                "listing_detail_support_email", "listing_detail_contact_email"
            )
            support_url = first(
                "listing_detail_support_url", "listing_detail_contact_url"
            )
            request_approver = first(
                "listing_detail_request_approver",
                "listing_detail_approver",
                "listing_detail_listing_approver",
            )

            # Persist mapped properties
            if documentation_url:
                props["documentation_url"] = documentation_url
            if quickstart_url:
                props["quickstart_url"] = quickstart_url
            if support_contact:
                props["support_contact"] = support_contact
            if support_email:
                props["support_email"] = support_email
            if support_url:
                props["support_url"] = support_url
            if request_approver:
                props["request_approver"] = request_approver

            owners_from_details: List[str] = []
            if support_email:
                owners_from_details.append(support_email)
            if request_approver:
                owners_from_details.append(request_approver)
            if support_contact and "@" in support_contact:
                owners_from_details.append(support_contact)

            result["owners"] = owners_from_details
            result["external_url"] = documentation_url or support_url

        except Exception as e:
            logger.debug(
                f"DESCRIBE AVAILABLE LISTING enrichment failed for {listing.listing_global_name}: {e}"
            )
        return result

    def _create_marketplace_data_products(self) -> Iterable[MetadataWorkUnit]:
        """Create Data Product entities for internal marketplace listings"""

        for listing in self._marketplace_listings.values():
            data_product_key = self.identifiers.gen_marketplace_data_product_key(
                listing.listing_global_name
            )
            data_product_key.as_urn()

            # Find all assets for this listing (purchased databases and/or provider source databases)
            asset_urns: List[str] = []
            purchased_db_names: List[str] = []

            # Consumer mode: Find purchased/imported databases
            if self.config.marketplace_mode in ["consumer", "both"]:
                for purchase in self._marketplace_purchases.values():
                    listing_global_name = self._find_listing_for_purchase(purchase)
                    if (
                        listing_global_name
                        and listing_global_name == listing.listing_global_name
                    ):
                        dataset_identifier = self.identifiers.snowflake_identifier(
                            purchase.database_name
                        )
                        database_urn = self.identifiers.gen_dataset_urn(
                            dataset_identifier
                        )
                        asset_urns.append(database_urn)
                        purchased_db_names.append(purchase.database_name)

            # Provider mode: Find source databases being shared
            if self.config.marketplace_mode in ["provider", "both"]:
                provider_share = self._provider_shares.get(listing.listing_global_name)
                if provider_share and provider_share.source_database:
                    dataset_identifier = self.identifiers.snowflake_identifier(
                        provider_share.source_database
                    )
                    database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)
                    asset_urns.append(database_urn)
                    purchased_db_names.append(provider_share.source_database)
                    logger.info(
                        f"Provider mode: Linked source database {provider_share.source_database} "
                        f"to Data Product for listing {listing.listing_global_name}"
                    )

            # Build properties (either custom or structured based on config)
            custom_properties: Optional[Dict[str, str]] = None
            structured_properties: Optional[Dict[str, str]] = None

            if self.config.marketplace_properties_as_structured_properties:
                # Use structured properties for searchability
                structured_properties = {
                    "snowflake.marketplace.provider": listing.provider,
                    "snowflake.marketplace.category": listing.category or "",
                    "snowflake.marketplace.listing_global_name": listing.listing_global_name,
                    "snowflake.marketplace.listing_name": listing.name,
                    "snowflake.marketplace.type": "internal",
                    "snowflake.marketplace.mode": self.config.marketplace_mode,
                }
                if listing.created_on:
                    structured_properties[
                        "snowflake.marketplace.listing_created_on"
                    ] = listing.created_on.isoformat()
                # Keep minimal custom properties for backward compatibility
                custom_properties = {
                    "marketplace_listing": "true",
                    "marketplace_mode": self.config.marketplace_mode,
                }
            else:
                # Use custom properties (default)
                custom_properties = {
                    "marketplace_listing": "true",
                    "marketplace_type": "internal",  # Internal/private data exchange
                    "marketplace_mode": self.config.marketplace_mode,  # consumer/provider/both
                    "provider": listing.provider,
                    "category": listing.category or "",
                    "listing_global_name": listing.listing_global_name,
                    "listing_name": listing.name,
                }
                # Add dates if available
                if listing.created_on:
                    custom_properties["listing_created_on"] = (
                        listing.created_on.isoformat()
                    )

            # Optional: add DESCRIBE details
            details = self._enrich_listing_custom_properties(
                listing, custom_properties or {}
            )

            # Resolve domain for this listing based on purchased database names
            domain_urn = self._resolve_domain_for_listing(listing, purchased_db_names)

            # Resolve owners for this listing
            owner_urns = self._resolve_owners_for_listing(listing)
            # Extend with owners derived from DESCRIBE details if any
            for hint in details.get("owners", []):
                urn = self._normalize_owner_urn(str(hint))
                if urn and urn not in owner_urns:
                    owner_urns.append(urn)
            # Choose external URL if available
            external_url = details.get("external_url")

            # Use the gen_data_product function from mcp_builder with assets and owners
            # Always use TECHNICAL_OWNER for marketplace data products
            yield from gen_data_product(
                data_product_key=data_product_key,
                name=listing.title,
                description=listing.description
                or f"Internal marketplace listing from {listing.provider}",
                external_url=str(external_url) if external_url else None,
                custom_properties=custom_properties,
                structured_properties=structured_properties,  # type: ignore
                domain_urn=domain_urn,
                owner_urns=owner_urns if owner_urns else None,
                owner_type=OwnershipTypeClass.TECHNICAL_OWNER,  # This is a string constant
                assets=asset_urns if asset_urns else None,
            )

            self.report.report_marketplace_data_product_created()

    def _find_listing_for_purchase(
        self, purchase: SnowflakeMarketplacePurchase
    ) -> Optional[str]:
        """
        Find the listing_global_name for a purchased database.

        Since Snowflake doesn't provide a direct system link between imported databases
        and their source listings, we REQUIRE the 'shares' configuration where users
        explicitly map imported databases to their source shares/listings.

        The shares config maps the imported database name to a source database/share.
        We then find a marketplace listing that matches that source.

        Returns None if no shares config is provided or no match is found.
        """
        if not self.config.shares:
            logger.warning(
                f"No 'shares' configuration provided. Cannot link imported database {purchase.database_name} "
                f"to marketplace listing. Please add shares configuration to your recipe. "
                f"See SNOWFLAKE_MARKETPLACE_SHARES_GUIDE.md for instructions."
            )
            return None

        # Check if this database is an inbound share (imported database)
        inbounds = self.config.inbounds()
        if purchase.database_name not in inbounds:
            logger.debug(
                f"Imported database {purchase.database_name} not found in shares configuration inbounds. "
                f"Available inbounds: {list(inbounds.keys())}"
            )
            return None

        source_db = inbounds[purchase.database_name]
        source_db_name = source_db.database
        source_db_lower = source_db_name.lower().replace("_", "").replace("-", "")

        # Try to find a listing that matches the source database
        for listing_global_name, listing in self._marketplace_listings.items():
            listing_lower = (
                listing_global_name.lower().replace("_", "").replace("-", "")
            )

            # Check if the source database name appears in the listing global name
            # e.g., source_db "CUSTOMER_360" matches "ACME_CORP.PRODUCT.CUSTOMER_360"
            # or "ACME_DATA" matches "ACME.DATA.LISTING"
            if (
                source_db_lower in listing_lower
                or source_db_name.lower() in listing_global_name.lower()
            ):
                logger.info(
                    f"Matched imported database {purchase.database_name} to listing {listing_global_name} "
                    f"via shares config (source: {source_db_name})"
                )
                return listing_global_name

            # Check parts of the source database name (split by underscores)
            source_parts = [part.lower() for part in source_db_name.split("_")]

            # If all source parts appear in the listing global name
            if all(
                part in listing_lower or part in listing_global_name.lower()
                for part in source_parts
                if len(part) > 2
            ):
                logger.info(
                    f"Matched imported database {purchase.database_name} to listing {listing_global_name} "
                    f"via shares config (partial match: {source_db_name})"
                )
                return listing_global_name

            # Check if listing name matches exactly
            if listing.name.lower() == source_db_name.lower():
                logger.info(
                    f"Matched imported database {purchase.database_name} to listing {listing_global_name} "
                    f"via shares config (exact name match: {source_db_name})"
                )
                return listing_global_name

        logger.warning(
            f"Could not match source database '{source_db_name}' (from shares config) "
            f"to any marketplace listing for imported database {purchase.database_name}. "
            f"Available listings: {list(self._marketplace_listings.keys())}"
        )
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
            enhanced_properties = DatasetPropertiesClass(
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

            # Note: Data Products do not currently participate in Dataset lineage aspects.

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

            # Create usage statistics for each database accessed via marketplace
            for listing_name, db_events in usage_by_listing_db.items():
                # Check if this listing is in our known listings (after filtering)
                if listing_name not in self._marketplace_listings:
                    logger.debug(f"Skipping usage for filtered listing: {listing_name}")
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
                                DatasetUserUsageCountsClass(
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
                    usage_stats = DatasetUsageStatisticsClass(
                        timestampMillis=start_time_millis,
                        eventGranularity=TimeWindowSizeClass(
                            unit=self.config.bucket_duration, multiple=1
                        ),
                        totalSqlQueries=total_queries,
                        uniqueUserCount=len(unique_users),
                        userCounts=sorted(user_counts, key=lambda x: x.user),
                    )

                    yield MetadataChangeProposalWrapper(
                        entityUrn=database_urn, aspect=usage_stats
                    ).as_workunit()

                    # Count events for this listing/database pair
                    self.report.marketplace_usage_events_processed += len(events)

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
