import json
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set

from pydantic import BaseModel, Field

from datahub.emitter.mce_builder import make_group_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DomainKey, gen_data_product, gen_domain
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
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    OwnershipTypeClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import DataTypeUrn, EntityTypeUrn, StructuredPropertyUrn
from datahub.specific.dataproduct import DataProductPatchBuilder

if TYPE_CHECKING:
    from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


class DataProductAssets(BaseModel):
    """Container for assets and databases associated with a data product"""

    table_urns: List[str] = Field(
        default_factory=list,
        description="List of table URNs associated with the data product",
    )
    database_names: List[str] = Field(
        default_factory=list, description="List of database names (for reference only)"
    )


class DataProductMetadata(BaseModel):
    """Complete metadata needed to create a data product"""

    name: str = Field(description="Display name of the data product")
    description: str = Field(description="Description of the data product")
    external_url: str = Field(description="URL to view in Snowflake marketplace")
    custom_properties: Dict[str, str] = Field(
        description="Custom properties for the data product"
    )
    structured_properties: Optional[Dict[str, str]] = Field(
        default=None, description="Structured properties for searchability"
    )
    domain_urn: Optional[str] = Field(
        default=None, description="Domain URN for organizational grouping"
    )
    owner_urns: List[str] = Field(
        default_factory=list, description="List of owner URNs"
    )
    documentation_links: List[str] = Field(
        default_factory=list, description="List of documentation URLs"
    )


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

        self._marketplace_listings: Dict[str, SnowflakeMarketplaceListing] = {}
        self._marketplace_purchases: Dict[str, SnowflakeMarketplacePurchase] = {}
        self._provider_shares: Dict[str, SnowflakeProviderShare] = {}
        self._data_product_assets: Dict[str, List[str]] = defaultdict(list)

    def get_marketplace_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.marketplace.enabled:
            return

        if self.config.marketplace.marketplace_properties_as_structured_properties:
            yield from self._create_marketplace_structured_property_definitions()

        self._load_marketplace_data()

        yield from self._create_marketplace_domains()
        yield from self._create_marketplace_data_products()
        yield from self._associate_assets_to_data_products()
        yield from self._enhance_purchased_datasets()
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
        org_name = listing.organization_profile_name or "Unknown Organization"

        domain_key = DomainKey(
            name=org_name,
            platform="snowflake",
            instance=self.config.platform_instance,
        )

        return domain_key.as_urn()

    def _load_marketplace_data(self) -> None:
        self._load_marketplace_listings()

        mode = self.config.marketplace.marketplace_mode
        if mode in ["consumer", "both"]:
            self._load_marketplace_purchases()
        if mode in ["provider", "both"]:
            self._load_provider_shares()

    def _load_marketplace_listings(self) -> None:
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_listings())

            for row in cur:
                # SHOW AVAILABLE LISTINGS returns: global_name, title, created_on, uniform_listing_locator, organization_profile_name
                # Note: The column is "global_name" not "listing_global_name"
                # Fields not in SHOW output (name, provider, category, description) can be obtained from DESCRIBE AVAILABLE LISTING
                listing = SnowflakeMarketplaceListing(
                    name=row.get(
                        "uniform_listing_locator", ""
                    ),  # SHOW returns uniform_listing_locator, not "name"
                    listing_global_name=row.get(
                        "global_name", ""
                    ),  # Column is "global_name"
                    title=row.get("title", ""),
                    provider=row.get(
                        "organization_profile_name", ""
                    ),  # SHOW returns organization_profile_name, not "provider"
                    category=None,  # Not available in SHOW AVAILABLE LISTINGS
                    description=None,  # Not available in SHOW AVAILABLE LISTINGS
                    created_on=row.get("created_on"),
                    organization_profile_name=row.get(
                        "organization_profile_name"
                    ),  # For domain grouping
                )

                if self.config.marketplace.internal_marketplace_listing_pattern.allowed(
                    listing.listing_global_name
                ):
                    self._marketplace_listings[listing.listing_global_name] = listing
                    self.report.report_marketplace_listing_scanned()
                else:
                    self.report.report_dropped(listing.listing_global_name)

        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                "Failed to get marketplace listings - insufficient permissions",
                context="Make sure the role has privileges to run SHOW AVAILABLE LISTINGS",
                exc=e,
            )

    def _load_marketplace_purchases(self) -> None:
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

        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                "Failed to get marketplace purchases - insufficient permissions",
                context="Make sure the role has access to SNOWFLAKE.ACCOUNT_USAGE.DATABASES",
                exc=e,
            )

    def _load_provider_shares(self) -> None:
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_shares())

            for row in cur:
                if row.get("kind") != "OUTBOUND":
                    continue

                listing_global_name = row.get("listing_global_name")
                if not listing_global_name:
                    continue

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

                self._provider_shares[listing_global_name] = provider_share
                logger.info(
                    f"Found provider share {provider_share.share_name} for listing {listing_global_name}"
                )

        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                "Failed to get provider shares - insufficient permissions",
                context="Make sure the role has privileges to run SHOW SHARES",
                exc=e,
            )

    def _normalize_owner_urn(self, owner: str) -> str:
        if not owner:
            return owner
        val = owner.strip()
        if val.startswith("urn:li:corpuser:") or val.startswith("urn:li:corpGroup:"):
            return val
        if val.lower().startswith("user:"):
            return make_user_urn(val.split(":", 1)[1])
        if val.lower().startswith("group:"):
            return make_group_urn(val.split(":", 1)[1])
        return make_user_urn(val)

    def _resolve_owners_for_listing(
        self, listing: SnowflakeMarketplaceListing
    ) -> List[str]:
        owners: List[str] = []
        title = listing.title or ""
        global_name = listing.listing_global_name or ""
        provider = listing.provider or ""

        try:
            for (
                pattern,
                owner_vals,
            ) in self.config.marketplace.internal_marketplace_owner_patterns.items():
                if (
                    re.search(pattern, title, flags=re.IGNORECASE)
                    or re.search(pattern, global_name, flags=re.IGNORECASE)
                    or re.search(pattern, provider, flags=re.IGNORECASE)
                ):
                    owners.extend(self._normalize_owner_urn(o) for o in owner_vals)
        except Exception as e:
            logger.debug(f"Owner mapping failed for listing {global_name}: {e}")

        deduped: List[str] = []
        seen: Set[str] = set()
        for o in owners:
            if o and o not in seen:
                deduped.append(o)
                seen.add(o)
        return deduped

    def _extract_field_from_props(
        self, props: Dict[str, str], *keys: str
    ) -> Optional[str]:
        for k in keys:
            v = props.get(k)
            if v:
                return v
        return None

    def _parse_resources_json(self, resources_json: Optional[str]) -> Optional[str]:
        if not resources_json:
            return None
        try:
            resources = json.loads(resources_json)
            if isinstance(resources, dict) and "documentation" in resources:
                return resources["documentation"]
        except (json.JSONDecodeError, TypeError):
            logger.debug(f"Failed to parse resources JSON: {resources_json}")
        return None

    def _extract_owners_from_details(self, props: Dict[str, str]) -> List[str]:
        owners: List[str] = []

        support_email = self._extract_field_from_props(
            props, "listing_detail_support_email", "listing_detail_contact_email"
        )
        request_approver = self._extract_field_from_props(
            props,
            "listing_detail_request_approver",
            "listing_detail_approver",
            "listing_detail_approver_contact",
            "listing_detail_listing_approver",
        )
        support_contact = self._extract_field_from_props(
            props,
            "listing_detail_contact",
            "listing_detail_support_contact",
            "listing_detail_support_name",
        )

        if support_email:
            owners.append(support_email)
        if request_approver:
            owners.append(request_approver)
        if support_contact and "@" in support_contact:
            owners.append(support_contact)

        return list(dict.fromkeys(owners))

    def _enrich_listing_custom_properties(
        self, listing: SnowflakeMarketplaceListing, props: Dict[str, str]
    ) -> Dict[str, Any]:
        """Optionally enrich custom properties via DESCRIBE AVAILABLE LISTING (best-effort).

        Returns a dict with optional keys:
          - owners: List[str] of owner identifiers derived from details
          - external_url: Optional[str] preferred documentation URL
          - description: Optional[str] description from listing details
          - documentation_links: List[str] of documentation URLs for institutional memory
        """
        result: Dict[str, Any] = {
            "owners": [],
            "external_url": None,
            "description": None,
            "documentation_links": [],
        }
        if not self.config.marketplace.fetch_internal_marketplace_listing_details:
            return result

        try:
            query = f"DESCRIBE AVAILABLE LISTING {listing.listing_global_name}"
            cur = self.connection.query(query)

            for row in cur:
                if "property" in row and "value" in row:
                    key = str(row["property"]).strip()
                    val = row.get("value")
                    if key and val is not None:
                        norm_key = "listing_detail_" + key.lower().replace(
                            " ", "_"
                        ).replace("-", "_")
                        props[norm_key] = str(val)
                else:
                    for k, v in row.items():
                        if v is not None:
                            norm_key = "listing_detail_" + str(k).lower().replace(
                                " ", "_"
                            )
                            props[norm_key] = str(v)

            description = self._extract_field_from_props(
                props, "listing_detail_description", "listing_detail_desc"
            )

            resources_json = self._extract_field_from_props(
                props, "listing_detail_resources"
            )
            documentation_url = self._parse_resources_json(resources_json)
            if not documentation_url:
                documentation_url = self._extract_field_from_props(
                    props, "listing_detail_documentation_url", "listing_detail_docs_url"
                )

            quickstart_url = self._extract_field_from_props(
                props,
                "listing_detail_quickstart_url",
                "listing_detail_quick_start_url",
                "listing_detail_quickstart",
            )
            support_url = self._extract_field_from_props(
                props, "listing_detail_support_url", "listing_detail_contact_url"
            )
            support_email = self._extract_field_from_props(
                props, "listing_detail_support_email", "listing_detail_contact_email"
            )
            support_contact = self._extract_field_from_props(
                props,
                "listing_detail_contact",
                "listing_detail_support_contact",
                "listing_detail_support_name",
            )
            request_approver = self._extract_field_from_props(
                props,
                "listing_detail_request_approver",
                "listing_detail_approver",
                "listing_detail_approver_contact",
                "listing_detail_listing_approver",
            )

            if description:
                props["description"] = description
            if documentation_url:
                props["documentation_url"] = documentation_url
            if quickstart_url:
                props["quickstart_url"] = quickstart_url
            if support_url:
                props["support_url"] = support_url
            if support_email:
                props["support_email"] = support_email
            if support_contact:
                props["support_contact"] = support_contact
            if request_approver:
                props["request_approver"] = request_approver

            result["owners"] = self._extract_owners_from_details(props)
            result["description"] = description

            doc_links = []
            for url in [documentation_url, quickstart_url, support_url]:
                if url and url not in doc_links:
                    doc_links.append(url)
            result["documentation_links"] = doc_links

        except Exception as e:
            logger.debug(
                f"DESCRIBE AVAILABLE LISTING enrichment failed for {listing.listing_global_name}: {e}"
            )
        return result

    def _is_table_allowed(
        self, db_name: str, schema_name: str, table_name: str
    ) -> bool:
        if not self.config.database_pattern.allowed(db_name):
            return False
        if not self.config.schema_pattern.allowed(f"{db_name}.{schema_name}"):
            return False
        if not self.config.table_pattern.allowed(
            f"{db_name}.{schema_name}.{table_name}"
        ):
            return False
        return True

    def _create_table_urn(self, db_name: str, schema_name: str, table_name: str) -> str:
        table_identifier = self.identifiers.get_dataset_identifier(
            table_name=table_name,
            schema_name=schema_name,
            db_name=db_name,
        )
        return self.identifiers.gen_dataset_urn(table_identifier)

    def _collect_assets_from_share(
        self, share_name: str, listing_global_name: str
    ) -> List[str]:
        asset_urns: List[str] = []
        try:
            query = f"DESC SHARE {share_name}"
            cur = self.connection.query(query)
            table_count = 0

            for row in cur:
                kind = row.get("kind", "")
                name = row.get("name", "")

                if kind in ("TABLE", "VIEW", "MATERIALIZED_VIEW") and name:
                    parts = name.split(".")
                    if len(parts) == 3:
                        db_name, schema_name, table_name = parts

                        if self._is_table_allowed(db_name, schema_name, table_name):
                            table_urn = self._create_table_urn(
                                db_name, schema_name, table_name
                            )
                            asset_urns.append(table_urn)
                            table_count += 1

            if table_count > 0:
                logger.info(
                    f"Added {table_count} tables from share {share_name} "
                    f"as assets to Data Product for listing {listing_global_name}"
                )
        except Exception as e:
            logger.debug(
                f"Failed to describe share {share_name}: {e}. "
                f"Will only include database-level asset."
            )

        return asset_urns

    def _collect_assets_from_database(
        self, db_name: str, listing_global_name: str
    ) -> List[str]:
        asset_urns: List[str] = []

        if not db_name or not self.config.database_pattern.allowed(db_name):
            return asset_urns

        try:
            cur = self.connection.query(
                SnowflakeQuery.marketplace_imported_database_tables(db_name)
            )
            table_count = 0

            for row in cur:
                schema_name = row["SCHEMA_NAME"]
                table_name = row["TABLE_NAME"]

                if self._is_table_allowed(db_name, schema_name, table_name):
                    table_urn = self._create_table_urn(db_name, schema_name, table_name)
                    asset_urns.append(table_urn)
                    table_count += 1

            if table_count > 0:
                logger.info(
                    f"Added {table_count} tables from imported database {db_name} "
                    f"as assets to Data Product for listing {listing_global_name}"
                )
        except Exception as e:
            logger.debug(
                f"Failed to query tables from imported database {db_name}: {e}"
            )

        return asset_urns

    def _collect_databases_for_listing(
        self, listing: SnowflakeMarketplaceListing
    ) -> List[str]:
        database_names: List[str] = []

        if self.config.marketplace.marketplace_mode in ["consumer", "both"]:
            for purchase in self._marketplace_purchases.values():
                listing_global_name = self._find_listing_for_purchase(purchase)
                if (
                    listing_global_name
                    and listing_global_name == listing.listing_global_name
                ):
                    database_names.append(purchase.database_name)

        if self.config.marketplace.marketplace_mode in ["provider", "both"]:
            provider_share = self._provider_shares.get(listing.listing_global_name)
            if provider_share:
                if provider_share.source_database:
                    database_names.append(provider_share.source_database)
                    logger.info(
                        f"Provider mode: Found source database {provider_share.source_database} "
                        f"for listing {listing.listing_global_name}. Will query tables from share."
                    )
                else:
                    logger.warning(
                        f"Provider mode: Found share {provider_share.share_name} for listing {listing.listing_global_name} "
                        f"but source_database is empty!"
                    )
            else:
                logger.debug(
                    f"Provider mode: No provider share found for listing {listing.listing_global_name}. "
                    f"Available shares: {list(self._provider_shares.keys())}"
                )

        return database_names

    def _collect_all_assets_for_listing(
        self, listing: SnowflakeMarketplaceListing, database_names: List[str]
    ) -> List[str]:
        asset_urns: List[str] = []

        # For provider mode, get tables from the share
        if self.config.marketplace.marketplace_mode in ["provider", "both"]:
            provider_share = self._provider_shares.get(listing.listing_global_name)
            if provider_share and provider_share.share_name:
                share_assets = self._collect_assets_from_share(
                    provider_share.share_name, listing.listing_global_name
                )
                asset_urns.extend(share_assets)

        # For consumer mode, get tables from imported databases
        if self.config.marketplace.marketplace_mode in ["consumer", "both"]:
            for db_name in database_names:
                db_assets = self._collect_assets_from_database(
                    db_name, listing.listing_global_name
                )
                asset_urns.extend(db_assets)

        return asset_urns

    def _build_custom_and_structured_properties(
        self, listing: SnowflakeMarketplaceListing
    ) -> tuple[Dict[str, str], Optional[Dict[str, str]]]:
        custom_properties: Dict[str, str]
        structured_properties: Optional[Dict[str, str]] = None

        if self.config.marketplace.marketplace_properties_as_structured_properties:
            structured_properties = {
                "snowflake.marketplace.provider": listing.provider,
                "snowflake.marketplace.category": listing.category or "",
                "snowflake.marketplace.listing_global_name": listing.listing_global_name,
                "snowflake.marketplace.listing_name": listing.name,
                "snowflake.marketplace.type": "internal",
                "snowflake.marketplace.mode": self.config.marketplace.marketplace_mode,
            }
            if listing.created_on:
                structured_properties["snowflake.marketplace.listing_created_on"] = (
                    listing.created_on.isoformat()
                )

            custom_properties = {
                "marketplace_listing": "true",
                "marketplace_mode": self.config.marketplace.marketplace_mode,
            }
        else:
            custom_properties = {
                "platform": "snowflake",
                "marketplace_listing": "true",
                "marketplace_type": "internal",
                "marketplace_mode": self.config.marketplace.marketplace_mode,
                "provider": listing.provider,
                "category": listing.category or "",
                "listing_global_name": listing.listing_global_name,
                "listing_name": listing.name,
            }
            if listing.created_on:
                custom_properties["listing_created_on"] = listing.created_on.isoformat()

        return custom_properties, structured_properties

    def _build_data_product_metadata(
        self,
        listing: SnowflakeMarketplaceListing,
        database_names: List[str],
    ) -> DataProductMetadata:
        custom_properties, structured_properties = (
            self._build_custom_and_structured_properties(listing)
        )

        details = self._enrich_listing_custom_properties(listing, custom_properties)

        domain_urn = self._resolve_domain_for_listing(listing, database_names)

        owner_urns = self._resolve_owners_for_listing(listing)
        for hint in details.get("owners", []):
            urn = self._normalize_owner_urn(str(hint))
            if urn and urn not in owner_urns:
                owner_urns.append(urn)

        marketplace_url = f"https://app.snowflake.com/marketplace/internal/listing/{listing.listing_global_name}"

        description = details.get("description") or listing.description
        if not description:
            description = f"Internal marketplace listing from {listing.provider}"

        documentation_links = details.get("documentation_links", [])

        return DataProductMetadata(
            name=listing.title,
            description=description,
            external_url=marketplace_url,
            custom_properties=custom_properties,
            structured_properties=structured_properties,
            domain_urn=domain_urn,
            owner_urns=owner_urns,
            documentation_links=documentation_links,
        )

    def _create_marketplace_domains(self) -> Iterable[MetadataWorkUnit]:
        organizations: Dict[str, Set[str]] = defaultdict(set)

        for listing in self._marketplace_listings.values():
            org_name = listing.organization_profile_name or "Unknown Organization"
            organizations[org_name].add(listing.listing_global_name)

        for org_name, listing_names in organizations.items():
            domain_key = DomainKey(
                name=org_name,
                platform="snowflake",
                instance=self.config.platform_instance,
            )

            logger.info(
                f"Creating domain for organization '{org_name}' with {len(listing_names)} listings"
            )

            yield from gen_domain(
                domain_key=domain_key,
                name=org_name,
                description=f"Internal marketplace data products from {org_name}",
            )

            self.report.report_entity_scanned("domain")

    def _create_marketplace_data_products(self) -> Iterable[MetadataWorkUnit]:
        for listing in self._marketplace_listings.values():
            data_product_key = self.identifiers.gen_marketplace_data_product_key(
                listing.listing_global_name
            )

            database_names = self._collect_databases_for_listing(listing)
            asset_urns = self._collect_all_assets_for_listing(listing, database_names)
            metadata = self._build_data_product_metadata(listing, database_names)

            data_product_urn = data_product_key.as_urn()
            if asset_urns:
                self._data_product_assets[data_product_urn] = asset_urns
                logger.info(
                    f"Stored {len(asset_urns)} assets for Data Product {listing.title} "
                    f"(will create bidirectional relationships)"
                )

            # NOTE: The assets parameter in gen_data_product only adds them to DataProductProperties.
            # We also need to create bidirectional DataProductContains relationships separately.
            yield from gen_data_product(
                data_product_key=data_product_key,
                name=metadata.name,
                description=metadata.description,
                external_url=metadata.external_url,
                custom_properties=metadata.custom_properties,
                structured_properties=metadata.structured_properties,  # type: ignore
                domain_urn=metadata.domain_urn,
                owner_urns=metadata.owner_urns if metadata.owner_urns else None,
                owner_type=OwnershipTypeClass.TECHNICAL_OWNER,
                assets=asset_urns if asset_urns else None,
            )

            if metadata.documentation_links:
                memory_elements = [
                    InstitutionalMemoryMetadataClass(
                        url=link,
                        description="Documentation",
                        createStamp=AuditStamp(
                            time=int(listing.created_on.timestamp() * 1000)
                            if listing.created_on
                            else 0,
                            actor="urn:li:corpuser:datahub",
                        ),
                    )
                    for link in metadata.documentation_links
                ]

                yield MetadataChangeProposalWrapper(
                    entityUrn=data_product_urn,
                    aspect=InstitutionalMemoryClass(elements=memory_elements),
                ).as_workunit()

            self.report.report_marketplace_data_product_created()

    def _associate_assets_to_data_products(self) -> Iterable[MetadataWorkUnit]:
        """
        Create bidirectional DataProductContains relationships.
        This makes assets appear on the Data Product page, and vice versa.
        """
        for data_product_urn, asset_urns in self._data_product_assets.items():
            if not asset_urns:
                continue

            logger.info(
                f"Creating bidirectional relationships: Data Product {data_product_urn} -> {len(asset_urns)} assets"
            )

            # Use DataProductPatchBuilder to add each asset
            patch_builder = DataProductPatchBuilder(data_product_urn)
            for asset_urn in asset_urns:
                patch_builder.add_asset(asset_urn)
                logger.debug(
                    f"Added asset {asset_urn} to Data Product {data_product_urn}"
                )

            # Emit the patch MCPs
            # DataProductPatchBuilder.build() returns MetadataChangeProposalClass objects
            # We wrap them in MetadataWorkUnit with auto-generated IDs
            for mcp in patch_builder.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
                )

    def _find_listing_for_purchase(
        self, purchase: SnowflakeMarketplacePurchase
    ) -> Optional[str]:
        """
        Find the listing_global_name for a purchased database.
        Requires 'shares' configuration to be set.

        Strategy:
        1. Use explicit listing_global_name from shares config (most reliable)
        2. Match source database name to listing name (case-insensitive)
        3. Return None if no match found
        """
        if not self.config.shares:
            logger.warning(
                f"No 'shares' configuration provided. Cannot link imported database {purchase.database_name} "
                f"to marketplace listing. Please add shares configuration with explicit listing_global_name mapping."
            )
            return None

        for share_name, share_config in self.config.shares.items():
            for consumer in share_config.consumers:
                if (
                    consumer.database == purchase.database_name
                    and consumer.platform_instance == self.config.platform_instance
                ):
                    if share_config.listing_global_name:
                        listing_global_name = share_config.listing_global_name
                        if listing_global_name in self._marketplace_listings:
                            logger.info(
                                f"Matched imported database {purchase.database_name} to listing {listing_global_name} "
                                f"via explicit listing_global_name in shares config"
                            )
                            return listing_global_name
                        else:
                            logger.warning(
                                f"Explicit listing_global_name '{listing_global_name}' specified for database {purchase.database_name} "
                                f"but listing not found in marketplace. Available: {list(self._marketplace_listings.keys())}"
                            )
                            return None

                    inbounds = self.config.inbounds()
                    if purchase.database_name not in inbounds:
                        logger.warning(
                            f"Database {purchase.database_name} found in share {share_name} but not in inbounds mapping"
                        )
                        return None

                    source_db = inbounds[purchase.database_name]
                    source_db_name = source_db.database

                    for (
                        listing_global_name,
                        listing,
                    ) in self._marketplace_listings.items():
                        if (
                            listing.name
                            and listing.name.lower() == source_db_name.lower()
                        ):
                            logger.info(
                                f"Matched imported database {purchase.database_name} to listing {listing_global_name} "
                                f"via exact name match on source database {source_db_name}"
                            )
                            return listing_global_name

                        if (
                            listing.name
                            and source_db_name.lower() in listing.name.lower()
                        ):
                            logger.info(
                                f"Matched imported database {purchase.database_name} to listing {listing_global_name} "
                                f"via substring match: source database '{source_db_name}' found in listing name '{listing.name}'"
                            )
                            return listing_global_name

                    logger.warning(
                        f"Could not match source database '{source_db_name}' to any marketplace listing "
                        f"for database {purchase.database_name}. Consider adding explicit listing_global_name to shares config. "
                        f"Available listings: {list(self._marketplace_listings.keys())}"
                    )
                    return None

        logger.debug(
            f"Database {purchase.database_name} not found as consumer in any shares config"
        )
        return None

    def _enhance_purchased_datasets(self) -> Iterable[MetadataWorkUnit]:
        for purchase in self._marketplace_purchases.values():
            dataset_identifier = self.identifiers.snowflake_identifier(
                purchase.database_name
            )
            database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

            marketplace_properties = {
                "marketplace_purchase": "true",
                "database_type": "IMPORTED_DATABASE",
                "owner": purchase.owner,
                "purchase_date": purchase.purchase_date.isoformat(),
            }

            if purchase.comment:
                marketplace_properties["comment"] = purchase.comment

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
        Format: [{"objectName": "DB.SCHEMA.TABLE", "objectDomain": "Table", ...}, ...]
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
                    object_name = obj.get("objectName", "")
                    if object_name:
                        parts = object_name.split(".")
                        if parts:
                            databases.add(parts[0].strip('"').strip("'"))

            return list(databases)
        except (json.JSONDecodeError, AttributeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse share objects: {e}")
            return []

    def _create_marketplace_usage_statistics(self) -> Iterable[MetadataWorkUnit]:
        start_time_millis = int(self.config.marketplace.start_time.timestamp() * 1000)
        end_time_millis = int(self.config.marketplace.end_time.timestamp() * 1000)

        try:
            cur = self.connection.query(
                SnowflakeQuery.marketplace_listing_access_history(
                    start_time_millis, end_time_millis
                )
            )

            usage_by_listing_db: Dict[
                str, Dict[str, List[SnowflakeMarketplaceAccessEvent]]
            ] = defaultdict(lambda: defaultdict(list))

            for row in cur:
                databases = self._parse_share_objects(
                    row.get("SHARE_OBJECTS_ACCESSED", "[]")
                )

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

                listing_name = event.listing_global_name
                for db_name in databases:
                    usage_by_listing_db[listing_name][db_name].append(event)

            for listing_name, db_events in usage_by_listing_db.items():
                if listing_name not in self._marketplace_listings:
                    logger.debug(f"Skipping usage for filtered listing: {listing_name}")
                    continue

                for database_name, events in db_events.items():
                    if database_name not in self._marketplace_purchases:
                        logger.debug(
                            f"Skipping usage for unknown database: {database_name}"
                        )
                        continue

                    dataset_identifier = self.identifiers.snowflake_identifier(
                        database_name
                    )
                    database_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

                    unique_users: Set[str] = set(event.user_name for event in events)
                    total_queries = len(set(event.query_id for event in events))

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

                    usage_stats = DatasetUsageStatisticsClass(
                        timestampMillis=start_time_millis,
                        eventGranularity=TimeWindowSizeClass(
                            unit=self.config.marketplace.bucket_duration, multiple=1
                        ),
                        totalSqlQueries=total_queries,
                        uniqueUserCount=len(unique_users),
                        userCounts=sorted(user_counts, key=lambda x: x.user),
                    )

                    yield MetadataChangeProposalWrapper(
                        entityUrn=database_urn, aspect=usage_stats
                    ).as_workunit()

                    self.report.marketplace_usage_events_processed += len(events)

        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                "Failed to get marketplace usage statistics - insufficient permissions",
                context="Make sure the role has access to SNOWFLAKE.DATA_SHARING_USAGE.LISTING_ACCESS_HISTORY",
                exc=e,
            )
