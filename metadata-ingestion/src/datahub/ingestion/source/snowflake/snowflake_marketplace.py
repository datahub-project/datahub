import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, TypedDict

from datahub.emitter.mce_builder import (
    get_sys_time,
    make_group_urn,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    MarketplaceMode,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeMarketplaceListing,
    SnowflakeMarketplacePurchase,
    SnowflakeProviderShare,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeIdentifierBuilder,
    SnowsightUrlBuilder,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantUsageRunSkipHandler,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetUsageStatisticsClass,
    DomainsClass,
    InstitutionalMemoryMetadataClass,
    OwnerClass,
    OwnershipTypeClass,
    StatusClass,
    TagAssociationClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import (
    DataTypeUrn,
    EntityTypeUrn,
    StructuredPropertyUrn,
    TagUrn,
)
from datahub.specific.aspect_helpers.institutional_memory import (
    HasInstitutionalMemoryPatch,
)
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.utilities.registries.domain_registry import DomainRegistry


class _ListingEnrichmentResult(TypedDict):
    owners: List[str]
    external_url: Optional[str]
    description: Optional[str]
    documentation_links: List[str]


class _ContainerPatcher(
    HasTagsPatch, HasInstitutionalMemoryPatch, MetadataPatchProposal
):
    pass


logger = logging.getLogger(__name__)

# Snowflake ``OBJECT_DOMAIN`` values from ``LISTING_ACCESS_HISTORY`` that
# correspond to dataset entities in DataHub.
_USAGE_OBJECT_DOMAINS = frozenset(
    {"table", "view", "materialized view", "materialized_view", "external table"}
)

# Used to decide whether a Snowflake "owner" string from listing details is
# a person (email) or just free-form text. Free-form text is dropped to
# avoid polluting the corpuser directory.
_EMAIL_SHAPE_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


@dataclass
class DataProductMetadata:
    name: str
    description: str
    custom_properties: Dict[str, str]
    external_url: Optional[str] = None
    structured_properties: Optional[Dict[str, str]] = None
    domain_urn: Optional[str] = None
    owner_urns: List[str] = field(default_factory=list)
    documentation_links: List[str] = field(default_factory=list)


class SnowflakeMarketplaceHandler(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        identifiers: SnowflakeIdentifierBuilder,
        snowsight_url_builder: Optional[SnowsightUrlBuilder] = None,
        redundant_run_skip_handler: Optional[RedundantUsageRunSkipHandler] = None,
        domain_registry: Optional[DomainRegistry] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.connection = connection
        self.identifiers = identifiers
        self.snowsight_url_builder = snowsight_url_builder
        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.domain_registry = domain_registry

        self._marketplace_listings: Dict[str, SnowflakeMarketplaceListing] = {}
        self._marketplace_purchases: Dict[str, SnowflakeMarketplacePurchase] = {}
        self._provider_shares: Dict[str, SnowflakeProviderShare] = {}
        self._purchase_to_listing: Optional[Dict[str, str]] = None

    def get_marketplace_workunits(self) -> Iterable[MetadataWorkUnit]:
        if not self.config.marketplace.enabled:
            return

        if self.config.marketplace.marketplace_properties_as_structured_properties:
            yield from self._create_marketplace_structured_property_definitions()

        self._load_marketplace_data()

        yield from self._create_marketplace_data_products()
        yield from self._enhance_purchased_datasets()

        # LISTING_ACCESS_HISTORY is provider-side, so skip the query in pure
        # consumer mode — it would always return zero rows.
        if self.config.marketplace.marketplace_mode in (
            MarketplaceMode.provider,
            MarketplaceMode.both,
        ):
            yield from self._create_marketplace_usage_statistics()

    def _create_marketplace_structured_property_definitions(
        self,
    ) -> Iterable[MetadataWorkUnit]:
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

        type_mapping = {
            "string": "datahub.string",
            "date": "datahub.date",
        }
        for prop_def in marketplace_properties:
            urn = StructuredPropertyUrn(prop_def["id"]).urn()
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
                headers={"If-None-Match": "*"},
            ).as_workunit()

    def _resolve_domain_for_listing(
        self, listing: SnowflakeMarketplaceListing
    ) -> Optional[str]:
        """Look up the configured DataHub domain for a marketplace organization.

        Returns ``None`` when no mapping exists; never auto-creates domains,
        which previously orphaned a per-``(name, platform, instance)`` domain
        against the rest of the pipeline.
        """
        org_name = listing.organization_profile_name
        if not org_name:
            return None

        mapping = self.config.marketplace.organization_to_domain
        identifier = mapping.get(org_name)
        if not identifier:
            return None

        if identifier.startswith("urn:li:domain:"):
            return identifier
        if self.domain_registry is not None:
            resolved = self.domain_registry.get_domain_urn(identifier)
            if resolved.startswith("urn:li:domain:"):
                return resolved
        return f"urn:li:domain:{identifier}"

    def _load_marketplace_data(self) -> None:
        self._load_marketplace_listings()

        mode = self.config.marketplace.marketplace_mode
        if mode in (MarketplaceMode.consumer, MarketplaceMode.both):
            self._load_marketplace_purchases()
        if mode in (MarketplaceMode.provider, MarketplaceMode.both):
            self._load_provider_shares()

    def _load_marketplace_listings(self) -> None:
        try:
            cur = self.connection.query(SnowflakeQuery.marketplace_listings())

            for row in cur:
                # SHOW AVAILABLE LISTINGS only exposes a subset of fields
                # (global_name, title, created_on, uniform_listing_locator,
                # organization_profile_name); the rest come from
                # DESCRIBE AVAILABLE LISTING during enrichment.
                listing = SnowflakeMarketplaceListing(
                    name=row.get("uniform_listing_locator") or row.get("name", ""),
                    listing_global_name=row.get("global_name", ""),
                    title=row.get("title", ""),
                    category=None,
                    description=None,
                    created_on=row.get("created_on"),
                    organization_profile_name=row.get("organization_profile_name", ""),
                )

                if self.config.marketplace.internal_marketplace_listing_pattern.allowed(
                    listing.listing_global_name
                ):
                    self._marketplace_listings[listing.listing_global_name] = listing
                    self.report.report_marketplace_listing_scanned()
                else:
                    self.report.report_marketplace_listing_filtered()

            logger.info(
                f"Loaded {len(self._marketplace_listings)} marketplace listings"
            )
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

            logger.info(
                f"Loaded {len(self._marketplace_purchases)} marketplace purchases"
            )
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
                logger.debug(
                    f"Found provider share {provider_share.share_name} for "
                    f"listing {listing_global_name}"
                )

            logger.info(f"Loaded {len(self._provider_shares)} provider shares")
        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                "Failed to get provider shares - insufficient permissions",
                context="Make sure the role has privileges to run SHOW SHARES",
                exc=e,
            )

    def _build_marketplace_listing_url(self, listing_global_name: str) -> str:
        if self.snowsight_url_builder is None:
            return SnowsightUrlBuilder.marketplace_listing_url(listing_global_name)
        return self.snowsight_url_builder.get_external_url_for_internal_marketplace_listing(
            listing_global_name
        )

    def _normalize_owner_urn(self, owner: str) -> Optional[str]:
        """Map a Snowflake owner string to a DataHub URN, or ``None`` for
        free-form text. Accepts existing URNs, ``user:``/``group:`` prefixes,
        and email-shaped values; returns ``None`` for everything else so
        marketplace strings like ``"Data Governance Team"`` don't get minted
        into the corpuser directory."""
        if not owner:
            return None
        val = owner.strip()
        if not val:
            return None
        if val.startswith("urn:li:corpuser:") or val.startswith("urn:li:corpGroup:"):
            return val
        if val.lower().startswith("user:"):
            ident = val.split(":", 1)[1].strip()
            return make_user_urn(ident) if ident else None
        if val.lower().startswith("group:"):
            ident = val.split(":", 1)[1].strip()
            return make_group_urn(ident) if ident else None
        if _EMAIL_SHAPE_RE.match(val):
            return make_user_urn(val)
        return None

    def _resolve_owners_for_listing(
        self, listing: SnowflakeMarketplaceListing
    ) -> List[str]:
        owners: List[str] = []
        title = listing.title or ""
        global_name = listing.listing_global_name or ""
        provider = listing.organization_profile_name or ""

        for (
            pattern,
            owner_vals,
        ) in self.config.marketplace.internal_marketplace_owner_patterns.items():
            try:
                matches = (
                    re.search(pattern, title, flags=re.IGNORECASE)
                    or re.search(pattern, global_name, flags=re.IGNORECASE)
                    or re.search(pattern, provider, flags=re.IGNORECASE)
                )
            except re.error as e:
                self.structured_reporter.warning(
                    title="Invalid marketplace owner regex",
                    message=f"Pattern {pattern!r} in "
                    "`marketplace.internal_marketplace_owner_patterns` is "
                    "not a valid regex; skipped.",
                    exc=e,
                )
                continue
            if matches:
                for o in owner_vals:
                    urn = self._normalize_owner_urn(o)
                    if urn:
                        owners.append(urn)

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
    ) -> _ListingEnrichmentResult:
        """Best-effort enrichment via DESCRIBE AVAILABLE LISTING. Mutates
        ``props`` in place and returns owner hints, description, and
        documentation links pulled from the listing detail rows."""
        result: _ListingEnrichmentResult = {
            "owners": [],
            "external_url": None,
            "description": None,
            "documentation_links": [],
        }
        if not self.config.marketplace.fetch_internal_marketplace_listing_details:
            return result

        try:
            query = SnowflakeQuery.marketplace_describe_available_listing(
                listing.listing_global_name
            )
        except ValueError as e:
            self.structured_reporter.warning(
                title="Skipping marketplace listing enrichment for malformed name",
                message=str(e),
            )
            return result

        try:
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
            if isinstance(e, SnowflakePermissionError):
                # DESCRIBE AVAILABLE LISTING needs IMPORTED PRIVILEGES ON
                # DATABASE SNOWFLAKE; soft-fail so the rest of the stage runs.
                self.structured_reporter.warning(
                    title="Marketplace listing enrichment skipped (insufficient permissions)",
                    message=f"Could not run DESCRIBE AVAILABLE LISTING for {listing.listing_global_name}",
                    context="Grant IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE to the ingestion role "
                    "to populate marketplace listing details (description, documentation links, contacts).",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    title="Optional listing enrichment failed",
                    message=f"Could not fetch details for listing {listing.listing_global_name}",
                    exc=e,
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
        self,
        share_name: str,
        listing_global_name: str,
        source_database: Optional[str] = None,
        source_database_schemas: Optional[List[str]] = None,
    ) -> List[str]:
        asset_urns: List[str] = []
        try:
            query = SnowflakeQuery.marketplace_describe_share(share_name)
        except ValueError as e:
            self.structured_reporter.warning(
                title="Skipping share with malformed name",
                message=str(e),
                context=f"Listing {listing_global_name!r}.",
            )
            return asset_urns

        try:
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
                logger.debug(
                    f"Added {table_count} tables from share {share_name} "
                    f"as assets to Data Product for listing {listing_global_name}"
                )
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                if source_database:
                    # DESC SHARE requires share ownership (ACCOUNTADMIN or owner
                    # role). Snowflake does not allow transferring share ownership,
                    # so fall back to enumerating the source database directly.
                    schema_hint = (
                        f" (schemas: {source_database_schemas})"
                        if source_database_schemas
                        else " (all schemas — set marketplace.listing_to_schemas_overrides to restrict)"
                    )
                    self.structured_reporter.info(
                        title="DESC SHARE permission denied — falling back to database enumeration",
                        message=(
                            f"Role cannot run DESC SHARE on {share_name}. "
                            f"Falling back to enumerating tables in {source_database}"
                            f"{schema_hint}. "
                            "Grant ACCOUNTADMIN role or use `role: ACCOUNTADMIN` in the recipe "
                            "to use DESC SHARE instead."
                        ),
                    )
                    return self._collect_assets_from_database(
                        source_database,
                        listing_global_name,
                        schemas=source_database_schemas,
                    )
                self.structured_reporter.warning(
                    title="Failed to describe provider share",
                    message=f"Could not query tables from share {share_name}",
                    context="Insufficient permissions to run DESC SHARE; "
                    "data product will be created without table assets.",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    title="Failed to parse share description",
                    message=f"Could not parse share {share_name} metadata",
                    exc=e,
                )

        return asset_urns

    def _collect_assets_from_database(
        self,
        db_name: str,
        listing_global_name: str,
        schemas: Optional[List[str]] = None,
    ) -> List[str]:
        asset_urns: List[str] = []

        if not db_name or not self.config.database_pattern.allowed(db_name):
            return asset_urns

        try:
            query = SnowflakeQuery.marketplace_imported_database_tables(
                db_name, schemas=schemas
            )
        except ValueError as e:
            self.structured_reporter.warning(
                title="Skipping database with malformed name",
                message=str(e),
                context=f"Listing {listing_global_name!r}.",
            )
            return asset_urns

        try:
            cur = self.connection.query(query)
            table_count = 0

            for row in cur:
                schema_name = row["SCHEMA_NAME"]
                table_name = row["TABLE_NAME"]

                if self._is_table_allowed(db_name, schema_name, table_name):
                    table_urn = self._create_table_urn(db_name, schema_name, table_name)
                    asset_urns.append(table_urn)
                    table_count += 1

            if table_count > 0:
                logger.debug(
                    f"Added {table_count} tables from imported database {db_name} "
                    f"as assets to Data Product for listing {listing_global_name}"
                )
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                self.structured_reporter.warning(
                    title="Failed to query imported database tables",
                    message=f"Could not list tables in database {db_name}",
                    context="Insufficient permissions to query "
                    "INFORMATION_SCHEMA; data product will be created without "
                    "table assets.",
                    exc=e,
                )
            else:
                self.structured_reporter.warning(
                    title="Failed to parse database tables",
                    message=f"Could not parse table metadata from database {db_name}",
                    exc=e,
                )

        return asset_urns

    def _collect_databases_for_listing(
        self, listing: SnowflakeMarketplaceListing
    ) -> List[str]:
        database_names: List[str] = []

        if self.config.marketplace.marketplace_mode in (
            MarketplaceMode.consumer,
            MarketplaceMode.both,
        ):
            for purchase in self._marketplace_purchases.values():
                listing_global_name = self._find_listing_for_purchase(purchase)
                if (
                    listing_global_name
                    and listing_global_name == listing.listing_global_name
                ):
                    database_names.append(purchase.database_name)

        if self.config.marketplace.marketplace_mode in (
            MarketplaceMode.provider,
            MarketplaceMode.both,
        ):
            provider_share = self._provider_shares.get(listing.listing_global_name)
            if provider_share:
                if provider_share.source_database:
                    database_names.append(provider_share.source_database)
                    logger.debug(
                        f"Provider mode: Found source database "
                        f"{provider_share.source_database} for listing "
                        f"{listing.listing_global_name}. Will query tables "
                        f"from share."
                    )
                else:
                    self.structured_reporter.warning(
                        title="Provider share has no source database",
                        message=f"Share {provider_share.share_name!r} for "
                        f"listing {listing.listing_global_name!r} returned "
                        "no source database; assets cannot be enumerated.",
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

        if self.config.marketplace.marketplace_mode in (
            MarketplaceMode.provider,
            MarketplaceMode.both,
        ):
            provider_share = self._provider_shares.get(listing.listing_global_name)
            if provider_share and provider_share.share_name:
                schema_overrides = (
                    self.config.marketplace.listing_to_schemas_overrides.get(
                        listing.listing_global_name
                    )
                )
                asset_urns.extend(
                    self._collect_assets_from_share(
                        provider_share.share_name,
                        listing.listing_global_name,
                        source_database=provider_share.source_database or None,
                        source_database_schemas=schema_overrides,
                    )
                )

        if self.config.marketplace.marketplace_mode in (
            MarketplaceMode.consumer,
            MarketplaceMode.both,
        ):
            for db_name in database_names:
                asset_urns.extend(
                    self._collect_assets_from_database(
                        db_name, listing.listing_global_name
                    )
                )

        return asset_urns

    def _build_custom_and_structured_properties(
        self, listing: SnowflakeMarketplaceListing
    ) -> tuple[Dict[str, str], Optional[Dict[str, str]]]:
        custom_properties: Dict[str, str]
        structured_properties: Optional[Dict[str, str]] = None

        if self.config.marketplace.marketplace_properties_as_structured_properties:
            structured_properties = {
                "snowflake.marketplace.provider": listing.organization_profile_name,
                "snowflake.marketplace.category": listing.category or "",
                "snowflake.marketplace.listing_global_name": listing.listing_global_name,
                "snowflake.marketplace.listing_name": listing.name,
                "snowflake.marketplace.type": "internal",
            }
            if listing.created_on:
                structured_properties["snowflake.marketplace.listing_created_on"] = (
                    listing.created_on.isoformat()
                )

            custom_properties = {
                "marketplace_listing": "true",
                "marketplace_mode": str(self.config.marketplace.marketplace_mode),
            }
        else:
            custom_properties = {
                "platform": "snowflake",
                "marketplace_listing": "true",
                "marketplace_type": "internal",
                "marketplace_mode": str(self.config.marketplace.marketplace_mode),
                "provider": listing.organization_profile_name,
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

        domain_urn = self._resolve_domain_for_listing(listing)

        owner_urns = self._resolve_owners_for_listing(listing)
        for hint in details.get("owners", []):
            urn = self._normalize_owner_urn(str(hint))
            if urn and urn not in owner_urns:
                owner_urns.append(urn)

        marketplace_url = self._build_marketplace_listing_url(
            listing.listing_global_name
        )

        description = details.get("description") or listing.description
        if not description:
            description = (
                f"Internal marketplace listing from {listing.organization_profile_name}"
            )

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

    def _create_marketplace_data_products(self) -> Iterable[MetadataWorkUnit]:
        """Emit each marketplace listing as a Data Product via
        ``DataProductPatchBuilder``. ``name``, ``description``, and
        ``externalUrl`` are set as field-level patches (the listing is the
        source of truth); owners, custom/structured properties, assets, and
        documentation links are added additively so UI edits survive re-runs.
        ``Domain`` is emitted as a one-shot CREATE only when the listing
        organization is mapped via ``marketplace.organization_to_domain``.
        """
        for listing in self._marketplace_listings.values():
            data_product_key = self.identifiers.gen_marketplace_data_product_key(
                listing.listing_global_name
            )

            database_names = self._collect_databases_for_listing(listing)
            asset_urns = self._collect_all_assets_for_listing(listing, database_names)
            metadata = self._build_data_product_metadata(listing, database_names)

            data_product_urn = data_product_key.as_urn()

            yield MetadataChangeProposalWrapper(
                entityUrn=data_product_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

            if metadata.domain_urn:
                # CREATE-only: preserves UI reassignment, but later edits to
                # ``organization_to_domain`` are no-ops (see docs caveat).
                yield MetadataChangeProposalWrapper(
                    entityUrn=data_product_urn,
                    aspect=DomainsClass(domains=[metadata.domain_urn]),
                    changeType=ChangeTypeClass.CREATE,
                    headers={"If-None-Match": "*"},
                ).as_workunit()

            dp_patcher = DataProductPatchBuilder(data_product_urn)

            dp_patcher.set_name(metadata.name)
            dp_patcher.set_description(metadata.description)
            if metadata.external_url:
                dp_patcher.set_external_url(metadata.external_url)

            for k, v in metadata.custom_properties.items():
                dp_patcher.add_custom_property(k, v)

            if metadata.structured_properties:
                for k, v in metadata.structured_properties.items():
                    dp_patcher.set_structured_property(
                        StructuredPropertyUrn(k).urn(), v
                    )

            for owner_urn in metadata.owner_urns:
                dp_patcher.add_owner(
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                    )
                )

            for asset_urn in asset_urns:
                dp_patcher.add_asset(asset_urn)

            for link in metadata.documentation_links:
                dp_patcher.add_institutional_memory(
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
                )

            for mcp in dp_patcher.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
                )

            self.report.report_marketplace_data_product_created()

    def _build_purchase_to_listing_map(self) -> Dict[str, str]:
        """Compute ``database_name -> listing_global_name`` once, with
        warnings aggregated across all purchases (so a misconfigured share
        produces one report entry, not N).
        """
        result: Dict[str, str] = {}
        if not self._marketplace_purchases:
            return result

        if not self.config.shares:
            self.structured_reporter.warning(
                title="Missing shares configuration",
                message="Cannot link imported marketplace databases to listings",
                context=(
                    "Consumer mode needs `shares` (or "
                    "`marketplace.listing_to_share_overrides`) to associate "
                    f"purchased databases with listings. "
                    f"{len(self._marketplace_purchases)} purchased databases "
                    "cannot be linked."
                ),
            )
            return result

        share_to_listing = {
            share_name: listing_global_name
            for listing_global_name, share_name in self.config.marketplace.listing_to_share_overrides.items()
        }
        unknown_override_listings: List[str] = []
        unmatched_databases: List[str] = []

        inbounds = self.config.inbounds()

        for purchase in self._marketplace_purchases.values():
            for share_name, share_config in self.config.shares.items():
                if not any(
                    consumer.database == purchase.database_name
                    and consumer.platform_instance == self.config.platform_instance
                    for consumer in share_config.consumers
                ):
                    continue

                override_listing = share_to_listing.get(share_name)
                if override_listing is not None:
                    if override_listing in self._marketplace_listings:
                        result[purchase.database_name] = override_listing
                    else:
                        unknown_override_listings.append(override_listing)
                    break

                if purchase.database_name not in inbounds:
                    unmatched_databases.append(purchase.database_name)
                    break

                source_db_name = inbounds[purchase.database_name].database
                matched: Optional[str] = None
                for (
                    listing_global_name,
                    listing,
                ) in self._marketplace_listings.items():
                    if not listing.name:
                        continue
                    if (
                        listing.name.lower() == source_db_name.lower()
                        or source_db_name.lower() in listing.name.lower()
                    ):
                        matched = listing_global_name
                        break

                if matched is not None:
                    result[purchase.database_name] = matched
                else:
                    unmatched_databases.append(purchase.database_name)
                break

        if unknown_override_listings:
            self.structured_reporter.warning(
                title="Marketplace override references unknown listing",
                message="`marketplace.listing_to_share_overrides` references "
                "listing(s) not present in SHOW AVAILABLE LISTINGS",
                context=(
                    f"Unknown: {sorted(set(unknown_override_listings))}. "
                    f"Known: {sorted(self._marketplace_listings)}."
                ),
            )
        if unmatched_databases:
            self.structured_reporter.warning(
                title="Could not link purchased databases to marketplace listings",
                message="Some purchased databases could not be matched to a "
                "marketplace listing by source-database name",
                context=(
                    f"Databases: {sorted(set(unmatched_databases))}. "
                    "Consider adding entries to "
                    "`marketplace.listing_to_share_overrides`."
                ),
            )
        return result

    def _find_listing_for_purchase(
        self, purchase: SnowflakeMarketplacePurchase
    ) -> Optional[str]:
        if self._purchase_to_listing is None:
            self._purchase_to_listing = self._build_purchase_to_listing_map()
        return self._purchase_to_listing.get(purchase.database_name)

    def _enhance_purchased_datasets(self) -> Iterable[MetadataWorkUnit]:
        """Stamp marketplace tags + a documentation link onto each imported
        database container. Uses patch operations so existing tags and links
        added outside this pipeline are preserved. Richer purchase metadata
        lives on the Data Product."""
        for purchase in self._marketplace_purchases.values():
            container_urn = self.identifiers.gen_database_key(
                purchase.database_name
            ).as_urn()

            listing_global_name = self._find_listing_for_purchase(purchase)
            listing: Optional[SnowflakeMarketplaceListing] = (
                self._marketplace_listings.get(listing_global_name)
                if listing_global_name
                else None
            )

            container_patcher = _ContainerPatcher(container_urn)

            tag_urns = [make_tag_urn("Marketplace:Imported")]
            if listing is not None and listing.organization_profile_name:
                # Additive — stale ``Marketplace:Provider:<old>`` tags after a
                # provider rename require manual cleanup (see docs caveat).
                tag_urns.append(
                    make_tag_urn(
                        f"Marketplace:Provider:{listing.organization_profile_name}"
                    )
                )
            for tag_urn in tag_urns:
                container_patcher.add_tag(TagAssociationClass(tag=tag_urn))
                # Emit the tag key aspect explicitly: auto_materialize_referenced_tags_terms
                # cannot extract URNs from opaque PATCH payloads.
                yield MetadataChangeProposalWrapper(
                    entityUrn=tag_urn,
                    aspect=TagUrn.from_string(tag_urn).to_key_aspect(),
                ).as_workunit()

            if listing is not None:
                listing_url = self._build_marketplace_listing_url(
                    listing.listing_global_name
                )
                container_patcher.add_institutional_memory(
                    InstitutionalMemoryMetadataClass(
                        url=listing_url,
                        description=(
                            f"Marketplace Listing: {listing.title or listing.listing_global_name}"
                        ),
                        createStamp=AuditStamp(
                            time=int(purchase.purchase_date.timestamp() * 1000),
                            actor="urn:li:corpuser:datahub",
                        ),
                    )
                )

            for mcp in container_patcher.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
                )

            self.report.report_marketplace_dataset_enhanced()

    def _should_ingest_marketplace_usage(self) -> bool:
        if (
            self.redundant_run_skip_handler is not None
            and self.redundant_run_skip_handler.should_skip_this_run(
                cur_start_time=self.config.start_time,
                cur_end_time=self.config.end_time,
            )
        ):
            self.structured_reporter.report_warning(
                "marketplace-usage-extraction",
                "Skip this run as there was already a run for the current "
                "ingestion window.",
            )
            return False
        return True

    def _create_marketplace_usage_statistics(self) -> Iterable[MetadataWorkUnit]:
        """Emit ``DatasetUsageStatistics`` for marketplace listing access,
        bucketed in SQL. ``userCounts`` is intentionally omitted —
        ``CONSUMER_ACCOUNT_NAME`` is a Snowflake account, not a person, so
        minting ``urn:li:corpuser:`` URNs from it would pollute the user
        directory."""
        if not self._should_ingest_marketplace_usage():
            return

        # Read the window from the redundant-run handler when available, but
        # never call ``update_state`` — the main usage extractor owns the
        # checkpoint.
        if self.redundant_run_skip_handler is not None:
            start_time, end_time = (
                self.redundant_run_skip_handler.suggest_run_time_window(
                    self.config.start_time, self.config.end_time
                )
            )
        else:
            start_time, end_time = self.config.start_time, self.config.end_time

        listing_names = sorted(self._marketplace_listings.keys())
        if not listing_names:
            return

        # No auto_empty_dataset_usage_statistics wrapper: the regular usage
        # extractor already emits the (urn, timestampMillis, DAY) keys for
        # these shared datasets, so padding here would double-write per run.
        yield from self._marketplace_usage_workunits(
            start_time, end_time, listing_names
        )

    def _marketplace_usage_workunits(
        self,
        start_time: datetime,
        end_time: datetime,
        listing_names: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        try:
            cur = self.connection.query(
                SnowflakeQuery.marketplace_listing_access_history(
                    int(start_time.timestamp() * 1000),
                    int(end_time.timestamp() * 1000),
                    time_bucket_size=self.config.bucket_duration,
                    listing_global_names=listing_names,
                )
            )
        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                "Failed to get marketplace usage statistics - insufficient permissions",
                context="Make sure the role has access to SNOWFLAKE.DATA_SHARING_USAGE.LISTING_ACCESS_HISTORY",
                exc=e,
            )
            return

        for row in cur:
            wu = self._build_marketplace_usage_workunit(row)
            if wu is not None:
                yield wu

    def _build_marketplace_usage_workunit(
        self, row: Dict[str, Any]
    ) -> Optional[MetadataWorkUnit]:
        try:
            object_domain = str(row.get("OBJECT_DOMAIN", "") or "").lower()
            if object_domain not in _USAGE_OBJECT_DOMAINS:
                return None

            object_name = row.get("OBJECT_NAME")
            if not object_name:
                return None

            parts = [p.strip('"') for p in str(object_name).split(".")]
            if len(parts) != 3:
                return None
            db_name, schema_name, table_name = parts
            if not self._is_table_allowed(db_name, schema_name, table_name):
                return None

            bucket_start = row.get("BUCKET_START_TIME")
            if bucket_start is None:
                return None

            total_queries = int(row.get("TOTAL_QUERIES") or 0)
            unique_accounts = int(row.get("UNIQUE_ACCOUNTS") or 0)

            usage_stats = DatasetUsageStatisticsClass(
                timestampMillis=int(bucket_start.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=self.config.bucket_duration, multiple=1
                ),
                totalSqlQueries=total_queries,
                uniqueUserCount=unique_accounts,
            )
            self.report.report_marketplace_usage_events_processed(total_queries)
            return MetadataChangeProposalWrapper(
                entityUrn=self._create_table_urn(db_name, schema_name, table_name),
                aspect=usage_stats,
            ).as_workunit()
        except Exception as e:
            logger.debug(f"Failed to parse marketplace usage row: {e}", exc_info=e)
            self.structured_reporter.warning(
                title="Failed to parse marketplace usage row",
                message=f"Skipped one row of LISTING_ACCESS_HISTORY: "
                f"{type(e).__name__}: {e}",
            )
            return None
