# Snowplow Connector: Implementation Plan & Requirements Gap Analysis

**Date**: 2024-12-05 (Last Updated: 2025-01-27)
**Linear Issue**: [ING-1233](https://linear.app/acryl-data/issue/ING-1233)
**Customer**: Ryan's company (8B row atomic.events table, Snowflake → S3 Iceberg migration)
**Status**: Connector at ~40% completion relative to customer requirements

**Recent Updates (2025-01-27)**:

- ✅ API research completed - ownership via deployments array identified
- ⚠️ Critical issue: `initiatorId` not available in API (only `initiator` name)
- ✅ Users API available for user caching
- ✅ Authentication method corrected (GET not POST)
- ✅ Data Products added to implementation roadmap

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current State Assessment](#current-state-assessment)
3. [Gap Analysis](#gap-analysis)
4. [Implementation Roadmap](#implementation-roadmap)
5. [Technical Specifications](#technical-specifications)
6. [API Research Requirements](#api-research-requirements)
7. [Testing Strategy](#testing-strategy)
8. [Risk Assessment](#risk-assessment)
9. [Customer Deliverables](#customer-deliverables)

---

## Executive Summary

### Current Status: ~40% Complete

The Snowplow connector successfully extracts **basic schema metadata** and **table-level lineage**, providing a solid foundation. However, it **lacks the three critical features** that represent the customer's primary use case:

1. ❌ **Ownership Tracking** (0% complete) - **PRIMARY customer need**
2. ❌ **Column-Level Lineage** (0% complete) - Required for end-to-end governance
3. ❌ **Enrichment Lineage** (0% complete) - Explicitly requested by customer
4. ❌ **Data Products** (0% complete) - Group schemas, explicit ownership

### Completion Matrix

| Requirement Category | Status | Completion | Priority |
|---------------------|--------|------------|----------|
| Schema Ingestion | ✅ Complete | 100% | ✅ Done |
| Basic Metadata | ✅ Complete | 100% | ✅ Done |
| Table-Level Lineage | ✅ Complete | 100% | ✅ Done |
| Warehouse Integration | ✅ Complete | 100% | ✅ Done |
| Usage Statistics | ✅ Complete | 100% | ✅ Done |
| **Ownership Tracking** | ❌ **Missing** | **0%** | 🔴 **Critical** |
| **Column-Level Lineage** | ❌ **Missing** | **0%** | 🔴 **Critical** |
| **Enrichment Lineage** | ❌ **Missing** | **0%** | 🔴 **Critical** |
| **Data Products** | ❌ **Missing** | **0%** | 🟡 **High** |
| Enhanced Tagging | 🟡 Partial | 30% | 🟡 Important |

### Timeline to Full Implementation

- **Minimal Viable (Ownership only):** 3 weeks
- **Core Features (Ownership + Column Lineage):** 7 weeks
- **Full Implementation (All features):** 14 weeks (includes Data Products)

---

## Current State Assessment

### ✅ What Works Well (Foundation - 40%)

The current implementation provides a **professional, well-architected foundation**:

#### 1. Schema Extraction (100% Complete)

**Files**: `snowplow.py` (lines 288-366), `schema_parser.py`, `snowplow_client.py`, `iglu_client.py`

- ✅ BDP Data Structures API integration
- ✅ Iglu Schema Registry support
- ✅ JSON Schema → DataHub schema conversion
- ✅ Event and entity schema types
- ✅ Schema field extraction with types, descriptions, constraints
- ✅ Pattern-based filtering (`schema_pattern`)
- ✅ Hidden schema handling

**Code Quality:**

- Type-safe Pydantic models (`snowplow_models.py`)
- Comprehensive error handling
- Retry logic with exponential backoff
- JWT token auto-refresh

#### 2. Container Hierarchy (100% Complete)

**Files**: `snowplow.py` (lines 226-287)

- ✅ Organization containers (for BDP)
- ✅ Tracking scenario containers
- ✅ Proper container nesting
- ✅ Platform instance support

#### 3. Warehouse Integration (100% Complete)

**Files**: `warehouse_extractor.py`, `snowplow_config.py` (lines 107-155)

- ✅ Multi-warehouse support (Snowflake, BigQuery, Redshift, Databricks, PostgreSQL)
- ✅ SQLAlchemy-based connections
- ✅ atomic.events table queries
- ✅ Usage statistics extraction (event counts, unique users)
- ✅ Configurable lookback windows

#### 4. Table-Level Lineage (100% Complete)

**Files**: `warehouse_extractor.py` (lines 379-402), `snowplow.py` (lines 567-672)

- ✅ Creates lineage: `event_schema` → `atomic.events` table
- ✅ Bidirectional relationships
- ✅ Usage statistics attached to lineage

#### 5. Testing & Documentation (100% Complete)

**Files**: `tests/unit/snowplow/`, `tests/integration/snowplow/`, `docs/sources/snowplow/`

- ✅ 57 passing tests (55 unit + 2 integration)
- ✅ Golden file integration tests (13KB, 15 events)
- ✅ Comprehensive user documentation
- ✅ Multiple recipe examples
- ✅ API endpoint documentation (`_API_ENDPOINTS.md`)

### ❌ What's Missing (Critical Gaps - 60%)

#### The Customer's Actual Needs

From Linear Issue ING-1233:

> **Customer Priority:** "Ownership Framework - This is their PRIMARY use case for DataHub"
>
> **Customer Quote:** "They currently have NO ownership tracking for atomic.events fields"
>
> **Business Problem:** "8B row atomic.events table - different columns have different owners"

The current implementation has **ZERO ownership or column-level lineage code**.

---

## Gap Analysis

### 1. ❌ CRITICAL GAP: Ownership Framework (0% Complete)

#### Customer Requirement (from ING-1233)

```
PRIMARY USE CASE:
- Schema-level ownership: Track who created each Iglu schema
- Field-level ownership: Track who added specific fields (via version history)
- They currently have NO ownership tracking for atomic.events fields
- This is their PRIMARY use case for DataHub
```

#### Current Implementation

**Searched entire codebase:**

```bash
grep -r "ownership\|owner\|createdBy\|modifiedBy" src/datahub/ingestion/source/snowplow/
# Result: 0 matches
```

**Status:** ❌ Completely absent

#### What's Needed

##### A. Schema-Level Ownership

Extract from `deployments` array and resolve via Users API:

```python
# NEEDED IN: snowplow_models.py (already exists, but needs update)
class DataStructureDeployment(BaseModel):
    """Deployment information for a data structure."""

    version: str
    ts: Optional[str] = Field(None, description="Deployment timestamp")
    env: Optional[str] = Field(None, description="Environment (PROD, DEV)")
    initiator: Optional[str] = Field(None, description="Name of person who deployed")
    initiator_id: Optional[str] = Field(None, alias="initiatorId", description="User ID (may be missing)")

# NEEDED IN: snowplow.py
class SnowplowSource:
    def __init__(self, ...):
        self._user_cache: Dict[str, User] = {}  # Cache users for ownership resolution
        self._load_user_cache()
    
    def _load_user_cache(self) -> None:
        """Load all users and cache them for initiatorId lookups."""
        users = self.client.get_users()
        for user in users:
            self._user_cache[user.id] = user
        logger.info(f"Cached {len(self._user_cache)} users for ownership resolution")
    
    def _resolve_user_email(
        self, 
        initiator_id: Optional[str], 
        initiator_name: Optional[str]
    ) -> Optional[str]:
        """
        Resolve initiator to email address.
        
        Priority:
        1. Use initiatorId → lookup in user cache → return email (RELIABLE)
        2. If initiatorId missing: Try to match by name (UNRELIABLE - multiple matches possible)
        3. Return None if ambiguous or not found
        """
        # Best case: Use initiatorId (unique identifier)
        if initiator_id and initiator_id in self._user_cache:
            user = self._user_cache[initiator_id]
            return user.email or user.name
        
        # Fallback: Try to match by name (PROBLEMATIC - names not unique!)
        if initiator_name:
            matching_users = [
                user for user in self._user_cache.values()
                if user.name == initiator_name or user.display_name == initiator_name
            ]
            
            if len(matching_users) == 0:
                logger.warning(f"No user found with name '{initiator_name}'. Cannot resolve ownership.")
                return None
            elif len(matching_users) == 1:
                # Single match - use it (but log warning)
                user = matching_users[0]
                logger.warning(
                    f"Resolved '{initiator_name}' to {user.email} by name match. "
                    f"This is unreliable - initiatorId was missing."
                )
                return user.email if user.email else user.name
            else:
                # Multiple matches - ambiguous!
                logger.error(
                    f"Ambiguous ownership: Found {len(matching_users)} users with name '{initiator_name}'. "
                    f"Cannot reliably determine owner."
                )
                return None
        
        return None
    
    def _extract_ownership_from_deployments(
        self, 
        deployments: List[DataStructureDeployment]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract createdBy and modifiedBy from deployments array.
        
        Returns: (createdBy_email, modifiedBy_email)
        """
        if not deployments:
            return None, None
        
        # Sort by timestamp (oldest first)
        sorted_deployments = sorted(
            deployments, 
            key=lambda d: d.ts or "", 
            reverse=False
        )
        
        # Oldest deployment = creator
        oldest = sorted_deployments[0]
        created_by = self._resolve_user_email(
            oldest.initiator_id, 
            oldest.initiator
        )
        
        # Newest deployment = modifier
        newest = sorted_deployments[-1]
        modified_by = self._resolve_user_email(
            newest.initiator_id,
            newest.initiator
        )
        
        return created_by, modified_by

# NEEDED IN: snowplow.py
from datahub.metadata.schema_classes import (
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass
)

def _emit_ownership(
    self,
    dataset_urn: str,
    schema_metadata: DataStructureMetadata
) -> MetadataWorkUnit:
    """Emit schema-level ownership."""

    owners = []

    # Primary owner (creator)
    if schema_metadata.created_by:
        owners.append(
            OwnerClass(
                owner=make_user_urn(schema_metadata.created_by),
                type=OwnershipTypeClass.DATAOWNER,
                source=OwnershipSourceClass(
                    type=OwnershipSourceTypeClass.SOURCE_CONTROL,
                    url=f"https://console.snowplowanalytics.com/..."
                )
            )
        )

    # Producer (last modifier)
    if schema_metadata.modified_by and schema_metadata.modified_by != schema_metadata.created_by:
        owners.append(
            OwnerClass(
                owner=make_user_urn(schema_metadata.modified_by),
                type=OwnershipTypeClass.PRODUCER
            )
        )

    ownership = OwnershipClass(
        owners=owners,
        lastModified=AuditStampClass(
            time=int(time.time() * 1000),
            actor=make_user_urn("datahub")
        )
    )

    return MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=ownership
    ).as_workunit()
```

##### B. Field-Level Ownership (Version History)

Track who added each field across schema versions:

```python
# NEEDED: New file snowplow_version_tracker.py
class VersionHistoryTracker:
    """Tracks field authorship across schema versions."""

    def analyze_versions(
        self,
        schema_identifier: str,
        versions: List[str]
    ) -> Dict[str, str]:
        """
        Determine which user added each field.

        Returns:
            {
                "amount": "ryan@company.com",       # Added in v1-0-0
                "currency": "ryan@company.com",     # Added in v1-0-0
                "discount_code": "jane@company.com" # Added in v1-1-0
            }
        """
        field_authors = {}
        previous_fields = set()

        for version in sorted(versions):  # Sort by SchemaVer
            schema_data = self._fetch_version(schema_identifier, version)
            current_fields = set(schema_data.get("properties", {}).keys())

            # New fields in this version
            new_fields = current_fields - previous_fields
            author = schema_data.get("meta", {}).get("createdBy", "unknown")

            for field in new_fields:
                field_authors[field] = author

            previous_fields = current_fields

        return field_authors

# NEEDED IN: snowplow.py
def _add_field_authorship_tags(
    self,
    fields: List[SchemaFieldClass],
    field_authors: Dict[str, str]
) -> List[SchemaFieldClass]:
    """Add authorship tags to each field."""

    for field in fields:
        field_name = field.fieldPath.split(".")[-1]  # Get leaf field name
        author = field_authors.get(field_name)

        if author:
            # Add tag: added_by_ryan
            tag_urn = make_tag_urn(f"added_by_{author.split('@')[0]}")

            if not field.globalTags:
                field.globalTags = GlobalTagsClass(tags=[])

            field.globalTags.tags.append(
                TagAssociationClass(tag=tag_urn)
            )

    return fields
```

##### C. Configuration

```python
# NEEDED IN: snowplow_config.py
class OwnershipConfig(ConfigModel):
    """Configuration for ownership tracking."""

    strategy: str = Field(
        default="inherit_from_schema",
        description="Ownership strategy: 'inherit_from_schema' or 'manual'"
    )

    track_version_authorship: bool = Field(
        default=True,
        description="Track field-level authorship via version history"
    )

    default_owner: Optional[str] = Field(
        default=None,
        description="Default owner email when creator unknown"
    )

    owner_email_domain: Optional[str] = Field(
        default=None,
        description="Default email domain for usernames (e.g., '@company.com')"
    )

    extract_team_from_schema: bool = Field(
        default=False,
        description="Try to extract team from schema customData"
    )

# Add to SnowplowSourceConfig
class SnowplowSourceConfig:
    ownership_config: OwnershipConfig = Field(
        default_factory=OwnershipConfig,
        description="Ownership extraction configuration"
    )
```

#### API Research Status ✅ **RESOLVED**

**Findings (2025-01-27):**

1. **✅ Ownership Source Identified**: Ownership is available via `deployments` array in Data Structures API response
   - `deployments[].initiator`: Full name (e.g., "Tamás Németh", "John Doe")
   - `deployments[].initiatorId`: User ID (e.g., "user-uuid-123") - **⚠️ NOT AVAILABLE in current API**
   - `deployments[].ts`: Timestamp (ISO 8601)
   - `deployments[].version`: Schema version

2. **⚠️ CRITICAL ISSUE**: `initiatorId` is **NOT present** in API responses
   - Only `initiator` (name) is available
   - Names are **not unique** within organizations (multiple "John Doe" users possible)
   - Cannot reliably map to email addresses without unique identifier

3. **✅ Users API Available**: `GET /organizations/{orgId}/users`
   - Returns: `id`, `email`, `name`, `displayName`
   - Can be used to cache users and map `initiatorId` → `email` (if `initiatorId` becomes available)

4. **✅ Authentication Method**: Use `GET` (not `POST`) for token endpoint
   - Endpoint: `GET /organizations/{orgId}/credentials/v3/token`
   - Fixed in both test script and connector

**Implementation Strategy**:

**Primary Approach** (if `initiatorId` becomes available):

1. Extract `initiatorId` from `deployments[].initiatorId`
2. Cache all users at ingestion start via Users API
3. Map `initiatorId` → `email` using cached user map
4. Use email addresses for DataHub ownership

**Fallback Approach** (current - `initiatorId` missing):

1. Try to match by `initiator` name against cached users
2. If exactly 1 match → use it (with warning about unreliability)
3. If 0 or 2+ matches → return None (ambiguous, don't guess)
4. Use `default_owner` configuration when ownership cannot be resolved
5. Log warnings/errors for ambiguous cases

**Action Required**:

1. ✅ **Contact Snowplow** to request `initiatorId` in deployments array (critical feature request)
2. ✅ Implement user caching with Users API
3. ✅ Implement name matching with strict validation
4. ✅ Add comprehensive logging for ambiguous cases

**API Endpoints Confirmed**:

```
GET /organizations/{orgId}/data-structures/v1  ✅ (deployments array confirmed)
GET /organizations/{orgId}/users              ✅ (available, returns id/email/name)
GET /organizations/{orgId}/data-structures/v1/{hash}/versions  ⚠️ (may not exist)
```

#### Estimated Effort

- ✅ Research BDP API ownership fields: **COMPLETE** (deployments array identified)
- Implement user caching (Users API): **2 days**
- Implement schema-level ownership extraction: **5 days**
- Implement name matching fallback with strict validation: **3 days**
- Implement field-level authorship tracking: **7 days**
- Add configuration options: **2 days**
- Write tests: **3 days**
- **⚠️ Contact Snowplow** to request `initiatorId` feature: **1 day**

**Total: 3 weeks** (includes fallback implementation for missing `initiatorId`)

---

### 2. ❌ CRITICAL GAP: Column-Level Lineage (0% Complete)

#### Customer Requirement (from ING-1233)

```
REQUIRED:
- Map Iglu schema fields → atomic.events Snowflake columns
- Snowflake naming: contexts_{vendor}_{name}_{version}[0]:field
- Column-level lineage: Iglu field → warehouse column → dbt model
- Must handle 8B row wide table with ~130 base columns + custom columns
```

#### Current Implementation

**Existing (Table-Level Only):**

```python
# warehouse_extractor.py:379-402
upstream_lineage = UpstreamLineageClass(
    upstreams=[
        UpstreamClass(
            dataset=schema_urn,  # Links to entire schema
            type=DatasetLineageTypeClass.TRANSFORMED
        )
    ]
)
# ❌ NO FineGrainedLineage
# ❌ NO column mapping
```

**README Acknowledgment (line 231):**
> "🚧 **Column-level Lineage**: Currently creates table-level lineage. Column-level lineage (schema fields → atomic.events columns) coming soon."

#### What's Needed

##### A. Column Name Mapping Logic

```python
# NEEDED: New file column_mapper.py
class SnowflowColumnMapper:
    """Maps Iglu schema fields to Snowflake atomic.events columns."""

    @staticmethod
    def map_field_to_column(
        vendor: str,
        schema_name: str,
        version: str,
        field_path: str
    ) -> str:
        """
        Convert Iglu field to Snowflake column name.

        Examples:
            vendor="com.acme"
            schema_name="checkout_started"
            version="1-0-0"
            field_path="amount"

            Returns: "contexts_com_acme_checkout_started_1[0]:amount"

        Args:
            vendor: Schema vendor (e.g., "com.acme")
            schema_name: Schema name (e.g., "checkout_started")
            version: SchemaVer (e.g., "1-0-0")
            field_path: Field path in schema (e.g., "amount" or "user.id")

        Returns:
            Snowflake column name
        """
        # Convert vendor: com.acme → com_acme
        vendor_normalized = vendor.replace(".", "_")

        # Extract MODEL from version (1-0-0 → 1)
        version_major = version.split("-")[0]

        # Build column name
        column_name = f"contexts_{vendor_normalized}_{schema_name}_{version_major}"

        # Add array indexing and field path
        if "." in field_path:
            # Nested field: user.id → [0]:user:id
            nested_path = field_path.replace(".", ":")
            return f"{column_name}[0]:{nested_path}"
        else:
            # Top-level field: amount → [0]:amount
            return f"{column_name}[0]:{field_path}"

    def get_warehouse_columns(
        self,
        warehouse_connection: WarehouseConnectionConfig
    ) -> List[str]:
        """
        Query Snowflake to get actual atomic.events columns.

        Handles VARIANT columns and extracts nested paths.
        """
        query = f"""
        DESCRIBE TABLE {warehouse_connection.database}.{warehouse_connection.schema_name}.events
        """

        # Execute and parse columns
        # Filter for contexts_* columns
        # Return list of column names
        pass
```

##### B. Fine-Grained Lineage Emission

```python
# NEEDED IN: warehouse_extractor.py
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass
)

def create_column_lineage(
    self,
    schema_urn: str,
    schema_fields: List[str],
    warehouse_urn: str,
    vendor: str,
    schema_name: str,
    version: str
) -> List[FineGrainedLineageClass]:
    """
    Create field-level lineage from schema to warehouse.

    Returns list of FineGrainedLineage for each field mapping.
    """
    mapper = SnowflowColumnMapper()
    lineages = []

    for field_name in schema_fields:
        # Map field to warehouse column
        warehouse_column = mapper.map_field_to_column(
            vendor=vendor,
            schema_name=schema_name,
            version=version,
            field_path=field_name
        )

        # Create lineage: schema.field → warehouse.column
        lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[
                make_schema_field_urn(
                    dataset_urn=schema_urn,
                    field_path=field_name
                )
            ],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[
                make_schema_field_urn(
                    dataset_urn=warehouse_urn,
                    field_path=warehouse_column
                )
            ],
            transformOperation="IDENTITY",  # Direct mapping, no transformation
            confidenceScore=1.0
        )

        lineages.append(lineage)

    return lineages

def _emit_column_lineage(
    self,
    warehouse_urn: str,
    fine_grained_lineages: List[FineGrainedLineageClass]
) -> MetadataWorkUnit:
    """Emit column-level lineage to DataHub."""

    upstream_lineage = UpstreamLineageClass(
        upstreams=[],  # Schema-level already emitted
        fineGrainedLineages=fine_grained_lineages
    )

    return MetadataChangeProposalWrapper(
        entityUrn=warehouse_urn,
        aspect=upstream_lineage
    ).as_workunit()
```

##### C. Snowflake VARIANT Column Parsing

```python
# NEEDED IN: warehouse_extractor.py
def parse_variant_columns(
    self,
    database: str,
    schema: str,
    table: str = "events"
) -> Dict[str, List[str]]:
    """
    Parse Snowflake VARIANT columns to extract nested field paths.

    Returns:
        {
            "contexts_com_acme_checkout_started_1": [
                "amount",
                "currency",
                "discount_code"
            ],
            ...
        }
    """
    # Query to get column definition
    query = f"""
    SELECT
        column_name,
        data_type
    FROM {database}.information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
      AND data_type = 'VARIANT'
      AND column_name LIKE 'contexts_%'
    """

    variant_columns = {}

    with self.engine.connect() as conn:
        result = conn.execute(text(query))

        for row in result:
            column_name = row[0]

            # Query to sample VARIANT structure
            sample_query = f"""
            SELECT DISTINCT
                f.key as field_name
            FROM {database}.{schema}.{table},
                 LATERAL FLATTEN(input => {column_name}[0]) f
            LIMIT 100
            """

            field_sample = conn.execute(text(sample_query))
            fields = [r[0] for r in field_sample]

            variant_columns[column_name] = fields

    return variant_columns
```

##### D. Configuration

```python
# NEEDED IN: snowplow_config.py
class ColumnLineageConfig(ConfigModel):
    """Configuration for column-level lineage."""

    enable_column_lineage: bool = Field(
        default=True,
        description="Create field-level lineage from schemas to warehouse"
    )

    column_pattern: str = Field(
        default="contexts_{vendor}_{name}_{version}",
        description="Column naming pattern in warehouse"
    )

    parse_variant_columns: bool = Field(
        default=True,
        description="Parse Snowflake VARIANT columns for nested fields"
    )

    variant_sample_size: int = Field(
        default=100,
        description="Number of rows to sample for VARIANT structure"
    )

# Add to SnowplowSourceConfig
class SnowplowSourceConfig:
    column_lineage_config: ColumnLineageConfig = Field(
        default_factory=ColumnLineageConfig,
        description="Column-level lineage configuration"
    )
```

#### Validation Needed from Customer

**Must obtain from Ryan:**

1. **Actual atomic.events DDL:**

   ```sql
   SHOW COLUMNS FROM atomic.events;
   -- or
   DESCRIBE TABLE atomic.events;
   ```

2. **Sample column names** for their schemas:

   ```
   contexts_com_acme_checkout_started_1
   contexts_com_acme_user_profile_1
   etc.
   ```

3. **VARIANT structure example:**

   ```json
   contexts_com_acme_checkout_started_1[0]: {
     "amount": 99.99,
     "currency": "USD",
     "discount_code": "SAVE20"
   }
   ```

4. **Confirmation of naming pattern:**
   - Does version in column name match MODEL only (1) or full version (1-0-0)?
   - Are there variations across different schemas?

#### Estimated Effort

- Design column mapping algorithm: **3 days**
- Implement mapper with tests: **5 days**
- Integrate VARIANT column parsing: **5 days**
- Emit FineGrainedLineage: **3 days**
- Test with customer's schema: **3 days**
- Handle edge cases (nested fields, arrays): **3 days**
- Write comprehensive tests: **3 days**

**Total: 4 weeks**

---

### 3. ❌ CRITICAL GAP: Enrichment Lineage (0% Complete)

#### Customer Requirement (from ING-1233)

```
EXPLICITLY REQUESTED:
- Ryan specifically cares about Enrichments (IP Lookup, UA Parser, etc.)
- Need to show which enrichments add which fields to atomic.events
- Create DataJob entities for enrichments
- Emit lineage: Enrichment → atomic.events columns
- Example: IP Lookup enrichment → geo_country, geo_city, geo_latitude, geo_longitude
```

#### Current Implementation

**Searched entire codebase:**

```bash
grep -r "enrichment" src/datahub/ingestion/source/snowplow/
# Result: 0 matches (except in README as "future enhancement")
```

**Status:** ❌ Completely absent

#### What's Needed

##### A. Enrichment Discovery

```python
# NEEDED: New file enrichment_extractor.py
class EnrichmentExtractor:
    """Extracts Snowplow enrichment configurations."""

    # Mapping of known enrichments to fields they add
    ENRICHMENT_FIELD_MAPPING = {
        "IP Lookup Enrichment": [
            "geo_country",
            "geo_city",
            "geo_region",
            "geo_latitude",
            "geo_longitude",
            "geo_region_name",
            "geo_zipcode",
            "geo_timezone"
        ],
        "UA Parser Enrichment": [
            "br_name",
            "br_version",
            "br_family",
            "os_name",
            "os_version",
            "os_family",
            "device_family"
        ],
        "Campaign Attribution Enrichment": [
            "mkt_medium",
            "mkt_source",
            "mkt_term",
            "mkt_content",
            "mkt_campaign"
        ],
        "Currency Conversion Enrichment": [
            "base_currency",
            "conversion_rate"
        ],
        "Referer Parser Enrichment": [
            "refr_medium",
            "refr_source",
            "refr_term"
        ]
    }

    def extract_enrichments(
        self,
        bdp_client: Optional[SnowplowBDPClient] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> List[EnrichmentConfig]:
        """
        Extract enrichment configurations.

        Tries multiple approaches:
        1. BDP Enrichments API (if available)
        2. Manual configuration
        3. Default known enrichments
        """
        if bdp_client:
            try:
                # Try BDP API (research needed)
                return self._fetch_from_api(bdp_client)
            except Exception as e:
                logger.warning(f"BDP Enrichments API unavailable: {e}")

        if config and "enrichments" in config:
            # Manual configuration
            return self._parse_from_config(config["enrichments"])

        # Default: Return standard enrichments
        logger.info("Using default enrichment configuration")
        return self._get_default_enrichments()

    def _get_default_enrichments(self) -> List[EnrichmentConfig]:
        """Return standard Snowplow enrichments."""
        return [
            EnrichmentConfig(
                name="IP Lookup Enrichment",
                type="ip_lookup",
                description="Adds geolocation fields based on IP address",
                fields_added=self.ENRICHMENT_FIELD_MAPPING["IP Lookup Enrichment"],
                enabled=True
            ),
            EnrichmentConfig(
                name="UA Parser Enrichment",
                type="ua_parser",
                description="Parses user agent string for browser/OS information",
                fields_added=self.ENRICHMENT_FIELD_MAPPING["UA Parser Enrichment"],
                enabled=True
            ),
            # ... more enrichments
        ]

@dataclass
class EnrichmentConfig:
    """Configuration for a Snowplow enrichment."""

    name: str
    type: str
    description: str
    fields_added: List[str]
    enabled: bool = True
    owner: Optional[str] = None
    version: Optional[str] = None
    configuration: Optional[Dict[str, Any]] = None
```

##### B. DataJob Entity Creation

```python
# NEEDED IN: snowplow.py
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataFlowInfoClass
)

def _create_enrichment_datajob(
    self,
    enrichment: EnrichmentConfig,
    warehouse_urn: str
) -> Iterable[MetadataWorkUnit]:
    """
    Create DataJob entity for enrichment.

    Returns workunits for:
    - DataJob info
    - DataJob input/output (lineage)
    """
    # Create DataJob URN
    datajob_urn = make_data_job_urn(
        orchestrator="snowplow_enrichment",
        flow_id="snowplow_pipeline",
        job_id=enrichment.name.lower().replace(" ", "_"),
        cluster=self.config.platform_instance or "prod"
    )

    # DataJob Info
    job_info = DataJobInfoClass(
        name=enrichment.name,
        type="ENRICHMENT",
        description=enrichment.description,
        customProperties={
            "enrichment_type": enrichment.type,
            "version": enrichment.version or "unknown",
            "fields_added": ",".join(enrichment.fields_added)
        }
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=datajob_urn,
        aspect=job_info
    ).as_workunit()

    # DataJob Input/Output (Lineage)
    # Input: Raw event data (implicit)
    # Output: Warehouse with enriched fields
    input_output = DataJobInputOutputClass(
        inputDatasets=[],  # No explicit input dataset
        outputDatasets=[warehouse_urn],
        inputDatajobEdges=[],
        outputDatajobEdges=[],
        fineGrainedLineages=[
            # Create lineage for each field
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.NONE,
                upstreams=[],  # Enrichment creates fields
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[
                    make_schema_field_urn(
                        dataset_urn=warehouse_urn,
                        field_path=field
                    )
                    for field in enrichment.fields_added
                ],
                transformOperation="ENRICHMENT",
                confidenceScore=1.0
            )
        ]
    )

    yield MetadataChangeProposalWrapper(
        entityUrn=datajob_urn,
        aspect=input_output
    ).as_workunit()

    # Ownership (if specified)
    if enrichment.owner:
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=make_user_urn(enrichment.owner),
                    type=OwnershipTypeClass.DATAOWNER
                )
            ]
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=datajob_urn,
            aspect=ownership
        ).as_workunit()
```

##### C. Configuration

```python
# NEEDED IN: snowplow_config.py
class EnrichmentConfig(ConfigModel):
    """Configuration for enrichment tracking."""

    extract_enrichments: bool = Field(
        default=True,
        description="Create DataJob entities for Snowplow enrichments"
    )

    enrichment_owner: Optional[str] = Field(
        default=None,
        description="Default owner for enrichment DataJobs"
    )

    enrichments: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="""
        Manual enrichment configuration (if BDP API unavailable).
        Example:
        [
            {
                "name": "IP Lookup Enrichment",
                "type": "ip_lookup",
                "fields_added": ["geo_country", "geo_city"],
                "owner": "data-platform@company.com"
            }
        ]
        """
    )

    include_standard_enrichments: bool = Field(
        default=True,
        description="Include standard Snowplow enrichments (IP, UA, etc.)"
    )

# Add to SnowplowSourceConfig
class SnowplowSourceConfig:
    enrichment_config: EnrichmentConfig = Field(
        default_factory=EnrichmentConfig,
        description="Enrichment extraction configuration"
    )
```

#### API Research Required

**CRITICAL UNKNOWN:** Does BDP have an Enrichments API?

**Action Required:**

1. Check BDP API documentation for enrichments endpoint
2. Possible endpoints to test:

   ```
   GET /organizations/{orgId}/enrichments
   GET /organizations/{orgId}/enrichments/v1
   GET /organizations/{orgId}/pipeline/enrichments
   ```

3. If NO API exists:
   - Document manual configuration approach
   - Provide default enrichment mappings
   - Ask customer which enrichments they use

**Fallback Strategy:**
If no API exists, implement configuration-based approach:

```yaml
enrichment_config:
  enrichments:
    - name: "IP Lookup"
      fields_added: ["geo_country", "geo_city"]
      owner: "data-platform@acme.com"
```

#### Estimated Effort

- Research BDP Enrichments API: **5 days** ⚠️ (Critical path)
- Design enrichment extraction (with/without API): **3 days**
- Implement DataJob creation: **3 days**
- Implement field lineage: **3 days**
- Create default enrichment mappings: **2 days**
- Write tests: **3 days**

**Total: 3 weeks** (assuming API research successful within 5 days)

---

### 4. ❌ MISSING: Data Products (0% Complete)

#### Customer Requirement

Data Products in Snowplow BDP group related schemas and have explicit ownership fields. They should be captured to:

- Show organizational structure (how schemas are grouped)
- Extract explicit ownership at data product level
- Enable governance via domain/team ownership

#### Current Implementation

**Searched entire codebase:**

```bash
grep -r "data.product\|DataProduct" src/datahub/ingestion/source/snowplow/
# Result: Partial - DataProduct model exists but no processing logic
```

**Status:** 🟡 Model exists, but no extraction/processing

#### What's Needed

##### A. Data Product Extraction

```python
# NEEDED IN: snowplow_client.py
def get_data_products(self) -> List[DataProduct]:
    """
    Get all data products from organization.
    
    API: GET /organizations/{orgId}/data-products/v1
    """
    endpoint = f"organizations/{self.organization_id}/data-products/v1"
    response_data = self._request("GET", endpoint)
    # Parse and return data products
```

##### B. Container Creation

```python
# NEEDED IN: snowplow.py
class SnowplowDataProductKey(SnowplowOrganizationKey):
    """Container key for data products."""
    data_product_id: str

def _process_data_product(self, data_product: DataProduct) -> Iterable[MetadataWorkUnit]:
    """Create Container entity for data product."""
    # Use gen_containers() to create container
    # Extract ownership from data_product.owner
    # Extract custom properties (status, domain, type)
```

##### C. Schema Linking

Link schemas to their data products (via tracking scenarios or explicit mapping).

##### D. Ownership Priority

Handle ownership priority: Data Product owner vs Schema creator (configurable).

#### API Endpoint

**Endpoint**: `GET /organizations/{organizationId}/data-products/v1`

**Response Structure** (confirmed):

```json
{
  "data": [{
    "id": "04822844-7bdf-49c8-bdb5-6aeb4ce45e0d",
    "name": "E-commerce Analytics",
    "owner": "tamas.nemeth@acryl.io",  // ✅ Explicit owner field
    "domain": "analytics-team@company.com",
    "description": "Product analytics for e-commerce events",
    "status": "draft",
    "trackingScenarios": [...],
    "sourceApplications": [...]
  }]
}
```

**Reference**: [Managing Data Products via the API](https://docs.snowplow.io/docs/data-product-studio/data-products/api/)

#### Estimated Effort

- Update `DataProduct` model: **1 day**
- Add `get_data_products()` method: **1 day**
- Implement container creation: **3 days**
- Implement ownership extraction: **2 days**
- Implement schema linking: **3 days**
- Write tests: **2 days**

**Total: 2 weeks**

---

### 5. 🟡 PARTIAL GAP: Enhanced Tagging (30% Complete)

#### Customer Requirement (from ING-1233)

```
AUTO-TAG fields with:
- Schema version (snowplow_schema_v1-0-0)
- Event type (snowplow_event_checkout)
- Authorship (added_by_ryan)
- Data classification (PII, Sensitive - from Snowplow's PII flags)
```

#### Current Implementation

**What Exists:**

```python
# snowplow.py: Basic tags only
custom_properties = {
    "platform": "snowplow",
    "env": self.config.env,
}
```

**Status:** 🟡 Platform/environment tags only (~30%)

#### What's Needed

```python
# NEEDED IN: snowplow.py
def _create_tags(
    self,
    schema_data: DataStructure,
    field_authors: Optional[Dict[str, str]] = None
) -> GlobalTagsClass:
    """Create comprehensive tags for schema."""

    tags = []

    # 1. Schema version tag
    version = schema_data.data.self_descriptor.version
    tags.append(
        TagAssociationClass(
            tag=make_tag_urn(f"snowplow_schema_v{version.replace('-', '_')}")
        )
    )

    # 2. Event type tag
    schema_type = schema_data.meta.schema_type
    schema_name = schema_data.data.self_descriptor.name
    if schema_type == "event":
        tags.append(
            TagAssociationClass(
                tag=make_tag_urn(f"snowplow_event_{schema_name}")
            )
        )

    # 3. PII classification (from Snowplow schema)
    if self._is_pii_schema(schema_data):
        tags.append(
            TagAssociationClass(
                tag=make_tag_urn("pii_sensitive")
            )
        )

    # 4. Custom metadata tags
    for key, value in schema_data.meta.custom_data.items():
        if key in ["category", "team", "domain"]:
            tags.append(
                TagAssociationClass(
                    tag=make_tag_urn(f"snowplow_{key}_{value}")
                )
            )

    return GlobalTagsClass(tags=tags)

def _is_pii_schema(self, schema_data: DataStructure) -> bool:
    """Check if schema contains PII fields."""
    # Check for PII markers in field descriptions
    # Check for common PII field names (email, phone, ssn, etc.)
    # Check customData for pii=true
    pass
```

#### Estimated Effort

- Implement version/event tags: **1 day**
- Implement PII detection: **2 days**
- Implement authorship tags (part of Gap #1): **1 day**
- Write tests: **1 day**

**Total: 1 week**

---

## Implementation Roadmap

### Phase 1: Ownership Foundation (Weeks 1-3)

**Goal:** Enable PRIMARY customer use case - answer "Who owns this field?"

#### Week 1: Research & Design ✅ **COMPLETE**

- [x] **Day 1-3:** Research BDP API for ownership fields ✅
  - ✅ Tested with actual API responses
  - ✅ Documented deployments array structure
  - ✅ Identified `initiator` field (name only)
  - ⚠️ Identified missing `initiatorId` (critical issue)
  - ✅ Found Users API for user caching
- [x] **Day 4-5:** Design ownership extraction architecture ✅
  - ✅ Schema-level ownership from deployments array
  - ✅ User caching strategy with Users API
  - ✅ Name matching fallback with strict validation
  - ✅ Configuration design with fallback options

#### Week 2: Schema-Level Ownership

- [ ] **Day 1:** Implement user caching
  - Add `get_users()` method to client
  - Cache users at ingestion start
  - Create user lookup map
- [ ] **Day 2-3:** Implement `OwnershipClass` emission
  - Extract from deployments array (oldest = creator, newest = modifier)
  - Resolve `initiatorId` → email via user cache
  - Fallback to name matching with strict validation
  - Add source attribution
- [ ] **Day 4:** Add ownership configuration
  - Default owner
  - `allow_name_matching` flag (default: false, strict)
  - `fallback_to_default` flag
  - Manual override support
- [ ] **Day 5:** Write tests
  - Unit tests for user caching
  - Unit tests for ownership extraction
  - Unit tests for name matching (ambiguous cases)
  - Integration tests with mocked API

#### Week 3: Field-Level Authorship

- [ ] **Day 1-2:** Implement `VersionHistoryTracker`
  - Fetch all schema versions
  - Diff versions to find new fields
  - Map fields to authors
- [ ] **Day 3:** Add `added_by_{user}` tags
  - Tag generation
  - Field-level tag attachment
- [ ] **Day 4-5:** Testing & documentation
  - Unit tests for version tracking
  - Update docs with ownership examples
  - Update recipe files

**Deliverable:** Schema owners and field authors visible in DataHub UI

**Success Metrics:**

- ✅ Schema shows DATAOWNER and PRODUCER
- ✅ Fields tagged with `added_by_{user}`
- ✅ Tests pass
- ✅ Customer can answer "Who owns this field?"

---

### Phase 2: Column-Level Lineage (Weeks 4-7)

**Goal:** Show Iglu fields → atomic.events columns lineage

#### Week 4: Column Mapping Design

- [ ] **Day 1:** Get customer's atomic.events DDL
  - Sample column names
  - VARIANT structure
  - Confirm naming pattern
- [ ] **Day 2-3:** Design `SnowflowColumnMapper`
  - Column name algorithm
  - Handle nested fields
  - Handle array indexing
- [ ] **Day 4-5:** Implement mapper with tests
  - Unit tests for various schemas
  - Edge case handling

#### Week 5: VARIANT Column Parsing

- [ ] **Day 1-2:** Implement Snowflake VARIANT parsing
  - Query INFORMATION_SCHEMA
  - Sample VARIANT structure
  - Extract nested field paths
- [ ] **Day 3-4:** Test with customer's data
  - Verify column names match
  - Handle discrepancies
- [ ] **Day 5:** Performance optimization
  - Caching
  - Sampling strategies

#### Week 6: FineGrainedLineage Emission

- [ ] **Day 1-2:** Implement `create_column_lineage()`
  - Map each field to column
  - Create FineGrainedLineage objects
- [ ] **Day 3:** Emit lineage workunits
  - Attach to warehouse dataset
  - Verify in DataHub UI
- [ ] **Day 4-5:** Handle edge cases
  - Unmapped fields
  - Version mismatches
  - Nested structures

#### Week 7: Testing & Polish

- [ ] **Day 1-2:** Write comprehensive tests
  - Column mapping tests
  - VARIANT parsing tests
  - Lineage emission tests
- [ ] **Day 3:** Integration testing
  - Test with customer's schemas
  - Verify lineage graph
- [ ] **Day 4-5:** Documentation
  - Update README with column lineage examples
  - Add recipe examples
  - Create troubleshooting guide

**Deliverable:** Field-level lineage graph in DataHub UI

**Success Metrics:**

- ✅ Each Iglu field maps to warehouse column
- ✅ FineGrainedLineage visible in UI
- ✅ Lineage graph shows field → column → dbt model
- ✅ Handles customer's 8B row table

---

### Phase 3: Enrichment Lineage (Weeks 8-10)

**Goal:** Show enrichment → warehouse column lineage

#### Week 8: API Research & Design

- [ ] **Day 1-3:** Research BDP Enrichments API
  - Test endpoints
  - Document response structure
  - If unavailable: Design config-based approach
- [ ] **Day 4-5:** Design enrichment extraction
  - API-based vs config-based
  - Default enrichment mappings
  - DataJob entity design

#### Week 9: DataJob Implementation

- [ ] **Day 1-2:** Implement `EnrichmentExtractor`
  - API extraction (if available)
  - Config-based extraction
  - Default enrichments
- [ ] **Day 3-4:** Implement DataJob creation
  - DataJobInfoClass
  - DataJobInputOutputClass
  - Enrichment → field lineage
- [ ] **Day 5:** Add ownership to DataJobs
  - Default enrichment owner
  - Per-enrichment owners

#### Week 10: Testing & Documentation

- [ ] **Day 1-2:** Write tests
  - Enrichment extraction tests
  - DataJob creation tests
  - Lineage tests
- [ ] **Day 3:** Test with customer
  - Verify their enrichments
  - Correct field mappings
- [ ] **Day 4-5:** Documentation
  - Enrichment configuration guide
  - Example configurations
  - Troubleshooting

**Deliverable:** Enrichment DataJobs visible with lineage

**Success Metrics:**

- ✅ Enrichments appear as DataJobs
- ✅ Lineage shows Enrichment → warehouse columns
- ✅ Customer sees which enrichment added which field

---

### Phase 4: Data Products (Week 11-12)

**Goal:** Extract Data Products as Container entities with ownership

#### Week 11: Data Product Extraction

- [ ] **Day 1-2:** Update `DataProduct` model to match API response
- [ ] **Day 2-3:** Add `get_data_products()` method to client
- [ ] **Day 3-4:** Create `SnowplowDataProductKey` container key
- [ ] **Day 4-5:** Implement `_process_data_product()` method
- [ ] **Day 5:** Emit Container entities for data products
- [ ] **Day 5:** Extract ownership from `owner` field

#### Week 12: Schema Linking & Properties

- [ ] **Day 1-2:** Link schemas to data products (via tracking scenarios)
- [ ] **Day 2-3:** Extract custom properties (status, domain, type)
- [ ] **Day 3-4:** Implement ownership priority logic (Data Product vs Schema owner)
- [ ] **Day 4-5:** Integration tests and documentation

**Deliverable:** Data Products visible as Containers with ownership, schemas linked

---

### Phase 5: Enhanced Tagging (Week 13)

**Goal:** Rich metadata for discoverability and governance

#### Week 13: Tagging Implementation

- [ ] **Day 1:** Schema version & event type tags
- [ ] **Day 2:** PII classification tags
- [ ] **Day 3:** Authorship tags (integrate with Phase 1)
- [ ] **Day 4:** Custom metadata tags
- [ ] **Day 5:** Testing & documentation

**Deliverable:** Auto-tagged fields for governance

---

### Phase 6: Testing & Release (Week 14)

**Goal:** Production-ready GA release

#### Week 14: Final Validation

- [ ] **Day 1:** End-to-end test with customer's data
  - Full ingestion run
  - Verify all features
  - Performance testing
- [ ] **Day 2:** Documentation review
  - Complete user guide
  - API reference
  - Troubleshooting guide
- [ ] **Day 3:** Create demo materials
  - Demo video
  - Screenshot walkthrough
  - Sample recipes
- [ ] **Day 4:** Code review & polish
  - Final ruff/mypy check
  - Code cleanup
  - Performance optimization
- [ ] **Day 5:** Release preparation
  - Write release notes
  - Update changelog
  - Prepare blog post

**Deliverable:** GA-ready connector

---

## Technical Specifications

### Architecture Changes

```
Current Architecture (40% complete):
┌─────────────────────────────────────────┐
│ BDP API / Iglu Registry                  │
│ - Extract schemas                        │
│ - Extract event specs/scenarios          │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Snowplow Connector                       │
│ - Parse JSON Schema                      │
│ - Create datasets                        │
│ - Table-level lineage                    │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ DataHub                                  │
│ - Schemas visible                        │
│ - Basic metadata                         │
│ - NO ownership                           │
│ - NO column lineage                      │
└─────────────────────────────────────────┘

Target Architecture (100% complete):
┌─────────────────────────────────────────┐
│ BDP API / Iglu Registry                  │
│ - Extract schemas + OWNERSHIP            │ ← NEW
│ - Extract version history                │ ← NEW
│ - Extract enrichments (if available)     │ ← NEW
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Snowplow Connector                       │
│ - VersionHistoryTracker                  │ ← NEW
│ - EnrichmentExtractor                    │ ← NEW
│ - SnowflakeColumnMapper                  │ ← NEW
│ - Emit OwnershipClass                    │ ← NEW
│ - Emit FineGrainedLineage                │ ← NEW
│ - Emit DataJob (enrichments)            │ ← NEW
│ - Enhanced tagging                       │ ← NEW
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ Snowflake / Warehouse                    │
│ - Query atomic.events columns            │ ← NEW
│ - Parse VARIANT structure                │ ← NEW
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│ DataHub                                  │
│ + Schema owners (DATAOWNER/PRODUCER)     │ ← NEW
│ + Field authorship tags                  │ ← NEW
│ + Column-level lineage                   │ ← NEW
│ + Enrichment DataJobs                    │ ← NEW
│ + PII classification                     │ ← NEW
└─────────────────────────────────────────┘
```

### New Files Required

```
src/datahub/ingestion/source/snowplow/
├── version_tracker.py         # NEW - Track field authorship
├── enrichment_extractor.py    # NEW - Extract enrichments
├── column_mapper.py           # NEW - Map fields to columns
└── ownership_handler.py       # NEW - Handle ownership logic (user caching, name matching)

tests/unit/snowplow/
├── test_version_tracker.py    # NEW
├── test_enrichment_extractor.py # NEW
├── test_column_mapper.py      # NEW
└── test_ownership_handler.py  # NEW
```

### Configuration Schema Changes

```yaml
# CURRENT
source:
  type: snowplow
  config:
    bdp_connection: {...}
    schema_pattern: {...}
    warehouse_connection: {...}

# TARGET (with new features)
source:
  type: snowplow
  config:
    bdp_connection: {...}
    schema_pattern: {...}
    warehouse_connection: {...}

    # NEW: Ownership configuration
    ownership_config:
      strategy: "inherit_from_schema"
      track_version_authorship: true
      default_owner: "data-platform@company.com"
      allow_name_matching: false  # Default: strict (don't guess on ambiguous names)
      fallback_to_default: true   # Use default_owner when cannot resolve

    # NEW: Column lineage configuration
    column_lineage_config:
      enable_column_lineage: true
      column_pattern: "contexts_{vendor}_{name}_{version}"
      parse_variant_columns: true
      variant_sample_size: 100

    # NEW: Enrichment configuration
    enrichment_config:
      extract_enrichments: true
      enrichment_owner: "data-platform@company.com"
      include_standard_enrichments: true
      enrichments:  # Manual override if API unavailable
        - name: "IP Lookup"
          fields_added: ["geo_country", "geo_city"]
```

---

## API Research Requirements

### Critical Unknowns

#### 1. BDP Data Structures API - Ownership Fields

**Status:** ✅ **RESOLVED** (2025-01-27)

**Findings:**

```json
// Actual API response structure:
{
  "data": [{
    "deployments": [
      {
        "version": "1-0-0",
        "ts": "2025-12-12T11:11:34Z",
        "env": "PROD",
        "initiator": "Tamás Németh",      // ✅ EXISTS (name only)
        "initiatorId": null               // ❌ MISSING (not in API)
      }
    ]
  }]
}
```

**Key Findings:**

- ✅ Ownership available via `deployments` array
- ✅ `initiator` field contains full name
- ❌ **CRITICAL**: `initiatorId` is **NOT available** in API responses
- ⚠️ Names are **not unique** - cannot reliably map to emails without ID
- ✅ Users API available: `GET /organizations/{orgId}/users` (returns id/email/name)

**Action Required:**

1. ✅ **Contact Snowplow** to request `initiatorId` in deployments array
2. ✅ Implement user caching with Users API
3. ✅ Implement name matching fallback with strict validation
4. ✅ Document limitation and workaround

#### 2. BDP Version History API

**Status:** ⚠️ **UNKNOWN**

**Need to find:**

```
GET /organizations/{orgId}/data-structures/v1/{hash}/versions
GET /organizations/{orgId}/data-structures/v1/{hash}/history
```

**Expected response:**

```json
{
  "versions": [
    {
      "version": "1-0-0",
      "createdBy": "ryan@company.com",
      "createdAt": "2024-01-01T00:00:00Z",
      "fields": ["amount", "currency"]
    },
    {
      "version": "1-1-0",
      "createdBy": "jane@company.com",
      "createdAt": "2024-06-01T00:00:00Z",
      "fields": ["amount", "currency", "discount_code"]
    }
  ]
}
```

**Action Required:**

1. Check BDP API documentation
2. Test endpoints with customer credentials
3. If unavailable: Design diff-based approach

#### 3. BDP Enrichments API

**Status:** ⚠️ **UNKNOWN - CRITICAL**

**Possible endpoints:**

```
GET /organizations/{orgId}/enrichments
GET /organizations/{orgId}/enrichments/v1
GET /organizations/{orgId}/pipeline/enrichments
GET /organizations/{orgId}/pipeline/configuration
```

**Expected response:**

```json
{
  "enrichments": [
    {
      "name": "IP Lookup Enrichment",
      "type": "ip_lookup",
      "enabled": true,
      "version": "1.0.0",
      "configuration": {...},
      "fields_added": ["geo_country", "geo_city", ...]
    }
  ]
}
```

**Action Required:**

1. **HIGH PRIORITY:** Research BDP API docs
2. Test various endpoint patterns
3. If unavailable:
   - Document manual configuration approach
   - Provide default enrichment mappings
   - Ask customer which enrichments they use

**Fallback Strategy:** Configuration-based enrichment definitions

#### 4. Snowflake Column Structure

**Status:** ⚠️ **NEED CUSTOMER DATA**

**Need from Ryan:**

1. **DDL for atomic.events:**

   ```sql
   SHOW COLUMNS FROM atomic.events;
   -- or
   DESCRIBE TABLE atomic.events;
   ```

2. **Sample column names:**

   ```
   contexts_com_acme_checkout_started_1
   contexts_com_acme_user_profile_2
   contexts_com_snowplowanalytics_snowplow_web_page_1
   ```

3. **VARIANT structure:**

   ```sql
   SELECT
     contexts_com_acme_checkout_started_1[0]
   FROM atomic.events
   LIMIT 1;

   -- Expected result:
   {
     "amount": 99.99,
     "currency": "USD",
     "discount_code": "SAVE20"
   }
   ```

4. **Confirm naming pattern:**
   - Version format in column: `_1` (MODEL only) or `_1_0_0` (full version)?
   - Consistent across all schemas?
   - Any special cases or variations?

---

## Testing Strategy

### Unit Tests (Per Feature)

#### Ownership Tests

```python
# tests/unit/snowplow/test_ownership_handler.py

def test_extract_schema_owner():
    """Test extracting schema-level owner from BDP API."""
    schema_data = {
        "meta": {
            "createdBy": "ryan@company.com",
            "modifiedBy": "jane@company.com"
        }
    }

    ownership = extract_ownership(schema_data)

    assert len(ownership.owners) == 2
    assert ownership.owners[0].type == OwnershipTypeClass.DATAOWNER
    assert "ryan" in str(ownership.owners[0].owner)

def test_version_history_tracking():
    """Test field authorship via version diff."""
    versions = [
        {"version": "1-0-0", "fields": ["amount"], "author": "ryan"},
        {"version": "1-1-0", "fields": ["amount", "currency"], "author": "jane"}
    ]

    authors = track_field_authors(versions)

    assert authors["amount"] == "ryan"
    assert authors["currency"] == "jane"
```

#### Column Lineage Tests

```python
# tests/unit/snowplow/test_column_mapper.py

def test_column_mapping():
    """Test Iglu field → Snowflake column mapping."""
    mapper = SnowflowColumnMapper()

    column = mapper.map_field_to_column(
        vendor="com.acme",
        schema_name="checkout_started",
        version="1-0-0",
        field_path="amount"
    )

    assert column == "contexts_com_acme_checkout_started_1[0]:amount"

def test_nested_field_mapping():
    """Test nested field mapping."""
    column = mapper.map_field_to_column(
        vendor="com.acme",
        schema_name="user_profile",
        version="2-1-0",
        field_path="address.city"
    )

    assert column == "contexts_com_acme_user_profile_2[0]:address:city"
```

#### Enrichment Tests

```python
# tests/unit/snowplow/test_enrichment_extractor.py

def test_default_enrichments():
    """Test default enrichment mappings."""
    extractor = EnrichmentExtractor()
    enrichments = extractor.get_default_enrichments()

    ip_lookup = next(e for e in enrichments if e.type == "ip_lookup")
    assert "geo_country" in ip_lookup.fields_added
    assert "geo_city" in ip_lookup.fields_added

def test_enrichment_datajob_creation():
    """Test DataJob entity creation for enrichment."""
    enrichment = EnrichmentConfig(
        name="IP Lookup",
        type="ip_lookup",
        fields_added=["geo_country", "geo_city"]
    )

    workunits = list(create_enrichment_datajob(enrichment))

    assert len(workunits) >= 2  # Info + InputOutput
    # Verify DataJob URN format
    # Verify lineage to warehouse columns
```

### Integration Tests

#### End-to-End Test with All Features

```python
# tests/integration/snowplow/test_snowplow_full.py

@freeze_time("2024-01-01")
@pytest.mark.integration
def test_snowplow_full_ingestion(pytestconfig, tmp_path):
    """
    Test full ingestion with:
    - Schema extraction
    - Ownership
    - Column lineage
    - Enrichments
    - Tagging
    """
    config = {
        "source": {
            "type": "snowplow",
            "config": {
                "bdp_connection": {...},
                "warehouse_connection": {...},
                "ownership_config": {
                    "track_version_authorship": True
                },
                "column_lineage_config": {
                    "enable_column_lineage": True
                },
                "enrichment_config": {
                    "extract_enrichments": True
                }
            }
        },
        "sink": {"type": "file", "config": {...}}
    }

    # Mock all APIs
    with patch_bdp_api(), patch_warehouse():
        pipeline = Pipeline.create(config)
        pipeline.run()

    # Verify output
    output = load_output_file()

    # Check ownership aspects
    ownership_aspects = [a for a in output if "ownership" in a]
    assert len(ownership_aspects) > 0

    # Check fine-grained lineage
    lineage_aspects = [a for a in output if "fineGrainedLineages" in a]
    assert len(lineage_aspects) > 0

    # Check DataJob entities (enrichments)
    datajob_aspects = [a for a in output if "dataJob" in a["entityUrn"]]
    assert len(datajob_aspects) > 0
```

### Golden File Updates

After implementing each feature, regenerate golden files:

```bash
# Phase 1: Ownership
pytest tests/integration/snowplow/ --update-golden-files
# Verify ownership aspects in golden file

# Phase 2: Column Lineage
pytest tests/integration/snowplow/ --update-golden-files
# Verify fineGrainedLineages in golden file

# Phase 3: Enrichments
pytest tests/integration/snowplow/ --update-golden-files
# Verify DataJob entities in golden file
```

### Customer Validation Testing

#### Pre-Release Checklist

- [ ] **Ingest customer's actual schemas** (with permission)
- [ ] **Verify ownership**:
  - Schema shows correct DATAOWNER
  - Fields tagged with correct authorship
- [ ] **Verify column lineage**:
  - Each field maps to correct warehouse column
  - Lineage graph displays correctly in UI
- [ ] **Verify enrichment lineage**:
  - All enrichments appear as DataJobs
  - Lineage shows enrichment → columns
- [ ] **Performance test**:
  - Full ingestion completes in < 1 hour
  - No memory issues with 8B row queries
- [ ] **UI walkthrough**:
  - Navigate schema → warehouse → dbt
  - Search by owner
  - Search by tag
  - View lineage graph

---

## Risk Assessment

### 🔴 High Risk

#### Risk 1: BDP API Ownership Fields Not Available

**Impact:** Cannot implement schema-level ownership (PRIMARY customer need)

**Probability:** Medium (30%)

**Mitigation:**

1. **Plan A:** Extract from API (if available)
2. **Plan B:** Manual configuration:

   ```yaml
   ownership_config:
     manual_owners:
       "com.acme.checkout_started": "ryan@company.com"
       "com.acme.user_profile": "jane@company.com"
   ```

3. **Plan C:** Infer from schema customData/tags
4. **Plan D:** Default owner for all schemas

**Fallback Quality:** ⚠️ Degraded (manual maintenance required)

#### Risk 2: BDP Enrichments API Doesn't Exist

**Impact:** Cannot automatically discover enrichments

**Probability:** High (60%)

**Mitigation:**

1. **Plan A:** Extract from API (if available)
2. **Plan B:** Configuration-based (default enrichments + manual overrides):

   ```yaml
   enrichment_config:
     enrichments:
       - name: "IP Lookup"
         fields_added: ["geo_country", "geo_city"]
   ```

3. **Plan C:** Provide comprehensive default mappings for standard enrichments

**Fallback Quality:** ✅ Acceptable (configuration is reasonable for enrichments)

#### Risk 3: Version History API Not Available

**Impact:** Cannot track field-level authorship automatically

**Probability:** Medium (40%)

**Mitigation:**

1. **Plan A:** Use version history API
2. **Plan B:** Manually fetch all versions and diff schemas
3. **Plan C:** Fields inherit schema owner (no per-field granularity)

**Fallback Quality:** 🟡 Reduced (schema-level only, but still valuable)

### 🟡 Medium Risk

#### Risk 4: Column Naming Pattern Varies

**Impact:** Incorrect field → column mappings

**Probability:** Medium (30%)

**Mitigation:**

1. Get customer's actual DDL early
2. Make column pattern configurable
3. Support multiple patterns via regex
4. Add validation step to verify mappings

**Fallback Quality:** ✅ Good (configurable patterns handle variations)

#### Risk 5: VARIANT Column Structure Complex

**Impact:** Cannot parse nested fields correctly

**Probability:** Low (20%)

**Mitigation:**

1. Test with customer's actual data early
2. Support configurable depth limits
3. Fallback to top-level fields only if complex

**Fallback Quality:** ✅ Acceptable (top-level fields still valuable)

#### Risk 6: Performance with 8B Rows

**Impact:** Queries too slow or time out

**Probability:** Medium (30%)

**Mitigation:**

1. Use indexed columns (collector_tstamp)
2. Implement sampling for VARIANT parsing
3. Add result caching
4. Make lookback window configurable
5. Add query timeout configuration

**Fallback Quality:** ✅ Good (multiple optimization strategies)

### 🟢 Low Risk

#### Risk 7: Customer Data Format Differs

**Impact:** Need minor adjustments to extraction logic

**Probability:** High (70%) - but low impact

**Mitigation:**

1. Iterative testing with customer
2. Configuration overrides
3. Flexible parsing logic

**Fallback Quality:** ✅ Excellent (design for flexibility)

---

## Customer Deliverables

### Pre-Implementation Phase

#### 1. Metadata Inventory Spreadsheet

**Due:** Before Phase 1 starts

| Metadata Field | Source | DataHub Entity | DataHub Aspect | Example |
|----------------|--------|----------------|----------------|---------|
| Schema Name | BDP Data Structures API | Dataset | DatasetProperties | "checkout_started" |
| Schema Description | BDP API | Dataset | DatasetProperties | "Checkout flow started event" |
| Schema Owner | BDP API `createdBy` | Dataset | Ownership (DATAOWNER) | "<ryan@company.com>" |
| Last Modifier | BDP API `modifiedBy` | Dataset | Ownership (PRODUCER) | "<jane@company.com>" |
| Field Name | JSON Schema `properties` | SchemaField | SchemaMetadata | "amount" |
| Field Type | JSON Schema `type` | SchemaField | SchemaMetadata | "number" |
| Field Author | Version History (diff) | SchemaField | GlobalTags | "added_by_ryan" |
| Column Name | Snowflake DDL | SchemaField (warehouse) | SchemaMetadata | "contexts_com_acme_checkout_started_1[0]:amount" |
| Field Lineage | Mapper | UpstreamLineage | FineGrainedLineages | Schema.amount → Warehouse.contexts_..._amount |
| Enrichment Name | BDP API or Config | DataJob | DataJobInfo | "IP Lookup Enrichment" |
| Enrichment Fields | Mapping | DataJobInputOutput | FineGrainedLineages | ["geo_country", "geo_city"] |
| Event Count | Warehouse Query | Dataset | DatasetUsageStatistics | 1,000,000 events |

#### 2. Sample Ingest (POC)

**Due:** End of Week 3 (after Phase 1)

- Ingest 5-10 of Ryan's actual schemas
- Show ownership in test DataHub instance
- Let them explore:
  - Schema owners
  - Field authorship tags
  - Basic lineage
- Get feedback on accuracy

#### 3. Lineage Diagram

**Due:** Before Phase 2

```
┌──────────────────────────────────────┐
│ Iglu Schema: checkout_started        │
│ Owner: Team Checkout (Ryan)          │
│ Fields:                              │
│   - amount (added by Ryan)           │ ← Phase 1
│   - currency (added by Ryan)         │
│   - discount_code (added by Jane)    │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│ Snowplow Pipeline                    │
│ - Enrichments applied                │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│ Enrichment: IP Lookup                │ ← Phase 3
│ Owner: Data Platform                 │
│ Adds: geo_country, geo_city          │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│ atomic.events (Snowflake)            │
│ Columns:                             │
│   - contexts_..._checkout_...[0]:amount    │ ← Phase 2
│     Owner: Team Checkout (inherited)      │
│   - contexts_..._checkout_...[0]:currency │
│   - geo_country                           │
│     Owner: Data Platform (enrichment)     │
└──────────────────────────────────────┘
              ↓
┌──────────────────────────────────────┐
│ dbt: fct_checkouts                   │ ← Existing dbt integration
│ Owner: Team Data                     │
│ Columns:                             │
│   - checkout_amount                  │
│     (from contexts_..._amount)       │
└──────────────────────────────────────┘
```

#### 4. Ownership Strategy Document

**Due:** Before Phase 1

**Document should explain:**

1. **Schema-Level Ownership:**
   - Extracted from BDP API `createdBy` field
   - Mapped to DataHub DATAOWNER
   - Last modifier → PRODUCER role
   - Default owner if creator unknown

2. **Field-Level Authorship:**
   - Tracked via version history
   - Tag format: `added_by_{username}`
   - Inheritance: Fields inherit schema owner by default
   - Manual override capability

3. **Governance Recommendations:**
   - Use DATAOWNER for accountability
   - Use field tags for detailed attribution
   - Set up DataHub policies per owner
   - Configure alerts for schema changes

4. **Manual Override Example:**

   ```yaml
   ownership_config:
     manual_owners:
       "com.acme.checkout_started": "checkout-team@company.com"
     manual_field_authors:
       "com.acme.checkout_started.discount_code": "marketing-team@company.com"
   ```

#### 5. Enrichment Lineage Design

**Due:** After API research (Week 8)

**Document should cover:**

1. **API vs Configuration Approach:**
   - If API available: Automatic extraction
   - If not: Configuration-based with defaults

2. **Standard Enrichments Included:**
   - IP Lookup (geo fields)
   - UA Parser (browser/OS fields)
   - Campaign Attribution (marketing fields)
   - Referer Parser
   - Currency Conversion

3. **Custom Enrichment Configuration:**

   ```yaml
   enrichment_config:
     enrichments:
       - name: "Custom Enrichment"
         type: "custom"
         fields_added: ["custom_field_1", "custom_field_2"]
         owner: "data-team@company.com"
   ```

4. **DataJob Visualization:**
   - How enrichments appear in DataHub
   - Lineage graph examples
   - Ownership attribution

---

## Open Questions for Customer (Ryan)

### Immediate Questions (Pre-Phase 1)

#### Ownership

1. **How often do you update Snowplow schemas?**
   - Daily? Weekly? Monthly?
   - *Impact:* Determines polling frequency for version history

2. **Who should own enriched fields by default?**
   - Data Platform team?
   - Per-enrichment ownership?
   - *Impact:* Configuration for enrichment ownership

3. **What's your desired ownership granularity?**
   - Schema-level only (faster implementation)
   - Field-level preferred (more granular)
   - *Impact:* Prioritization of version history feature

#### Schemas

4. **Can you share 2-3 sample schemas for testing?**
   - Preferably with multiple versions
   - Include field types and descriptions
   - *Impact:* Early testing and validation

5. **Do you want ALL Snowplow standard events ingested, or only custom schemas?**
   - Standard events: `page_view`, `page_ping`, etc.
   - Custom only: `com.acme.*`
   - *Impact:* Filtering configuration

#### Technical

6. **Can you provide the Snowflake atomic.events DDL?**

   ```sql
   DESCRIBE TABLE atomic.events;
   -- or
   SHOW COLUMNS FROM atomic.events;
   ```

   - *Impact:* CRITICAL for column lineage implementation

7. **Which enrichments are you actively using?**
   - IP Lookup? UA Parser? Others?
   - Custom enrichments?
   - *Impact:* Enrichment configuration and testing

8. **Any specific challenges with the 8B row wide table?**
   - Query performance issues?
   - Specific columns that are problematic?
   - *Impact:* Performance optimization strategy

#### Future Planning

9. **Timeline for S3 Iceberg migration?**
   - When do you plan to migrate?
   - Will you run both Snowflake and Iceberg in parallel?
   - *Impact:* Warehouse adapter design

10. **Can you connect us with team that has Kafka/Kinesis access?**
    - For future real-time ingestion
    - *Impact:* Phase 2 planning (post-GA)

---

## Success Metrics

### Phase 1 Success (Ownership)

- ✅ All Iglu schemas show DATAOWNER in DataHub
- ✅ Schemas show PRODUCER (last modifier)
- ✅ Fields tagged with `added_by_{user}`
- ✅ Ryan can answer: "Who owns this field?"
- ✅ Ownership accuracy: >95% correct

### Phase 2 Success (Column Lineage)

- ✅ Each Iglu field maps to warehouse column
- ✅ FineGrainedLineage visible in UI
- ✅ Lineage graph shows: Schema field → Warehouse column → dbt model
- ✅ Handles 8B row table without performance issues
- ✅ Column mapping accuracy: >95% correct

### Phase 3 Success (Enrichments)

- ✅ All enrichments appear as DataJobs
- ✅ Lineage shows: Enrichment → warehouse columns
- ✅ Customer can answer: "Which enrichment added this field?"
- ✅ Enrichment → field accuracy: 100% (based on config)

### Overall Success (GA Release)

- ✅ End-to-end lineage visible: Iglu → Warehouse → dbt
- ✅ Ownership tracking solves customer's primary use case
- ✅ All tests pass (unit + integration)
- ✅ Documentation complete
- ✅ Ryan's team actively using DataHub for Snowplow governance
- ✅ Performance: Full ingestion < 1 hour

---

## Timeline Summary

| Phase | Duration | Deliverable |
|-------|----------|------------|
| **Phase 1: Ownership** | 3 weeks | Schema owners + field authorship |
| **Phase 2: Column Lineage** | 4 weeks | Field → column mappings |
| **Phase 3: Enrichments** | 3 weeks | Enrichment DataJobs |
| **Phase 4: Data Products** | 2 weeks | Data Product Containers with ownership |
| **Phase 5: Tagging** | 1 week | Enhanced metadata |
| **Phase 6: Release** | 1 week | GA-ready connector |
| **Total** | **14 weeks** | Full implementation |

**Milestone Delivery:**

- **Week 3:** Ownership MVP (immediate customer value)
- **Week 7:** Column lineage (end-to-end governance)
- **Week 10:** Enrichments (complete feature set)
- **Week 12:** Data Products (organizational structure)
- **Week 14:** GA Release

---

## Appendix A: Configuration Examples

### Minimal Configuration (Current - 40% features)

```yaml
source:
  type: snowplow
  config:
    bdp_connection:
      organization_id: "customer-org-uuid"
      api_key_id: "${SNOWPLOW_API_KEY_ID}"
      api_key: "${SNOWPLOW_API_KEY}"

    schema_pattern:
      allow: ["com.acme.*"]

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

### Full Configuration (Target - 100% features)

```yaml
source:
  type: snowplow
  config:
    # Connection
    bdp_connection:
      organization_id: "customer-org-uuid"
      api_key_id: "${SNOWPLOW_API_KEY_ID}"
      api_key: "${SNOWPLOW_API_KEY}"

    # Schema filtering
    schema_pattern:
      allow: ["com.acme.*"]
      deny: [".*\\.test$"]

    # Warehouse connection for lineage
    warehouse_connection:
      warehouse_type: "snowflake"
      host: "customer.snowflakecomputing.com"
      database: "RAW"
      schema_name: "SNOWPLOW"
      username: "${SNOWFLAKE_USER}"
      password: "${SNOWFLAKE_PASSWORD}"
      warehouse_options:
        account: "customer"

    # PHASE 1: Ownership configuration
    ownership_config:
      strategy: "inherit_from_schema"
      track_version_authorship: true
      default_owner: "data-platform@acme.com"
      owner_email_domain: "@acme.com"
      extract_team_from_schema: true

    # PHASE 2: Column lineage configuration
    column_lineage_config:
      enable_column_lineage: true
      column_pattern: "contexts_{vendor}_{name}_{version}"
      parse_variant_columns: true
      variant_sample_size: 100

    # PHASE 3: Enrichment configuration
    enrichment_config:
      extract_enrichments: true
      enrichment_owner: "data-platform@acme.com"
      include_standard_enrichments: true
      enrichments:
        - name: "IP Lookup Enrichment"
          type: "ip_lookup"
          fields_added:
            - "geo_country"
            - "geo_city"
            - "geo_latitude"
            - "geo_longitude"
          owner: "data-platform@acme.com"
        - name: "Custom Enrichment"
          type: "custom"
          fields_added: ["custom_field"]
          owner: "analytics@acme.com"

    # Feature flags
    extract_event_specifications: true
    extract_tracking_scenarios: true
    extract_data_products: true  # NEW - Extract Data Products as Containers
    extract_warehouse_lineage: true
    extract_usage_statistics: true
    
    # NEW: Data Product configuration
    data_product_ownership_priority: true  # Data Product owner overrides schema owner

    # Usage configuration
    warehouse_lookback_days: 30

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

---

## Appendix B: DataHub Aspect Mapping

### Current Implementation (40%)

| Snowplow Concept | DataHub Entity | DataHub Aspect | Status |
|-----------------|----------------|----------------|--------|
| Iglu Schema | Dataset | DatasetProperties | ✅ |
| Schema Fields | SchemaField | SchemaMetadata | ✅ |
| Event Specification | Dataset | DatasetProperties | ✅ |
| Tracking Scenario | Container | ContainerProperties | ✅ |
| Organization | Container | ContainerProperties | ✅ |
| atomic.events table | Dataset | DatasetProperties | ✅ |
| Schema → Warehouse (table) | Dataset | UpstreamLineage | ✅ |
| Usage Stats | Dataset | DatasetUsageStatistics | ✅ |

### Target Implementation (100%)

| Snowplow Concept | DataHub Entity | DataHub Aspect | Phase |
|-----------------|----------------|----------------|-------|
| Schema Creator | Dataset | **Ownership** (DATAOWNER) | **Phase 1** |
| Schema Modifier | Dataset | **Ownership** (PRODUCER) | **Phase 1** |
| Field Author | SchemaField | **GlobalTags** (added_by_X) | **Phase 1** |
| Schema Version | Dataset | **GlobalTags** (schema_v1-0-0) | **Phase 4** |
| Event Type | Dataset | **GlobalTags** (event_checkout) | **Phase 4** |
| PII Flag | SchemaField | **GlobalTags** (pii_sensitive) | **Phase 4** |
| Field → Column | SchemaField | **FineGrainedLineage** | **Phase 2** |
| Enrichment | **DataJob** | **DataJobInfo** | **Phase 3** |
| Enrichment → Fields | DataJob | **DataJobInputOutput** | **Phase 3** |

---

## Appendix C: References

### DataHub Documentation

- [Ownership](https://datahubproject.io/docs/api/graphql/ownership/)
- [Fine-Grained Lineage](https://datahubproject.io/docs/lineage/lineage-feature-guide/#column-level-lineage)
- [DataJobs](https://datahubproject.io/docs/generated/metamodel/entities/datajob/)
- [Tags](https://datahubproject.io/docs/tags/)

### Snowplow Documentation

- [BDP Console API](https://console.snowplowanalytics.com/api/msc/v1/docs/)
- [Data Structures](https://docs.snowplow.io/docs/understanding-tracking-design/managing-your-data-structures/)
- [Enrichments](https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/)
- [Iglu Schema Registry](https://docs.snowplow.io/docs/api-reference/iglu/)

### Customer Resources

- Linear Issue: [ING-1233](https://linear.app/acryl-data/issue/ING-1233)
- Customer: Ryan's company
- Key Contact: Ryan
- Use Case: Ownership tracking for 8B row atomic.events table

---

## Document History

| Date | Version | Changes |
|------|---------|---------|
| 2024-12-05 | 1.0 | Initial implementation plan created |
| 2025-01-27 | 1.1 | Updated with API research findings:<br/>- Ownership via deployments array (initiator field)<br/>- initiatorId NOT available (critical issue)<br/>- Users API available for user caching<br/>- Authentication method corrected (GET not POST)<br/>- Data Products added to roadmap |
| TBD | 1.2 | Updated after Phase 1 completion |
| TBD | 2.0 | Final version post-GA release |

---

**Next Actions:**

1. ✅ Review this plan with team
2. ⏭️ Contact Ryan for sample data and API access
3. ⏭️ Begin Phase 1: Ownership research
4. ⏭️ Schedule weekly sync with customer
