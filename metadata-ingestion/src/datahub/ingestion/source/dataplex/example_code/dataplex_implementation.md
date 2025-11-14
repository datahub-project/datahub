I'll help you create an implementation spec for a DataHub connector that extracts basic information from Google Dataplex. Based on your mapping table, let me draft a comprehensive specification.# DataHub Google Dataplex Connector - Implementation Specification

## 1. Overview

### 1.1 Purpose

This connector extracts metadata from Google Dataplex Universal Catalog and the Dataplex Data Mesh (Lakes/Zones/Assets) into DataHub, enabling unified data discovery and governance across Google Cloud Platform data assets.

### 1.2 Scope

**Phase 1 (Initial Implementation):**

- Extract Projects as Containers
- Extract Lakes as Domains
- Extract Zones as Sub-domains
- Extract Assets as Data Products
- Extract Entities (discovered tables/filesets) as Datasets
- Basic metadata: names, descriptions, properties, ownership

**Future Phases:**

- Entry Groups and Entries extraction
- Custom Aspects mapping
- Advanced lineage
- Schema evolution tracking
- Data quality metrics

### 1.3 Architecture

```
Google Cloud Project
├── Dataplex API Client
├── BigQuery API Client (for schema details)
└── Cloud Storage API Client (for filesets)
    ↓
DataHub Ingestion Framework
    ↓
DataHub (via REST/GraphQL API)
```

---

## 2. Entity Mapping Specification

### 2.1 Project → Container

**Dataplex Source:** `projects/{project-id}`

**DataHub Target:**

```
URN: urn:li:container:{project-id}
Type: Project
Platform: dataplex
```

**Metadata to Extract:**

- `name`: Project display name
- `description`: Project description
- `properties`: Custom properties (labels, project number)
- `created`: Project creation timestamp

**API Call:**

```python
# Using Resource Manager API
project = resourcemanager_v3.ProjectsClient().get_project(
    name=f"projects/{project_id}"
)
```

---

### 2.2 Lake → Domain

**Dataplex Source:** `projects/{project}/locations/{location}/lakes/{lake-id}`

**DataHub Target:**

```
URN: urn:li:domain:{lake-id}
Type: Domain
```

**Metadata to Extract:**

- `name`: Lake name
- `displayName`: Lake display name
- `description`: Lake description
- `labels`: Lake labels (convert to DataHub tags)
- `createTime`: Creation timestamp
- `updateTime`: Last update timestamp
- `state`: Lake state (ACTIVE, CREATING, DELETING, ACTION_REQUIRED)

**Parent Relationship:**

- Container: `urn:li:container:{project-id}`

**API Call:**

```python
# Using Dataplex API
from google.cloud import dataplex_v1

client = dataplex_v1.DataplexServiceClient()
lake = client.get_lake(
    name=f"projects/{project}/locations/{location}/lakes/{lake_id}"
)
```

**Business Context:**
Lakes represent business domains (e.g., Retail, Finance, Analytics). The domain name should reflect business function.

---

### 2.3 Zone → Sub-domain

**Dataplex Source:** `projects/{project}/locations/{location}/lakes/{lake}/zones/{zone-id}`

**DataHub Target:**

```
URN: urn:li:domain:{zone-id}
Type: Domain (Sub-domain)
```

**Metadata to Extract:**

- `name`: Zone name
- `displayName`: Zone display name
- `description`: Zone description
- `type`: Zone type (RAW, CURATED)
- `labels`: Zone labels
- `resourceSpec`: Resource specifications
  - `locationType`: SINGLE_REGION, MULTI_REGION
- `discoverySpec`: Discovery configuration
- `state`: Zone state

**Parent Relationship:**

- Parent Domain: `urn:li:domain:{lake-id}`
- Container: `urn:li:container:{project-id}`

**Special Handling:**

- **Zone Type Tagging**: Apply DataHub tags based on zone type:
  - `RAW` → Tag: "Raw Data Zone"
  - `CURATED` → Tag: "Curated Data Zone"

**API Call:**

```python
zone = client.get_zone(
    name=f"projects/{project}/locations/{location}/lakes/{lake_id}/zones/{zone_id}"
)
```

---

### 2.4 Asset → Data Product

**Dataplex Source:** `projects/{project}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset-id}`

**DataHub Target:**

```
URN: urn:li:dataProduct:{asset-id}
Type: Data Product
```

**Metadata to Extract:**

- `name`: Asset name
- `displayName`: Asset display name
- `description`: Asset description
- `labels`: Asset labels
- `resourceSpec`: Resource specifications
  - `type`: STORAGE_BUCKET or BIGQUERY_DATASET
  - `name`: Underlying GCS bucket or BQ dataset name
- `resourceStatus`:
  - `state`: Asset state
  - `message`: Status message
- `discoverySpec`: Discovery specifications
  - `enabled`: Whether discovery is enabled
  - `includePatterns`: Patterns for inclusion
  - `excludePatterns`: Patterns for exclusion
  - `schedule`: Discovery schedule

**Parent Relationship:**

- Domain: `urn:li:domain:{zone-id}`
- Container: `urn:li:container:{project-id}`

**Asset Types:**

1. **BigQuery Dataset Asset**: Points to a BigQuery dataset
2. **Cloud Storage Asset**: Points to a GCS bucket

**API Call:**

```python
asset = client.get_asset(
    name=f"projects/{project}/locations/{location}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
)
```

---

### 2.5 Entity → Dataset

**Dataplex Source:** Discovered entities within an Asset

**DataHub Target:**

```
URN: urn:li:dataset:(urn:li:dataPlatform:{platform},{entity-id},PROD)
Platform: bigquery | gcs
SubType: table | view | fileset
```

**Metadata to Extract:**

- `name`: Entity name
- `id`: Entity identifier
- `displayName`: Display name
- `description`: Entity description
- `type`: Entity type (TABLE, FILESET)
- `system`: Source system (BIGQUERY, CLOUD_STORAGE)
- `format`: Data format
  - For tables: AVRO, PARQUET, ORC, CSV, JSON
  - For filesets: CSV, JSON, PARQUET, AVRO, ORC
- `schema`: Schema information
  - `fields`: Column definitions
  - `partitionFields`: Partition column names
  - `partitionStyle`: Partition style (HIVE_COMPATIBLE)
- `dataPath`: Underlying data location
- `createTime`: Creation timestamp
- `updateTime`: Last update timestamp

**Parent Relationships:**

- Data Product: `urn:li:dataProduct:{asset-id}`
- Domain: `urn:li:domain:{zone-id}`
- Container: `urn:li:container:{project-id}`

**Platform Mapping:**

- **BigQuery Entities**:
  ```
  urn:li:dataset:(urn:li:dataPlatform:bigquery,{project}.{dataset}.{table},PROD)
  ```
- **Cloud Storage Entities**:
  ```
  urn:li:dataset:(urn:li:dataPlatform:gcs,{bucket}/{path},PROD)
  ```

**API Call:**

```python
# List entities in a zone
entities = client.list_entities(
    parent=f"projects/{project}/locations/{location}/lakes/{lake_id}/zones/{zone_id}"
)

# Get individual entity
entity = client.get_entity(
    name=f"projects/{project}/locations/{location}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id}"
)
```

**Schema Extraction:**
For BigQuery tables, use BigQuery API to get detailed schema:

```python
from google.cloud import bigquery

bq_client = bigquery.Client()
table = bq_client.get_table(f"{project}.{dataset}.{table_name}")
schema = table.schema
```

---

### 2.6 Sibling Relationships

**Purpose:** Link Dataplex entities to existing DataHub datasets from native connectors (BigQuery, GCS)

**Implementation:**
When a Dataplex entity references a BigQuery table or GCS path that already exists in DataHub:

```python
# Create sibling relationship
sibling = {
    "primary": "urn:li:dataset:(urn:li:dataPlatform:bigquery,{project}.{dataset}.{table},PROD)",
    "sibling": "urn:li:dataset:(urn:li:dataPlatform:dataplex,{entity-id},PROD)",
    "associationType": "DATAPLEX_MANAGED"
}
```

**Detection Logic:**

1. Extract the underlying resource reference from Dataplex entity
2. Construct expected URN for native platform (BigQuery/GCS)
3. Check if URN exists in DataHub
4. If exists, create sibling relationship
5. Add `Dataplex Managed` tag to the native dataset

---

## 3. Configuration

### 3.1 Connection Configuration

```yaml
source:
  type: dataplex
  config:
    # GCP Project Configuration
    project_id: "my-gcp-project"

    # Authentication
    credentials_path: "/path/to/service-account-key.json"
    # OR use default application credentials
    use_default_credentials: true

    # Dataplex Configuration
    location: "us-central1" # Or "us", "eu", etc.

    # Filters
    lake_pattern:
      allow:
        - "retail-lake"
        - "finance-lake"
      deny:
        - "test-*"

    zone_pattern:
      allow:
        - ".*"
      deny:
        - "deprecated-*"

    asset_pattern:
      allow:
        - ".*"
      deny:
        - "temp-*"

    # Feature Flags
    extract_lakes: true
    extract_zones: true
    extract_assets: true
    extract_entities: true
    extract_entry_groups: false # Phase 2
    extract_entries: false # Phase 2

    # Sibling Detection
    create_sibling_relationships: true
    sibling_platforms:
      - bigquery
      - gcs

    # Performance
    max_workers: 10
    entity_batch_size: 100

    # Profiling
    profile_entities: false # Future: row counts, stats

    # Tags
    apply_zone_type_tags: true
    apply_label_tags: true
```

---

## 4. Implementation Details

### 4.1 Ingestion Flow

```
1. Initialize APIs
   ├── Dataplex Service Client
   ├── BigQuery Client (optional)
   └── Cloud Storage Client (optional)

2. Extract Project
   └── Create Container entity

3. Extract Lakes
   ├── List all lakes in project/location
   ├── For each lake:
   │   ├── Create Domain entity
   │   └── Link to Project container

4. Extract Zones
   ├── For each lake:
   │   ├── List all zones
   │   ├── For each zone:
   │   │   ├── Create Sub-domain entity
   │   │   ├── Link to parent Lake domain
   │   │   └── Apply zone type tags

5. Extract Assets
   ├── For each zone:
   │   ├── List all assets
   │   ├── For each asset:
   │   │   ├── Create Data Product entity
   │   │   └── Link to Zone domain

6. Extract Entities
   ├── For each zone:
   │   ├── List all entities
   │   ├── For each entity:
   │   │   ├── Determine platform (BigQuery/GCS)
   │   │   ├── Create Dataset entity
   │   │   ├── Extract schema (if available)
   │   │   ├── Link to Asset data product
   │   │   └── Check for sibling relationships

7. Create Sibling Relationships
   └── Link Dataplex entities to native platform datasets

8. Emit MCEs/MCPs
   └── Write to DataHub
```

---

### 4.2 Error Handling

**Strategy:**

- **Fail-fast on authentication/authorization errors**
- **Skip individual entities on access errors** (log warning)
- **Retry on transient API errors** (exponential backoff)
- **Continue processing remaining entities on individual failures**

**Error Categories:**

1. **Authentication Errors**: Stop immediately
2. **Permission Errors**: Skip entity, log warning
3. **API Rate Limits**: Implement exponential backoff
4. **Entity Not Found**: Skip, log info
5. **Schema Extraction Failures**: Continue without schema

---

### 4.3 Performance Considerations

**Optimization Strategies:**

1. **Parallel Processing**: Use thread pool for entity extraction (max_workers)
2. **Batch API Calls**: Use list operations with pagination
3. **Caching**: Cache project/lake/zone metadata
4. **Incremental Ingestion**: Track last update times (Phase 2)
5. **Schema Sampling**: For large tables, sample schema instead of full scan

**Rate Limits:**

- Dataplex API: 60 requests/minute/user (default)
- BigQuery API: 100 requests/second/project (default)
- Implement exponential backoff with jitter

---

### 4.4 Testing Strategy

**Unit Tests:**

- Mock Dataplex API responses
- Test entity mapping logic
- Test URN generation
- Test sibling detection

**Integration Tests:**

- Use test GCP project with known structure
- Verify all entity types are created
- Verify relationships are correct
- Verify schema extraction

**Test Data Structure:**

```
test-project
└── test-lake (Domain)
    ├── raw-zone (Sub-domain)
    │   └── raw-bucket-asset (Data Product)
    │       └── customer_data.csv (Dataset)
    └── curated-zone (Sub-domain)
        └── analytics-dataset (Data Product)
            ├── customers (Dataset)
            └── orders (Dataset)
```

---

## 5. Dependencies

### 5.1 Python Libraries

```
google-cloud-dataplex >= 1.0.0
google-cloud-bigquery >= 3.0.0
google-cloud-storage >= 2.0.0
google-cloud-resource-manager >= 1.0.0
acryl-datahub >= 0.12.0
```

### 5.2 GCP APIs to Enable

- Dataplex API (`dataplex.googleapis.com`)
- BigQuery API (`bigquery.googleapis.com`)
- Cloud Storage API (`storage.googleapis.com`)
- Cloud Resource Manager API (`cloudresourcemanager.googleapis.com`)

### 5.3 Required IAM Permissions

```
# Dataplex permissions
dataplex.lakes.get
dataplex.lakes.list
dataplex.zones.get
dataplex.zones.list
dataplex.assets.get
dataplex.assets.list
dataplex.entities.get
dataplex.entities.list

# BigQuery permissions (for schema)
bigquery.tables.get
bigquery.tables.list

# Storage permissions (for GCS metadata)
storage.buckets.get
storage.objects.list

# Project permissions
resourcemanager.projects.get
```

---

## 6. Output Examples

### 6.1 Domain (Lake)

```json
{
  "entityType": "domain",
  "entityUrn": "urn:li:domain:retail-lake",
  "changeType": "UPSERT",
  "aspectName": "domainProperties",
  "aspect": {
    "name": "Retail Lake",
    "description": "Domain for all retail-related data assets",
    "created": {
      "time": 1699564800000,
      "actor": "urn:li:corpuser:dataplex-system"
    }
  }
}
```

### 6.2 Sub-domain (Zone)

```json
{
  "entityType": "domain",
  "entityUrn": "urn:li:domain:retail-curated-zone",
  "changeType": "UPSERT",
  "aspectName": "domainProperties",
  "aspect": {
    "name": "Retail Curated Zone",
    "description": "Curated retail data ready for analytics",
    "parentDomain": "urn:li:domain:retail-lake",
    "created": {
      "time": 1699564800000,
      "actor": "urn:li:corpuser:dataplex-system"
    }
  }
}
```

### 6.3 Dataset (Entity)

```json
{
  "entityType": "dataset",
  "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.retail.customers,PROD)",
  "changeType": "UPSERT",
  "aspectName": "datasetProperties",
  "aspect": {
    "name": "customers",
    "description": "Customer master data table",
    "customProperties": {
      "dataplex_lake": "retail-lake",
      "dataplex_zone": "retail-curated-zone",
      "dataplex_asset": "retail-analytics-dataset",
      "dataplex_entity_type": "TABLE",
      "dataplex_format": "PARQUET"
    },
    "tags": ["Curated Data Zone", "Dataplex Managed"]
  }
}
```

---

## 7. Future Enhancements (Phase 2+)

### 7.1 Entry Groups & Entries

- Extract Universal Catalog entries
- Support custom entry types
- Map system entries to existing datasets

### 7.2 Custom Aspects

- Extract Dataplex Aspect Types
- Map to DataHub custom aspects
- Support column-level aspects

### 7.3 Advanced Features

- Incremental ingestion (track update times)
- Data quality metrics extraction
- Lineage from Dataplex lineage API
- Data profile statistics
- Access policy extraction
- Cost and usage metrics

### 7.4 Monitoring & Observability

- Ingestion metrics dashboard
- Alert on extraction failures
- Track entity coverage
- Monitor API quota usage

---

## 8. Open Questions

1. **URN Strategy**: Should we use Dataplex-specific URNs or native platform URNs for entities?

   - **Recommendation**: Use native platform URNs with sibling relationships

2. **Entry Groups**: Should we extract Entry Groups in Phase 1 or defer to Phase 2?

   - **Recommendation**: Defer to Phase 2 as they overlap with Lakes/Zones

3. **Custom Properties**: Which Dataplex labels should become tags vs custom properties?

   - **Recommendation**: User-defined labels → tags, system labels → custom properties

4. **Incremental Updates**: How frequently should we run ingestion?

   - **Recommendation**: Start with daily batch, add incremental in Phase 2

5. **Schema Evolution**: How should we handle schema changes over time?
   - **Recommendation**: Track in Phase 2 with schema versioning

---

This specification provides a solid foundation for implementing a DataHub connector for Google Dataplex. Would you like me to elaborate on any specific section or create code examples for the implementation?
