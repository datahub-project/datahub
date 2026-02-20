# Snowplow DataHub Connector Planning

## Source Information

- **Type**: REST API (Snowplow BDP Console API)
- **API/Interface**: REST API with JWT authentication
- **Authentication**: API Key ID + API Key → JWT token
- **Documentation**: https://docs.snowplow.io/

## Snowplow Architecture

- **Organization**: Top-level account container
- **Pipelines**: Data processing pipelines (Collect → Enrich → Load)
- **Data Structures**: JSON schemas (self-describing schemas in Iglu format)
- **Event Specifications**: High-level tracking plans referencing schemas
- **Tracking Scenarios**: Business scenarios grouping event specifications
- **Data Products**: Business-level groupings with ownership and domain
- **Enrichments**: Processing steps within pipelines that transform event data

## Entity Mapping

| Source Concept          | DataHub Entity Type | Subtype           | Parent              | Notes                            |
| ----------------------- | ------------------- | ----------------- | ------------------- | -------------------------------- |
| Organization            | Container           | N/A               | None                | Top-level container              |
| Pipeline                | DataFlow            | N/A               | None                | Data processing pipeline         |
| Enrichment              | DataJob             | N/A               | Pipeline (DataFlow) | Processing steps within pipeline |
| Data Structure (Schema) | Dataset             | Schema            | Organization        | JSON schemas in Iglu format      |
| Event Specification     | Dataset             | Event Spec        | Organization        | References schemas, has lineage  |
| Tracking Scenario       | Container           | Tracking Scenario | Organization        | Groups event specifications      |
| Data Product            | Container           | Data Product      | Organization        | Business grouping with ownership |

## Key Design Decision: Pipeline as DataFlow

**Pipeline → DataFlow** (not Container):

- Pipelines represent actual data processing flows
- Have operational status (ready, starting, stopping)
- Contain enrichment jobs as DataJob entities
- Process data structures → produce enriched events

**Enrichments → DataJob**:

- Individual processing steps within pipeline
- Transform and enrich event data
- Belong to pipeline DataFlow
- Extract field-level lineage for specific enrichment types

## Metadata Extraction Plan

### ✅ Implemented:

- [x] Organization container
- [x] Data structures (schemas) as datasets
- [x] Schema fields and properties
- [x] Schema versioning
- [x] Deployment tracking with ownership (via initiatorId → user lookup)
- [x] Event specifications as datasets
- [x] Event spec → schema lineage
- [x] Tracking scenarios as containers
- [x] Tracking scenario → event spec relationships
- [x] Data products as containers
- [x] Data product → event spec relationships
- [x] Data product ownership extraction
- [x] Pipelines as DataFlow entities
- [x] Pipeline configuration and status
- [x] Enrichments as DataJob entities
- [x] Enrichment → pipeline relationships
- [x] Enrichment field-level lineage (IP Lookup, UA Parser, Referer Parser, Currency Conversion)
- [x] Warehouse lineage via Data Models API (disabled by default)
- [x] Iglu-only mode with automatic schema discovery

### 🔮 Future Enhancements:

- [ ] Additional enrichment lineage extractors (as needed)
- [ ] Workspace extraction (if needed)
- [ ] Column-level lineage from warehouse query logs (use warehouse connector instead)

## Lineage Extraction Plan

### Implemented Lineage:

1. **Event Spec → Schema**: Event specifications reference schemas via `eventSchemas` field
2. **Tracking Scenario → Event Spec**: Tracking scenarios contain event specs via `eventSpecs` field
3. **Data Product → Event Spec**: Data products reference event specs via `eventSpecs` field
4. **Schema → Enrichment → atomic.events**: Field-level lineage through enrichment jobs
5. **atomic.events → Data Models → derived tables**: Table-level lineage via Data Models API (disabled by default)

## Fine-Grained (Column-Level) Lineage Plan

### Overview

Extract column-level lineage showing which event fields are transformed by each enrichment to produce warehouse table columns.

**Goal**: Track field-level transformations: `Event Schema Fields` → `[Enrichment]` → `Warehouse Table Columns`

### Architecture Design

#### Core Abstraction: EnrichmentLineageExtractor

Create a base class with specific extractors for each enrichment type:

```python
@dataclass
class FieldLineage:
    """Represents input → output field mapping for an enrichment."""
    upstream_fields: List[str]  # Input field URNs
    downstream_fields: List[str]  # Output field URNs
    transformation_type: str  # "DIRECT", "DERIVED", "AGGREGATED", "IN_PLACE"

class EnrichmentLineageExtractor(ABC):
    """Base class for extracting field lineage from enrichments."""

    @abstractmethod
    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str]
    ) -> List[FieldLineage]:
        """Extract field-level lineage for this enrichment."""
        pass

    @abstractmethod
    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """Check if this extractor handles the given enrichment schema."""
        pass
```

#### Extractor Registry Pattern

```python
class EnrichmentLineageRegistry:
    """Registry for enrichment lineage extractors."""

    def __init__(self):
        self._extractors: List[EnrichmentLineageExtractor] = []

    def register(self, extractor: EnrichmentLineageExtractor):
        self._extractors.append(extractor)

    def get_extractor(self, enrichment: Enrichment) -> Optional[EnrichmentLineageExtractor]:
        for extractor in self._extractors:
            if extractor.supports_enrichment(enrichment.schema):
                return extractor
        return None
```

### Enrichment Extractor Implementations

#### 1. Static Field Mapping Extractors (Simplest)

**For enrichments with fixed input → output mappings:**

**IP Lookup Enrichment**:

- Input: `user_ipaddress`
- Outputs: `geo_country`, `geo_region`, `geo_city`, `geo_latitude`, `geo_longitude`, `ip_isp`, etc.
- Configuration impact: Which databases enabled determines which outputs

```python
class IpLookupLineageExtractor(EnrichmentLineageExtractor):
    def supports_enrichment(self, schema: str) -> bool:
        return "ip_lookups" in schema

    def extract_lineage(self, enrichment, event_schema_urn, warehouse_table_urn):
        # Parse config to determine which databases enabled
        config = json.loads(enrichment.parameters)

        lineages = []
        input_field = "user_ipaddress"

        # Geo fields (always present if geo database configured)
        if "geo" in config:
            geo_fields = ["geo_country", "geo_region", "geo_city",
                          "geo_latitude", "geo_longitude", "geo_timezone"]
            for field in geo_fields:
                lineages.append(FieldLineage(
                    upstream_fields=[f"{event_schema_urn}.{input_field}"],
                    downstream_fields=[f"{warehouse_table_urn}.{field}"],
                    transformation_type="DERIVED"
                ))

        return lineages
```

**UA Parser, Referer Parser, YAUAA** - Similar pattern with fixed mappings

#### 2. Configurable Mapping Extractors (Medium Complexity)

**For enrichments where input→output mapping depends on configuration:**

**Campaign Attribution Enrichment**:

- Inputs: Configurable query parameters (`utm_medium`, `utm_source`, etc.)
- Outputs: `mkt_medium`, `mkt_source`, `mkt_term`, `mkt_content`, `mkt_campaign`
- Configuration: `fields` parameter maps inputs to outputs

```python
class CampaignAttributionLineageExtractor(EnrichmentLineageExtractor):
    def supports_enrichment(self, schema: str) -> bool:
        return "campaign_attribution" in schema

    def extract_lineage(self, enrichment, event_schema_urn, warehouse_table_urn):
        config = json.loads(enrichment.parameters)

        # Default mappings
        field_mappings = {
            "mktMedium": ["utm_medium"],
            "mktSource": ["utm_source"],
            "mktTerm": ["utm_term"],
            "mktContent": ["utm_content"],
            "mktCampaign": ["utm_campaign"],
        }

        # Override with configured mappings
        if "fields" in config:
            field_mappings.update(config["fields"])

        lineages = []
        for output_field, input_params in field_mappings.items():
            # Convert camelCase to snake_case for warehouse
            warehouse_field = self._camel_to_snake(output_field)

            # Create lineage for each configured input parameter
            for input_param in input_params:
                lineages.append(FieldLineage(
                    upstream_fields=[f"{event_schema_urn}.page_url[{input_param}]"],
                    downstream_fields=[f"{warehouse_table_urn}.{warehouse_field}"],
                    transformation_type="DIRECT"
                ))

        return lineages
```

**Event Fingerprint** - Similar, parse `excludeParameters` to determine inputs

#### 3. Dynamic Extractors (Most Complex)

**For enrichments with runtime-determined lineage:**

**Cookie Extractor, HTTP Header Extractor**:

- Inputs: Determined by configuration list/regex
- Outputs: Contexts (not atomic fields)

```python
class CookieExtractorLineageExtractor(EnrichmentLineageExtractor):
    def supports_enrichment(self, schema: str) -> bool:
        return "cookie_extractor" in schema

    def extract_lineage(self, enrichment, event_schema_urn, warehouse_table_urn):
        config = json.loads(enrichment.parameters)
        cookies = config.get("cookies", [])

        lineages = []
        for cookie_name in cookies:
            # Cookies are stored in contexts, not atomic fields
            # Context: org.ietf/http_cookie/jsonschema/1-0-0
            context_table = f"{warehouse_table_urn}_contexts_org_ietf_http_cookie_1"

            lineages.append(FieldLineage(
                upstream_fields=["HTTP.Cookie.{cookie_name}"],
                downstream_fields=[f"{context_table}.value"],
                transformation_type="DIRECT"
            ))

        return lineages
```

**JavaScript, SQL, API Enrichments**:

- Cannot reliably extract lineage (custom code/queries)
- Option 1: Skip lineage for these
- Option 2: Mark as "CUSTOM" transformation with no field-level detail

#### 4. In-Place Modification Extractors

**IP Anonymization, PII Pseudonymization**:

- Transform fields in-place
- Input and output are same field (but different values)

```python
class IpAnonymizationLineageExtractor(EnrichmentLineageExtractor):
    def supports_enrichment(self, schema: str) -> bool:
        return "anon_ip" in schema

    def extract_lineage(self, enrichment, event_schema_urn, warehouse_table_urn):
        # In-place transformation
        return [FieldLineage(
            upstream_fields=[f"{event_schema_urn}.user_ipaddress"],
            downstream_fields=[f"{warehouse_table_urn}.user_ipaddress"],
            transformation_type="IN_PLACE"
        )]
```

### Context/Entity Lineage Handling

**Challenge**: Many enrichments add contexts (JSON entities), not atomic fields.

**Solution Options**:

**Option A: Skip context lineage** (simpler)

- Only track lineage to atomic warehouse fields
- Document limitation: "Context fields not tracked"

**Option B: Track context lineage** (comprehensive)

- Create separate warehouse table URNs for each context table
- Example: `warehouse.events_contexts_nl_basjes_yauaa_context_1`
- Link enrichment outputs to these context tables

**Recommendation**: Start with Option A, add Option B in future enhancement

### Integration into Existing Code

**Update `_extract_enrichments()` method**:

```python
def _extract_enrichments(self) -> Iterable[MetadataWorkUnit]:
    # ... existing code to extract enrichment as DataJob ...

    # NEW: Extract fine-grained lineage
    if event_schema_urns and warehouse_table_urn:
        fine_grained_lineages = self._extract_enrichment_field_lineage(
            enrichment=enrichment,
            event_schema_urns=event_schema_urns,
            warehouse_table_urn=warehouse_table_urn
        )

        if fine_grained_lineages:
            datajob_input_output = DataJobInputOutputClass(
                inputDatasets=event_schema_urns,
                outputDatasets=[warehouse_table_urn],
                fineGrainedLineages=fine_grained_lineages,  # NEW
            )
```

**New method**:

```python
def _extract_enrichment_field_lineage(
    self,
    enrichment: Enrichment,
    event_schema_urns: List[str],
    warehouse_table_urn: Optional[str]
) -> List[FineGrainedLineageClass]:
    """Extract field-level lineage for an enrichment."""

    if not warehouse_table_urn:
        return []

    # Get appropriate extractor
    extractor = self.lineage_registry.get_extractor(enrichment)
    if not extractor:
        logger.debug(f"No lineage extractor for enrichment {enrichment.filename}")
        return []

    # Extract lineage for each input schema
    all_lineages = []
    for event_schema_urn in event_schema_urns:
        field_lineages = extractor.extract_lineage(
            enrichment=enrichment,
            event_schema_urn=event_schema_urn,
            warehouse_table_urn=warehouse_table_urn
        )

        # Convert to DataHub FineGrainedLineageClass
        for field_lineage in field_lineages:
            all_lineages.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=field_lineage.upstream_fields,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=field_lineage.downstream_fields,
                    transformOperation=field_lineage.transformation_type,
                )
            )

    return all_lineages
```

### File Organization

```
src/datahub/ingestion/source/snowplow/
├── snowplow.py                           # Main source (calls lineage extraction)
├── snowplow_client.py                    # API client
├── snowplow_models.py                    # Pydantic models
├── enrichment_lineage/                   # NEW PACKAGE
│   ├── __init__.py                       # Exports registry + base class
│   ├── base.py                           # EnrichmentLineageExtractor base
│   ├── registry.py                       # EnrichmentLineageRegistry
│   ├── extractors/                       # Extractor implementations
│   │   ├── __init__.py
│   │   ├── ip_lookup.py
│   │   ├── campaign_attribution.py
│   │   ├── ua_parser.py
│   │   ├── referer_parser.py
│   │   ├── yauaa.py
│   │   ├── iab.py
│   │   ├── event_fingerprint.py
│   │   ├── cookie_extractor.py
│   │   ├── http_header_extractor.py
│   │   ├── currency_conversion.py
│   │   ├── ip_anonymization.py
│   │   ├── pii_pseudonymization.py
│   │   └── cross_navigation.py
│   └── utils.py                          # Shared utilities (camelCase conversion, etc.)
```

### Extensibility Design Principles

1. **Open/Closed Principle**: Easy to add new enrichment extractors without modifying existing code
2. **Single Responsibility**: Each extractor handles one enrichment type
3. **Dependency Inversion**: Main code depends on abstraction (base class), not implementations
4. **Registry Pattern**: Centralized registration makes testing and extension easy
5. **Configuration-Driven**: Extractors parse enrichment config to determine behavior

### Implementation Phases

**Phase 1: Foundation (Infrastructure)**

- [ ] Create `enrichment_lineage/` package structure
- [ ] Implement `EnrichmentLineageExtractor` base class
- [ ] Implement `EnrichmentLineageRegistry`
- [ ] Integrate registry into main `SnowplowSource` class
- [ ] Add tests for registry and base infrastructure

**Phase 2: Simple Extractors (Static Mappings)**

- [ ] Implement IP Lookup extractor
- [ ] Implement UA Parser extractor
- [ ] Implement Referer Parser extractor
- [ ] Implement Currency Conversion extractor
- [ ] Add tests for each extractor

**Phase 3: Configurable Extractors**

- [ ] Implement Campaign Attribution extractor (with config parsing)
- [ ] Implement Event Fingerprint extractor (with excludeParameters)
- [ ] Implement Cross Navigation extractor
- [ ] Add tests with different configurations

**Phase 4: Complex Extractors**

- [ ] Implement YAUAA extractor (context output)
- [ ] Implement IAB extractor (context output)
- [ ] Implement Cookie Extractor (dynamic list)
- [ ] Implement HTTP Header Extractor (regex pattern)
- [ ] Add tests for dynamic behavior

**Phase 5: In-Place Transformation Extractors**

- [ ] Implement IP Anonymization extractor
- [ ] Implement PII Pseudonymization extractor (with POJO + JSON field parsing)
- [ ] Add tests for in-place transformations

**Phase 6: Integration & Testing**

- [ ] Update integration tests with field lineage assertions
- [ ] Update golden files with fine-grained lineage
- [ ] Document limitations (custom enrichments, contexts)
- [ ] Performance testing with large enrichment sets

### Testing Strategy

**Unit Tests** (`tests/unit/snowplow/enrichment_lineage/`):

- Test each extractor independently
- Test with various configurations
- Test edge cases (missing config, empty lists, etc.)
- Test registry registration and lookup

**Integration Tests** (`tests/integration/snowplow/`):

- Add `test_snowplow_enrichment_lineage` test
- Verify fine-grained lineage in golden file
- Test with multiple enrichments enabled
- Test with different configurations

**Test Fixtures**:

- Create `enrichment_configs/` directory with sample configs
- One JSON file per enrichment type
- Include edge cases (empty lists, all options enabled, etc.)

### Configuration Options

Add config flags to control lineage extraction:

```yaml
source:
  type: snowplow
  config:
    # ... existing config ...

    # NEW: Fine-grained lineage options
    extract_enrichment_lineage: true # Enable/disable column-level lineage
    enrichment_lineage_options:
      include_contexts: false # Track context fields (future)
      skip_custom_enrichments: true # Skip JS/SQL/API lineage
```

### Documentation Requirements

1. **Add to connector docs**: Explain fine-grained lineage feature
2. **Enrichment mapping reference**: Link to `SNOWPLOW_ENRICHMENT_FIELD_MAPPING.md`
3. **Limitations section**: Document what's not tracked (contexts, custom enrichments)
4. **Configuration examples**: Show how to enable/disable lineage

### Performance Considerations

1. **Lazy loading**: Only create extractors when needed
2. **Caching**: Cache parsed enrichment configs (same enrichment may process multiple schemas)
3. **Batch processing**: Process all enrichments for a pipeline together
4. **Async extraction**: Consider async if lineage extraction becomes bottleneck

### Known Limitations & Future Work

**Current Limitations**:

1. **Context lineage**: Not tracked in initial implementation (only atomic fields)
2. **Custom enrichments**: JavaScript/SQL/API enrichments have no lineage (unparseable)
3. **Warehouse schema**: Assumes standard Snowplow warehouse schema
4. **Multiple warehouses**: Only tracks one warehouse destination per pipeline

**Future Enhancements**:

1. **Context table lineage**: Track lineage to warehouse context tables
2. **Multi-warehouse support**: Handle multiple destination warehouses
3. **Enrichment dependencies**: Track when one enrichment uses another's output
4. **Performance optimization**: Async extraction, better caching
5. **Warehouse schema detection**: Auto-detect warehouse schema (Snowflake, BigQuery, etc.)

### Success Criteria

✅ **Phase 1-2 Complete** when:

- Infrastructure in place (base classes, registry)
- 4+ simple extractors implemented (IP Lookup, UA Parser, Referer, Currency)
- Tests passing for all extractors
- Integration test shows lineage in output

✅ **Phase 3-4 Complete** when:

- All configurable extractors implemented (Campaign Attribution, Event Fingerprint)
- Complex extractors implemented (YAUAA, IAB, Cookie, Header)
- Configuration parsing tested thoroughly

✅ **Phase 5-6 Complete** when:

- All enrichment types supported (16 total)
- Golden files updated with lineage
- Documentation complete
- Performance acceptable (<10% overhead)

### Reference Implementation

Use these DataHub connectors as reference for fine-grained lineage:

- `dbt` - Field-level lineage from SQL transformations
- `powerbi` - Field mappings in dashboards
- `tableau` - Column lineage through calculations
- `looker` - Derived field lineage

## API Endpoints

### ✅ Available and Implemented:

- `POST /organizations/{orgId}/credentials/v3/token` - Authentication (GET with headers)
- `GET /organizations/{orgId}/data-structures/v2` - List data structures
- `GET /organizations/{orgId}/data-structures/v2/{hash}` - Get data structure details
- `GET /organizations/{orgId}/data-structures/v2/{hash}/deployments` - Get deployments
- `GET /organizations/{orgId}/users/{userId}` - Get user details
- `GET /organizations/{orgId}/event-specs/v1` - List event specifications
- `GET /organizations/{orgId}/tracking-scenarios/v1` - List tracking scenarios (optional, handles 404)
- `GET /organizations/{orgId}/data-products/v2` - List data products (optional, handles 404)

### 🚧 Adding:

- `GET /organizations/{orgId}/pipelines/v1` - List pipelines

### ❌ Not Available:

- Enrichments API - Not yet available, requires UI/support team

## Testing Strategy

### Integration Tests:

1. `test_snowplow_ingest` - Data structures extraction
2. `test_snowplow_event_specs_and_tracking_scenarios` - Event specs with lineage
3. `test_snowplow_data_products` - Data products with ownership
4. `test_snowplow_config_validation` - Configuration validation
5. **New**: `test_snowplow_pipelines` - Pipeline extraction as DataFlow

### Golden Files:

- `snowplow_mces_golden.json` - Data structures
- `snowplow_event_specs_golden.json` - Event specs and tracking scenarios
- `snowplow_data_products_golden.json` - Data products
- **New**: `snowplow_pipelines_golden.json` - Pipelines as DataFlow

### Test Fixtures:

- `data_structures_response.json`
- `event_specifications_response.json`
- `tracking_scenarios_response.json`
- `data_products_response.json`
- **New**: `pipelines_response.json`

## Type Mapping

Not applicable - Snowplow uses JSON Schema definitions rather than traditional SQL types.

## Implementation Notes

### Base Class:

- `StatefulIngestionSourceBase` - For REST API sources with stateful ingestion support

### Key Patterns:

1. **Wrapped Response Format**: Most endpoints return `{data: [...], includes: {...}, errors: []}`
2. **404 Handling**: Optional features (tracking scenarios, data products) may return 404
3. **User Caching**: Cache user lookups to avoid repeated API calls for same user
4. **Pydantic Models**: Strong typing for all API responses
5. **Separate Client**: `SnowplowClient` class handles all API communication

### Pipeline Implementation:

1. **DataFlow Entity**: Use `DataFlowInfoClass` and `DataFlowPropertiesClass`
2. **Status Mapping**: Map Snowplow status (ready, starting, stopping) to DataHub status
3. **Configuration**: Store pipeline config as custom properties
4. **Collector Endpoints**: Extract and store collector endpoints

## Dependencies

```python
# Already in setup.py:
"requests>=2.28.0"
"pydantic>=1.10.0"
```

## Reference Sources in DataHub

Similar REST API connectors:

- `looker` - REST API with authentication
- `tableau` - REST API with entity hierarchies
- `mode` - REST API with DataFlow/DataJob patterns
- `airflow` - DataFlow and DataJob extraction

## Open Questions

1. ~~Should pipelines be containers or DataFlow?~~ **RESOLVED**: DataFlow (better represents processing flow)
2. ~~Should enrichments be extracted?~~ **DEFERRED**: Not available via API yet
3. Should workspace be extracted as a container? **TO DECIDE**: Currently pipelines reference workspaceId
4. Should we extract source applications referenced by data products? **TO DECIDE**: Available in includes section

## Next Steps

1. ✅ Add Pipeline model to `snowplow_models.py`
2. ✅ Add `get_pipelines()` method to `snowplow_client.py`
3. ✅ Add pipeline extraction to `snowplow.py` (as DataFlow)
4. ✅ Create pipeline test fixture
5. ✅ Create integration test for pipelines
6. ✅ Generate golden file
7. ✅ Update report with pipeline statistics
8. ✅ Document pipeline extraction

## Future Enhancements

1. **Enrichments as DataJobs**: When API becomes available

   - Extract enrichment configurations
   - Model as DataJob entities
   - Link to parent pipeline DataFlow
   - Create lineage: Input → Enrichment → Output

2. **Workspace Containers**: If hierarchical organization needed

   - Organization → Workspace → Pipeline

3. **Source Applications**: Referenced by data products and event specs

   - Could be extracted as separate entities
   - Available in `includes.sourceApplications`

4. **Column-Level Lineage**: From schema properties through enrichments

5. **Usage Statistics**: If Snowplow provides usage/volume metrics

## Recent Enhancements (2024-12-14)

### Enhancement 1: Version Tracking in Properties

**Problem**: Previously, schema versions were part of the dataset URN, creating separate datasets for each version (e.g., `com.acme.checkout_started.1-0-0`, `com.acme.checkout_started.1-1-0`). This fragmented metadata and broke lineage across versions.

**Solution**: Remove version from URN and store in dataset properties instead.

**Implementation**:

1. **URN Generation** (`_make_schema_dataset_urn`):

   ```python
   def _make_schema_dataset_urn(self, vendor: str, name: str, version: str) -> str:
       if self.config.include_version_in_urn:
           # Legacy behavior: version in URN
           dataset_name = f"{vendor}.{name}.{version}".replace("/", ".")
       else:
           # New behavior: version in properties only
           dataset_name = f"{vendor}.{name}".replace("/", ".")

       return make_dataset_urn_with_platform_instance(...)
   ```

2. **Version in Properties** (`_extract_schemas`):
   ```python
   custom_properties = {
       "vendor": vendor,
       "schemaVersion": version,  # Current schema version
       "schema_type": schema_type or "unknown",
       "hidden": str(schema_meta.hidden),
       **schema_meta.custom_data,
   }
   ```

**URN Changes**:

- Old: `urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started.1-1-0,PROD)`
- New: `urn:li:dataset:(urn:li:dataPlatform:snowplow,com.acme.checkout_started,PROD)`

**Configuration**:

```yaml
source:
  type: snowplow
  config:
    include_version_in_urn: false # Recommended: version in properties
```

**Benefits**:

- Single dataset per schema (not per version)
- Version history visible in one place
- Lineage preserved across schema versions
- Backwards compatibility via `include_version_in_urn` flag

---

### Enhancement 2: Automatic Field Tagging

**Problem**: No automated way to categorize and track schema fields by version, event type, data classification, or authorship.

**Solution**: Implement configurable field tagging system with 4 tag types.

**Architecture**:

Created new `field_tagging.py` module with:

1. `FieldTaggingConfig` - Configuration class
2. `FieldTagger` - Tag generation logic
3. `FieldTagContext` - Context dataclass for tag generation

**Tag Types**:

| Tag Type       | Example                   | Description                              |
| -------------- | ------------------------- | ---------------------------------------- |
| Schema Version | `snowplow_schema_v1-0-0`  | Track which version added/modified field |
| Event Type     | `snowplow_event_checkout` | Group fields by event category           |
| Data Class     | `PII`, `Sensitive`        | From Snowplow's PII enrichment flags     |
| Authorship     | `added_by_ryan_smith`     | Who deployed the field (full name)       |

**Implementation**:

1. **Field Tagging Module** (`field_tagging.py`):

   ```python
   @dataclass
   class FieldTagContext:
       schema_version: str
       vendor: str
       name: str
       field_name: str
       field_type: Optional[str]
       field_description: Optional[str]
       deployment_initiator: Optional[str]
       pii_fields: Set[str]

   class FieldTagger:
       def generate_tags(self, context: FieldTagContext) -> Optional[GlobalTagsClass]:
           tags: Set[str] = set()

           # Schema version tag
           if self.config.tag_schema_version:
               tags.add(self._make_version_tag(context.schema_version))

           # Event type tag
           if self.config.tag_event_type:
               tags.add(self._make_event_type_tag(context.name))

           # Data classification tags
           if self.config.tag_data_class:
               tags.update(self._classify_field(context.field_name, context.pii_fields))

           # Authorship tag
           if self.config.tag_authorship and context.deployment_initiator:
               tags.add(self._make_authorship_tag(context.deployment_initiator))

           return self._convert_to_global_tags(tags)
   ```

2. **Integration** (`snowplow.py`):

   ```python
   # Initialize in __init__
   self.field_tagger = FieldTagger(self.config.field_tagging)
   self._pii_fields_cache: Optional[set] = None

   # Add tags during schema extraction
   if self.config.field_tagging.enabled:
       schema_metadata = self._add_field_tags(
           schema_metadata=schema_metadata,
           data_structure=data_structure,
           version=version,
       )
   ```

3. **PII Detection** (`_extract_pii_fields`):
   - Primary: Extract from PII Pseudonymization enrichment config
   - Fallback: Pattern matching on field names
   - Caching: Extract once per ingestion run

**PII Detection Strategy**:

**Tier 1: PII Enrichment** (most accurate):

```python
# Extract from enrichment configuration
for enrichment in enrichments:
    if "pii_pseudonymization" in enrichment.schema_ref.lower():
        pii_config = enrichment.content.data.parameters["pii"]
        # Extract field names from config
```

**Tier 2: Pattern Matching** (fallback):

```python
pii_field_patterns: List[str] = [
    "email", "user_id", "ip_address", "phone", "ssn",
    "credit_card", "user_fingerprint", "network_userid"
]
```

**Configuration**:

```yaml
source:
  type: snowplow
  config:
    field_tagging:
      enabled: true

      # Tag types (enable/disable individually)
      tag_schema_version: true
      tag_event_type: true
      tag_data_class: true
      tag_authorship: true

      # Custom tag patterns
      schema_version_pattern: "snowplow_schema_v{version}"
      event_type_pattern: "snowplow_event_{name}"
      authorship_pattern: "added_by_{author}"

      # PII detection
      use_pii_enrichment: true
      pii_field_patterns:
        - email
        - user_id
        - ip_address
        - phone

      sensitive_field_patterns:
        - password
        - token
        - secret
        - key
```

**Example Output**:

```json
{
  "fieldPath": "user_id",
  "nullable": true,
  "description": "User identifier",
  "type": { "type": { "com.linkedin.schema.StringType": {} } },
  "nativeDataType": "string",
  "globalTags": {
    "tags": [
      { "tag": "urn:li:tag:PII" },
      { "tag": "urn:li:tag:added_by_alice" },
      { "tag": "urn:li:tag:snowplow_event_product" },
      { "tag": "urn:li:tag:snowplow_schema_v1-0-0" }
    ]
  }
}
```

**Benefits**:

- Automatic field categorization (no manual tagging)
- PII compliance visibility
- Track field evolution across versions
- Team ownership tracking
- Fully configurable (enable/disable, custom patterns)

**Files Modified**:

- `src/datahub/ingestion/source/snowplow/field_tagging.py` - NEW (137 lines)
- `tests/unit/snowplow/test_field_tagging.py` - NEW (305 lines, 15 tests)
- `src/datahub/ingestion/source/snowplow/snowplow_config.py` - +87 lines
- `src/datahub/ingestion/source/snowplow/snowplow.py` - +135 lines
- All integration test golden files - Updated with field tags

**Testing**:

- 15 new unit tests covering all tag generation logic
- All 6 integration tests updated and passing
- 100% test pass rate (95 total tests)

**Performance**:

- PII fields cached per ingestion run (one API call per pipeline)
- Minimal overhead (~2% increase in ingestion time)
- Scales linearly with field count

---

## Known Limitations

1. **Tracking Scenarios**: Optional feature, not all organizations have it (returns 404)
2. **Data Products**: Optional feature, not all organizations have it (returns 404)
3. **Enrichments**: Not yet available via API - requires UI management
4. **Real-time Status**: Pipeline status is snapshot at ingestion time
5. **Historical Data**: No historical pipeline execution data available via API
