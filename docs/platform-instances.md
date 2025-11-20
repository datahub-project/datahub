# Working With Platform Instances

DataHub's metadata model for Datasets supports a three-part key currently:

- Data Platform (e.g. urn:li:dataPlatform:mysql)
- Name (e.g. db.schema.name)
- Env or Fabric (e.g. DEV, PROD, etc.)

This naming scheme unfortunately does not allow for easy representation of the multiplicity of platforms (or technologies) that might be deployed at an organization within the same environment or fabric. For example, an organization might have multiple Redshift instances in Production and would want to see all the data assets located in those instances inside the DataHub metadata repository.

**Note**: While platform instances provide one solution to this problem it comes with trade-offs with respect to immutability. DataHub also offers alternative approaches for organizing and managing multiple platform instances. See the [Alternative Approaches](#alternative-approaches) section below for more information.

As part of the `v0.8.24+` releases, we are unlocking the first phase of supporting Platform Instances in the metadata model. This is done via two main additions:

- The `dataPlatformInstance` aspect that has been added to Datasets which allows datasets to be associated to an instance of a platform
- Enhancements to all ingestion sources that allow them to attach a platform instance to the recipe that changes the generated urns to go from `urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,ENV)` format to `urn:li:dataset:(urn:li:dataPlatform:<platform>,<instance.name>,ENV)` format. Sources that produce lineage to datasets in other platforms (e.g. Looker, Superset etc) also have specific configuration additions that allow the recipe author to specify the mapping between a platform and the instance name that it should be mapped to.

## ⚠️ Critical: URN Immutability

**DataHub URNs are immutable identifiers that must remain unchanged once assigned to an entity.** This immutability is fundamental to maintaining data integrity, lineage tracking, and consistent references throughout the system. Once a URN is created, it should never be modified, even if the underlying data asset's attributes change.

### The URN Immutability Challenge

Many organizations face a critical challenge: **URNs serve dual purposes** - they are both internal system identifiers AND visible user-facing identifiers in the DataHub UI. This creates a conflict when organizational taxonomy changes (domains, products, systems) because:

1. **Orphaned Assets**: When URNs change, all metadata added outside of ingestion (descriptions, tags, lineage, ownership) associated with the old asset
2. **Integration Disruption**: Downstream applications and integrations that rely on specific URNs break
3. **User Confusion**: URNs visible in the UI become outdated and misleading
4. **Operational Overhead**: Teams must migrate all references to new URNs

### Solution: Separate Technical Identifiers from Business Context

When establishing platform instance naming conventions, it is crucial to choose names that are:

- **Intrinsic to the data**: Based on stable, inherent properties of the data asset
- **Not subject to change**: Avoid names that might change due to organizational restructuring, technology migrations, or operational changes
- **Consistent across all ingestion sources**: The same platform instance name must be used consistently across all recipes to ensure URN alignment

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/platform-instances-for-ingestion.png"/>
</p>

## Naming Platform Instances

When configuring a platform instance, choose an instance name that is understandable and will be stable for the foreseeable future. e.g. `core_warehouse` or `finance_redshift` are allowed names, as are pure guids like `a37dc708-c512-4fe4-9829-401cd60ed789`. Remember that whatever instance name you choose, you will need to specify it in more than one recipe to ensure that the identifiers produced by different sources will line up.

### Best Practices for Platform Instance Naming

To ensure URN immutability and long-term stability, platform instance names should be **technical identifiers** that are intrinsic to the infrastructure, not business concepts. Use DataHub's built-in features for domains, ownership, and business context.

**✅ Good Examples:**

- **Infrastructure identifiers**: `us-east-1-cluster-1`, `eu-west-2-cluster-2`
- **Technical naming**: `primary-redshift`, `secondary-mysql`, `analytics-snowflake`
- **GUIDs/UUIDs**: `a37dc708-c512-4fe4-9829-401cd60ed789`
- **Infrastructure codes**: `rds-prod-001`, `redshift-analytics-01`

**❌ Avoid These Patterns:**

- **Organizational taxonomy**: `company.domain.product.system` (domains, products, systems change)
- **Business domain names**: `customer_data_warehouse`, `finance_redshift` (use DataHub domains instead)
- **Ownership references**: `john_warehouse`, `sarah_analytics` (use DataHub ownership features)
- **Version numbers**: `redshift_v2`, `mysql_8_0` (use DataHub's versioning capabilities)
- **Temporary indicators**: `temp_warehouse`, `migration_db`
- **Technology migration names**: `legacy_mysql`, `old_redshift` (use DataHub tags instead)

**Key Principles:**

1. **Technical focus**: Use infrastructure-level identifiers, not business concepts
2. **Stability**: Choose names that reflect permanent technical characteristics
3. **Consistency**: Use the same naming pattern across all platform instances
4. **Uniqueness**: Ensure each platform instance has a unique identifier
5. **Separation of concerns**: Use DataHub's domain and ownership features for business context

**Note**: Business context like domains, ownership, data classification, and technology migration status should be managed through DataHub's dedicated features (domains, ownership, tags, etc.) rather than embedded in the platform instance name. Environment information is best handled by tags instead of fabric type which allows for promotion over time, and versioning should use DataHub's versioning capabilities.

## Enabling Platform Instances

Read the Ingestion source specific guides for how to enable platform instances in each of them.
The general pattern is to add an additional optional configuration parameter called `platform_instance`.

e.g. here is how you would configure a recipe to ingest a mysql instance that you want to call `primary-mysql`

```yaml
source:
  type: mysql
  config:
    # Coordinates
    host_port: localhost:3306
    platform_instance: primary-mysql
    database: dbname

    # Credentials
    username: root
    password: example

sink:
  # sink configs
```

## Alternative Solutions to URN Immutability Challenges

Instead of changing URNs when organizational taxonomy evolves, DataHub provides several alternative approaches that maintain URN immutability while enabling flexible business context management:

### Recommended Approach: Separate Technical from Business Context

The most effective solution is to design your platform instance naming to be **technically stable** while using DataHub's metadata features for **business context**:

1. **Use Stable Technical Identifiers**: Design platform instance names that won't change

   - ✅ `us-east-1-cluster-001`, `anomalo-prod-01`, `primary-redshift`
   - ❌ `company.domain.product.system` (changes when taxonomy evolves)

2. **Leverage DataHub's Business Context Features**:
   - **Data Products**: Group related assets for business purposes
   - **Tags and Custom Properties**: Add flexible metadata that can be updated
   - **Glossary Terms**: Associate business concepts with technical assets
   - **Domains**: Use DataHub domains for business domain classification

## Detailed Alternative Approaches

DataHub offers several organizational concepts that can complement or serve as alternatives to platform instances:

### Data Products

**Data Products** group related data assets for business purposes, following data mesh principles:

- **Domain-Oriented**: Owned by specific business teams
- **Cohesive Units**: Related assets (tables, dashboards, pipelines) managed together
- **Business Context**: Focus on business value and consumer needs
- **Cross-Platform**: Can span multiple platform instances

**Example Data Product**:

```
Customer Analytics Data Product
├── Tables from Redshift Cluster 1
├── Tables from Snowflake Analytics
├── Dashboards from Looker
└── Pipelines from Airflow
```

### Additional Metadata Management Approaches

DataHub offers several other ways to handle organizational context without changing URNs:

#### Tags and Labels

- **Purpose**: Add flexible metadata context without changing URNs
- **Use Cases**:
  - Tag datasets with organizational context (domain, product, system)
  - Add environment-specific labels
  - Mark migration status or legacy systems
- **Benefits**: Flexible, searchable, and can be updated without URN changes
- **Example**: Tag a dataset with `domain.voice`, `product.billing`, `system.anomalo`

#### Custom Properties

- **Purpose**: Add structured metadata to entities
- **Use Cases**:
  - Store organizational taxonomy as structured data
  - Add infrastructure-specific metadata
  - Track business context that changes over time
- **Benefits**: Structured data that can be queried and filtered
- **Example**: Add custom property `org_domain: "voice"` that can be updated when domain changes

#### Glossary Terms and Business Context

- **Purpose**: Associate business meaning with technical assets
- **Use Cases**:
  - Link datasets to business concepts
  - Associate platform instances with business domains
  - Create business-friendly groupings
- **Benefits**: Bridges technical and business perspectives
- **Example**: Associate datasets with glossary term "Customer Billing" that can be renamed without affecting URNs

#### Search and Discovery Features

- **Purpose**: Find and organize assets without changing URNs
- **Use Cases**:
  - Search by organizational tags
  - Filter by custom properties
  - Use saved searches for common organizational queries
- **Benefits**: Flexible discovery without structural changes

#### DataHub Actions and Automation

- **Purpose**: Automate metadata management
- **Use Cases**:
  - Auto-tag datasets based on organizational context
  - Automatically assign ownership based on business rules
  - Sync metadata across platform instances
- **Benefits**: Reduces manual effort and ensures consistency

### Comparison of Approaches

| Approach               | URN Impact  | Flexibility | Complexity | Best Use Case                               |
| ---------------------- | ----------- | ----------- | ---------- | ------------------------------------------- |
| **Platform Instances** | Changes URN | Low         | Low        | Technical differentiation needed in URNs    |
| **Data Products**      | No change   | High        | High       | Business-oriented grouping across platforms |
| **Tags/Labels**        | No change   | High        | Low        | Flexible metadata and searchable context    |
| **Custom Properties**  | No change   | Medium      | Medium     | Structured metadata storage                 |
| **Glossary Terms**     | No change   | High        | Medium     | Business context and domain association     |
| **Search Features**    | No change   | High        | Low        | Discovery and organization without changes  |
| **Automation**         | No change   | Medium      | High       | Consistent metadata management              |

### Choosing the Right Approach

- **Platform Instances**: When you need technical differentiation in URNs
- **Data Products**: When you need business-oriented grouping across platforms
- **Tags/Labels**: When you need flexible, searchable metadata
- **Custom Properties**: When you need structured metadata storage
- **Glossary Terms**: When you need business context association
- **Combined Approach**: Use multiple concepts together for comprehensive organization

## Summary

Platform instances and data products each address different aspects of data organization in DataHub. Platform instances modify URNs to include technical identifiers, while data products provide organizational structure without changing the physical identity of the asset. For organizations with evolving taxonomy, the key is to separate technical identifiers (in URNs) from business context (in metadata), ensuring both immutability and flexibility.
