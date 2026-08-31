# Application

The application entity represents software applications, services, or systems that produce or consume data assets in your data ecosystem. Applications serve as a way to organize and group related data assets by the software that creates, transforms, or uses them, enabling better understanding of data provenance and usage patterns.

## Identity

Applications are identified by a single piece of information:

- The unique identifier (`id`) for the application: this is a string that uniquely identifies the application within your DataHub instance. It can be any meaningful identifier such as an application name, service name, or system identifier. Common patterns include using kebab-case names (e.g., `customer-analytics-service`), fully qualified service names (e.g., `com.company.analytics.customer-service`), or simple descriptive names (e.g., `tableau-reports`).

An example of an application identifier is `urn:li:application:customer-analytics-service`.

Unlike datasets which require platform and environment qualifiers, applications use a simpler URN structure with just the application identifier, making them flexible for representing any type of software application across your organization.

## Important Capabilities

### Application Properties

The core metadata about an application is stored in the `applicationProperties` aspect. This aspect contains:

- **Name**: A human-readable display name for the application (e.g., "Customer Analytics Service", "Data Pipeline Orchestrator")
- **Description**: Documentation describing what the application does, its purpose, and how it's used
- **Custom Properties**: Key-value pairs for storing additional application-specific metadata
- **External References**: Links to external documentation, source code repositories, dashboards, or other relevant resources

The following code snippet shows you how to create an application with basic properties.

<details>
<summary>Python SDK: Create an application</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_create.py show_path_as_comment }}
```

</details>

### Updating Application Properties

You can update the properties of an existing application by emitting a new `applicationProperties` aspect. The SDK will merge the new properties with existing ones.

<details>
<summary>Python SDK: Update application properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_update_properties.py show_path_as_comment }}
```

</details>

### Tags and Glossary Terms

Applications can have Tags or Terms attached to them, just like other entities. Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms so you understand when you should use which.

#### Adding Tags to an Application

Tags are added to applications using the `globalTags` aspect. Tags are useful for categorizing applications by technology stack, team ownership, criticality level, or other organizational dimensions.

<details>
<summary>Python SDK: Add a tag to an application</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_add_tag.py show_path_as_comment }}
```

</details>

#### Adding Glossary Terms to an Application

Glossary terms are added using the `glossaryTerms` aspect. Terms are useful for associating applications with business concepts, data domains, or standardized terminology.

<details>
<summary>Python SDK: Add a term to an application</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_add_term.py show_path_as_comment }}
```

</details>

### Ownership

Ownership is associated to an application using the `ownership` aspect. Owners can be of a few different types, `TECHNICAL_OWNER`, `BUSINESS_OWNER`, `DATA_STEWARD`, etc. See [OwnershipType.pdl](https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-models/src/main/pegasus/com/linkedin/common/OwnershipType.pdl) for the full list of ownership types and their meanings.

The following script shows you how to add an owner to an application using the low-level Python SDK.

<details>
<summary>Python SDK: Add an owner to an application</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_add_owner.py show_path_as_comment }}
```

</details>

### Domains

Applications can be associated with domains to organize them by business area, department, or data domain. Domains are set using the `domains` aspect.

<details>
<summary>Python SDK: Add a domain to an application</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_add_domain.py show_path_as_comment }}
```

</details>

### Application Assets (Membership)

One of the most powerful features of applications is the ability to associate them with data assets. This creates a relationship between the application and the datasets, dashboards, charts, or other entities that the application produces or consumes.

The `applications` aspect is attached to the data assets (not the application entity itself) to indicate which applications are associated with those assets. This bidirectional relationship enables:

- Finding all data assets associated with a specific application
- Discovering which applications produce or consume a particular dataset
- Tracking data lineage through application boundaries
- Understanding the full scope of an application's data footprint

<details>
<summary>Python SDK: Associate assets with an application</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_add_assets.py show_path_as_comment }}
```

</details>

### Querying Applications

Applications can be queried using the standard DataHub REST API to retrieve all aspects and metadata.

<details>
<summary>Fetch application entity via REST API</summary>

```python
{{ inline /metadata-ingestion/examples/library/application_query_rest_api.py show_path_as_comment }}
```

Or using curl:

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3Aapplication%3Acustomer-analytics-service'
```

</details>

You can also query for all assets associated with an application using the search API:

<details>
<summary>Find all assets associated with an application</summary>

```bash
# Using the search API to find entities with a specific application
curl -X POST 'http://localhost:8080/entities?action=search' \
  -H 'Content-Type: application/json' \
  -d '{
    "input": "*",
    "entity": "dataset",
    "start": 0,
    "count": 10,
    "filter": {
      "or": [
        {
          "and": [
            {
              "field": "applications",
              "value": "urn:li:application:customer-analytics-service",
              "condition": "EQUAL"
            }
          ]
        }
      ]
    }
  }'
```

</details>

## Integration Points

### Relationships with Other Entities

Applications can be associated with multiple types of entities in DataHub:

1. **Datasets**: The most common use case is associating applications with the datasets they produce or consume. This helps track data provenance and understand which applications are upstream or downstream of specific datasets.

2. **Dashboards and Charts**: Applications that generate reports or visualizations can be linked to dashboard and chart entities, making it clear which application produces which analytics artifacts.

3. **Data Jobs and Data Flows**: ETL pipelines and data processing applications can be associated with the dataJob and dataFlow entities they orchestrate or execute.

4. **ML Models and ML Features**: Machine learning applications can be linked to the models and features they train or use for inference.

5. **Domains**: Applications can belong to business domains, helping organize them by organizational structure or business function.

6. **Ownership**: Applications have owners (technical owners, business owners) who are responsible for maintaining and operating them.

### Common Usage Patterns

#### Pattern 1: Service-to-Dataset Mapping

The most common pattern is mapping microservices or applications to the datasets they produce:

```
Customer Service Application → produces → customer_events dataset
                             → produces → customer_profiles dataset
                             → consumes → user_authentication dataset
```

This pattern is typically implemented during ingestion by:

- Adding the `applications` aspect to each dataset
- Referencing the application URN in the aspect

#### Pattern 2: Reporting Application Organization

Business intelligence and reporting tools can organize their dashboards and charts by application:

```
Tableau Application → contains → Sales Dashboard
                    → contains → Marketing Dashboard
Power BI Application → contains → Finance Dashboard
```

#### Pattern 3: Data Pipeline Grouping

Data pipeline orchestration systems can group related jobs under an application:

```
Airflow Application → orchestrates → daily_etl_pipeline
                    → orchestrates → hourly_refresh_pipeline
                    → orchestrates → weekly_aggregation_pipeline
```

### GraphQL API

Applications are fully integrated with DataHub's GraphQL API, enabling:

- Creating applications via `createApplication` mutation
- Updating application properties
- Deleting applications via `deleteApplication` mutation
- Batch setting application assets via `batchSetApplication` mutation
- Querying applications and their relationships

The GraphQL resolvers for applications are located in `datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/resolvers/application/`.

## Notable Exceptions

### Application vs Data Product

Applications and Data Products are related but distinct concepts:

- **Applications** represent software systems that produce or consume data. They are technical entities representing code and infrastructure.
- **Data Products** represent curated data assets packaged for consumption. They are business entities representing data as a product.

An application might produce multiple data products, and a data product might be produced by multiple applications working together.

### Application vs Platform

Applications should not be confused with Data Platforms:

- **Data Platform** (e.g., Snowflake, BigQuery, Kafka) represents the technology or system that hosts data assets.
- **Application** represents the specific software application that creates or uses those data assets.

For example:

- Platform: `snowflake`
- Dataset: `urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)`
- Application: `urn:li:application:customer-analytics-service` (which writes to that Snowflake table)

### UUID-based Identifiers

While the application ID can be any string, using UUIDs is supported but not required. If you don't provide an ID when creating an application through the ApplicationService, a UUID will be automatically generated. However, meaningful, human-readable identifiers are generally preferred for better discoverability and maintenance.

### Future Enhancements

The application entity is relatively new and may be expanded in the future to include:

- Service mesh integration for automatic discovery
- API specifications and endpoint documentation
- Deployment and version tracking
- Resource consumption metrics
- Dependency graph visualization
