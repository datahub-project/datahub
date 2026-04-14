# DataFlow Entity

The `DataFlow` entity represents a data processing pipeline or workflow in DataHub. It models orchestrated workflows from tools like Apache Airflow, Apache Spark, dbt, Apache Flink, and other data orchestration platforms.

## Overview

A DataFlow is a logical grouping of data processing tasks that work together to achieve a data transformation or movement goal. DataFlows are typically:

- **Scheduled workflows** (Airflow DAGs)
- **Batch processing jobs** (Spark applications)
- **Transformation projects** (dbt projects)
- **Streaming pipelines** (Flink jobs)

DataFlows serve as parent containers for `DataJob` entities, representing the overall pipeline while individual jobs represent specific tasks within that pipeline.

## URN Structure

DataFlow URNs follow this format:

```
urn:li:dataFlow:(orchestrator,flowId,cluster)
```

**Components:**

- **orchestrator**: The platform or tool running the workflow (e.g., "airflow", "spark", "dbt", "flink")
- **flowId**: The unique identifier for the flow within the orchestrator (e.g., DAG ID, job name, project name)
- **cluster**: The cluster or environment where the flow runs (e.g., "prod", "prod-us-west-2", "emr-cluster")

**Examples:**

```
urn:li:dataFlow:(airflow,customer_etl_daily,prod)
urn:li:dataFlow:(spark,ml_feature_generation,emr-prod-cluster)
urn:li:dataFlow:(dbt,marketing_analytics,prod)
```

## Creating a DataFlow

### Basic Creation

```java
DataFlow dataflow = DataFlow.builder()
    .orchestrator("airflow")
    .flowId("my_dag_id")
    .cluster("prod")
    .displayName("My ETL Pipeline")
    .description("Daily ETL pipeline for customer data")
    .build();

client.entities().upsert(dataflow);
```

### With Custom Properties

```java
Map<String, String> properties = new HashMap<>();
properties.put("schedule", "0 2 * * *");
properties.put("team", "data-engineering");
properties.put("sla_hours", "4");

DataFlow dataflow = DataFlow.builder()
    .orchestrator("airflow")
    .flowId("customer_pipeline")
    .cluster("prod")
    .customProperties(properties)
    .build();
```

## Properties

### Core Properties

| Property       | Type   | Description                          | Example                         |
| -------------- | ------ | ------------------------------------ | ------------------------------- |
| `orchestrator` | String | Platform running the flow (required) | "airflow", "spark", "dbt"       |
| `flowId`       | String | Unique flow identifier (required)    | "my_dag_id", "my_job_name"      |
| `cluster`      | String | Cluster/environment (required)       | "prod", "dev", "prod-us-west-2" |
| `displayName`  | String | Human-readable name                  | "Customer ETL Pipeline"         |
| `description`  | String | Flow description                     | "Processes customer data daily" |

### Additional Properties

| Property           | Type                | Description                            |
| ------------------ | ------------------- | -------------------------------------- |
| `externalUrl`      | String              | Link to flow in orchestration tool     |
| `project`          | String              | Associated project or namespace        |
| `customProperties` | Map<String, String> | Key-value metadata                     |
| `created`          | Long                | Creation timestamp (milliseconds)      |
| `lastModified`     | Long                | Last modified timestamp (milliseconds) |

## Operations

### Ownership

```java
// Add owners
dataflow.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
dataflow.addOwner("urn:li:corpuser:analytics_team", OwnershipType.BUSINESS_OWNER);

// Remove owner
dataflow.removeOwner("urn:li:corpuser:johndoe");
```

### Tags

```java
// Add tags (with or without "urn:li:tag:" prefix)
dataflow.addTag("etl");
dataflow.addTag("production");
dataflow.addTag("urn:li:tag:pii");

// Remove tag
dataflow.removeTag("etl");
```

### Glossary Terms

```java
// Add terms
dataflow.addTerm("urn:li:glossaryTerm:ETL");
dataflow.addTerm("urn:li:glossaryTerm:DataPipeline");

// Remove term
dataflow.removeTerm("urn:li:glossaryTerm:ETL");
```

### Domain

```java
// Set domain
dataflow.setDomain("urn:li:domain:DataEngineering");

// Remove specific domain
dataflow.removeDomain("urn:li:domain:DataEngineering");

// Or clear all domains
dataflow.clearDomains();
```

### Custom Properties

```java
// Add individual properties
dataflow.addCustomProperty("schedule", "0 2 * * *");
dataflow.addCustomProperty("team", "data-engineering");

// Remove property
dataflow.removeCustomProperty("schedule");

// Set all properties (replaces existing)
Map<String, String> props = new HashMap<>();
props.put("key1", "value1");
props.put("key2", "value2");
dataflow.setCustomProperties(props);
```

### Description and Display Name

```java
// Set description
dataflow.setDescription("Daily ETL pipeline for customer data");

// Set display name
dataflow.setDisplayName("Customer ETL Pipeline");

// Get description
String description = dataflow.getDescription();

// Get display name
String displayName = dataflow.getDisplayName();
```

### Timestamps and URLs

```java
// Set external URL
dataflow.setExternalUrl("https://airflow.example.com/dags/my_dag");

// Set project
dataflow.setProject("customer_analytics");

// Set timestamps
dataflow.setCreated(System.currentTimeMillis() - 86400000L); // 1 day ago
dataflow.setLastModified(System.currentTimeMillis());
```

## Orchestrator-Specific Examples

### Apache Airflow

```java
DataFlow airflowFlow = DataFlow.builder()
    .orchestrator("airflow")
    .flowId("customer_etl_daily")
    .cluster("prod")
    .displayName("Customer ETL Pipeline")
    .description("Daily pipeline processing customer data from MySQL to Snowflake")
    .build();

airflowFlow
    .addTag("etl")
    .addTag("production")
    .addCustomProperty("schedule", "0 2 * * *")
    .addCustomProperty("catchup", "false")
    .addCustomProperty("max_active_runs", "1")
    .setExternalUrl("https://airflow.company.com/dags/customer_etl_daily");
```

### Apache Spark

```java
DataFlow sparkFlow = DataFlow.builder()
    .orchestrator("spark")
    .flowId("ml_feature_generation")
    .cluster("emr-prod-cluster")
    .displayName("ML Feature Generation Job")
    .description("Large-scale Spark job generating ML features")
    .build();

sparkFlow
    .addTag("spark")
    .addTag("machine-learning")
    .addCustomProperty("spark.executor.memory", "8g")
    .addCustomProperty("spark.driver.memory", "4g")
    .addCustomProperty("spark.executor.cores", "4")
    .setDomain("urn:li:domain:MachineLearning");
```

### dbt

```java
DataFlow dbtFlow = DataFlow.builder()
    .orchestrator("dbt")
    .flowId("marketing_analytics")
    .cluster("prod")
    .displayName("Marketing Analytics Models")
    .description("dbt transformations for marketing data")
    .build();

dbtFlow
    .addTag("dbt")
    .addTag("transformation")
    .addCustomProperty("dbt_version", "1.5.0")
    .addCustomProperty("target", "production")
    .addCustomProperty("models_count", "87")
    .setProject("marketing")
    .setExternalUrl("https://github.com/company/dbt-marketing");
```

### Apache Flink (Streaming)

```java
DataFlow flinkFlow = DataFlow.builder()
    .orchestrator("flink")
    .flowId("real_time_fraud_detection")
    .cluster("prod-flink-cluster")
    .displayName("Real-time Fraud Detection")
    .description("Real-time streaming pipeline for fraud detection")
    .build();

flinkFlow
    .addTag("streaming")
    .addTag("real-time")
    .addTag("fraud-detection")
    .addCustomProperty("parallelism", "16")
    .addCustomProperty("checkpoint_interval", "60000")
    .setDomain("urn:li:domain:Security");
```

## Fluent API

All mutation methods return `this` to support method chaining:

```java
DataFlow dataflow = DataFlow.builder()
    .orchestrator("airflow")
    .flowId("sales_pipeline")
    .cluster("prod")
    .build();

dataflow
    .addTag("etl")
    .addTag("production")
    .addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER)
    .addOwner("urn:li:corpuser:owner2", OwnershipType.BUSINESS_OWNER)
    .addTerm("urn:li:glossaryTerm:Sales")
    .setDomain("urn:li:domain:Sales")
    .setDescription("Sales data pipeline")
    .addCustomProperty("schedule", "0 2 * * *")
    .addCustomProperty("team", "sales-analytics");

client.entities().upsert(dataflow);
```

## Relationship with DataJob

DataFlows are parent entities to DataJobs. A DataJob represents a specific task or step within a DataFlow:

```java
// Create the parent DataFlow
DataFlow dataflow = DataFlow.builder()
    .orchestrator("airflow")
    .flowId("customer_etl")
    .cluster("prod")
    .build();

client.entities().upsert(dataflow);

// Create child DataJobs that reference the parent flow
DataJob extractJob = DataJob.builder()
    .flow(dataflow.getUrn())  // References parent DataFlow
    .jobId("extract_customers")
    .build();

DataJob transformJob = DataJob.builder()
    .flow(dataflow.getUrn())
    .jobId("transform_customers")
    .build();

client.entities().upsert(extractJob);
client.entities().upsert(transformJob);
```

This hierarchy allows you to:

- Model the overall pipeline (DataFlow)
- Model individual tasks within the pipeline (DataJob)
- Track task-level lineage and dependencies
- Organize governance metadata at both levels

## Best Practices

1. **Use consistent naming**: Keep orchestrator names consistent across your organization (e.g., always use "airflow", not "Airflow" or "AIRFLOW")

2. **Choose appropriate clusters**: Use meaningful cluster names that indicate environment and region (e.g., "prod-us-west-2", "staging-eu-central-1")

3. **Add scheduling information**: Include schedule expressions in custom properties for batch workflows

4. **Link to source systems**: Always set `externalUrl` to link back to the orchestration tool's UI

5. **Set ownership early**: Assign technical and business owners when creating flows

6. **Use tags for categorization**: Tag flows by type (etl, streaming, ml), environment (production, staging), and criticality

7. **Document SLAs**: Use custom properties to document SLA requirements and alert channels

8. **Track versions**: For versioned workflows (like dbt), include version information in custom properties

## Complete Example

```java
// Initialize client
DataHubClientConfigV2 config = DataHubClientConfigV2.builder()
    .server("http://localhost:8080")
    .token(System.getenv("DATAHUB_TOKEN"))
    .build();

try (DataHubClientV2 client = new DataHubClientV2(config)) {

    // Create comprehensive DataFlow
    Map<String, String> customProps = new HashMap<>();
    customProps.put("schedule", "0 2 * * *");
    customProps.put("catchup", "false");
    customProps.put("team", "data-engineering");
    customProps.put("sla_hours", "4");
    customProps.put("alert_channel", "#data-alerts");

    DataFlow dataflow = DataFlow.builder()
        .orchestrator("airflow")
        .flowId("production_etl_pipeline")
        .cluster("prod-us-east-1")
        .displayName("Production ETL Pipeline")
        .description("Main ETL pipeline for customer data processing")
        .customProperties(customProps)
        .build();

    dataflow
        .addTag("etl")
        .addTag("production")
        .addTag("pii")
        .addOwner("urn:li:corpuser:data_eng_team", OwnershipType.TECHNICAL_OWNER)
        .addOwner("urn:li:corpuser:product_owner", OwnershipType.BUSINESS_OWNER)
        .addTerm("urn:li:glossaryTerm:ETL")
        .addTerm("urn:li:glossaryTerm:CustomerData")
        .setDomain("urn:li:domain:DataEngineering")
        .setProject("customer_analytics")
        .setExternalUrl("https://airflow.company.com/dags/production_etl_pipeline")
        .setCreated(System.currentTimeMillis() - 86400000L * 30)
        .setLastModified(System.currentTimeMillis());

    // Upsert to DataHub
    client.entities().upsert(dataflow);

    System.out.println("Created DataFlow: " + dataflow.getUrn());
}
```

## See Also

- [DataJob Entity](datajob-entity.md) - Child tasks within a DataFlow
- [Dataset Entity](dataset-entity.md) - Data sources and targets for DataFlows
