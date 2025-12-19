# DataJob Entity

The DataJob entity represents a unit of work in a data processing pipeline (e.g., an Airflow task, a dbt model, a Spark job). DataJobs belong to DataFlows (pipelines) and can have lineage to datasets and other DataJobs. This guide covers comprehensive DataJob operations in SDK V2.

## Creating a DataJob

### Minimal DataJob

Orchestrator, flowId, and jobId are required:

```java
DataJob dataJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("my_dag")
    .jobId("my_task")
    .build();
```

### With Cluster

Specify cluster (default is "prod"):

```java
DataJob dataJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("analytics_pipeline")
    .cluster("staging")
    .jobId("transform_data")
    .build();
// URN: urn:li:dataJob:(urn:li:dataFlow:(airflow,analytics_pipeline,staging),transform_data)
```

### With Metadata

Add description and name at construction (requires both name AND type):

```java
DataJob dataJob = DataJob.builder()
    .orchestrator("dagster")
    .flowId("customer_etl")
    .cluster("prod")
    .jobId("load_customers")
    .description("Loads customer data from PostgreSQL to Snowflake")
    .name("Load Customers to DWH")
    .type("BATCH")
    .build();
```

### With Custom Properties

Include custom properties in builder (requires name and type when using customProperties):

```java
Map<String, String> props = new HashMap<>();
props.put("schedule", "0 2 * * *");
props.put("retries", "3");
props.put("timeout", "3600");

DataJob dataJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("daily_pipeline")
    .jobId("my_task")
    .name("My Daily Task")
    .type("BATCH")
    .customProperties(props)
    .build();
```

## URN Construction

DataJob URNs follow the pattern:

```
urn:li:dataJob:(urn:li:dataFlow:({orchestrator},{flowId},{cluster}),{jobId})
```

**Automatic URN creation:**

```java
DataJob dataJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("finance_reporting")
    .cluster("prod")
    .jobId("aggregate_transactions")
    .build();

DataJobUrn urn = dataJob.getDataJobUrn();
// urn:li:dataJob:(urn:li:dataFlow:(airflow,finance_reporting,prod),aggregate_transactions)
```

## Description Operations

### Setting Description

```java
dataJob.setDescription("Processes daily customer transactions");
```

### Reading Description

Get description (lazy-loaded from DataJobInfo):

```java
String description = dataJob.getDescription();
```

## Display Name Operations

### Setting Name

```java
dataJob.setName("Process Customer Transactions");
```

### Reading Name

```java
String name = dataJob.getName();
```

## Tags

### Adding Tags

```java
// Simple tag name (auto-prefixed)
dataJob.addTag("critical");
// Creates: urn:li:tag:critical

// Full tag URN
dataJob.addTag("urn:li:tag:etl");
```

### Removing Tags

```java
dataJob.removeTag("critical");
dataJob.removeTag("urn:li:tag:etl");
```

### Tag Chaining

```java
dataJob.addTag("critical")
       .addTag("pii")
       .addTag("production");
```

## Owners

### Adding Owners

```java
import com.linkedin.common.OwnershipType;

// Technical owner
dataJob.addOwner(
    "urn:li:corpuser:data_team",
    OwnershipType.TECHNICAL_OWNER
);

// Data steward
dataJob.addOwner(
    "urn:li:corpuser:compliance",
    OwnershipType.DATA_STEWARD
);

// Business owner
dataJob.addOwner(
    "urn:li:corpuser:product_team",
    OwnershipType.BUSINESS_OWNER
);
```

### Removing Owners

```java
dataJob.removeOwner("urn:li:corpuser:data_team");
```

### Owner Types

Available ownership types:

- `TECHNICAL_OWNER` - Maintains the technical implementation
- `BUSINESS_OWNER` - Business stakeholder
- `DATA_STEWARD` - Manages data quality and compliance
- `DATAOWNER` - Generic data owner
- `DEVELOPER` - Software developer
- `PRODUCER` - Data producer
- `CONSUMER` - Data consumer
- `STAKEHOLDER` - Other stakeholder

## Glossary Terms

### Adding Terms

```java
dataJob.addTerm("urn:li:glossaryTerm:DataProcessing");
dataJob.addTerm("urn:li:glossaryTerm:ETL");
```

### Removing Terms

```java
dataJob.removeTerm("urn:li:glossaryTerm:DataProcessing");
```

### Term Chaining

```java
dataJob.addTerm("urn:li:glossaryTerm:DataProcessing")
       .addTerm("urn:li:glossaryTerm:ETL")
       .addTerm("urn:li:glossaryTerm:FinancialReporting");
```

## Domain

### Setting Domain

```java
dataJob.setDomain("urn:li:domain:Engineering");
```

### Removing Domain

```java
dataJob.removeDomain();
```

## Custom Properties

### Adding Individual Properties

```java
dataJob.addCustomProperty("schedule", "0 2 * * *");
dataJob.addCustomProperty("retries", "3");
dataJob.addCustomProperty("timeout", "3600");
```

### Setting All Properties

Replace all custom properties:

```java
Map<String, String> properties = new HashMap<>();
properties.put("schedule", "0 2 * * *");
properties.put("retries", "3");
properties.put("timeout", "3600");
properties.put("priority", "high");

dataJob.setCustomProperties(properties);
```

### Removing Properties

```java
dataJob.removeCustomProperty("timeout");
```

## Lineage Operations

DataJob lineage defines the relationship between data jobs and the datasets they operate on. Lineage enables impact analysis, data provenance tracking, and understanding data flows through your pipelines.

The DataJob SDK supports four types of lineage:

1. **Dataset-level lineage** - Track which datasets a job reads from and writes to
2. **DataJob dependencies** - Track which jobs depend on other jobs (task dependencies)
3. **Field-level lineage** - Track specific columns consumed and produced
4. **Fine-grained lineage** - Track column-to-column transformations

### Understanding Input and Output Datasets

**Input Datasets** - Datasets that the job reads from:

- Represent source data for the job
- Create upstream lineage: Dataset → DataJob

**Output Datasets** - Datasets that the job writes to:

- Represent destination data from the job
- Create downstream lineage: DataJob → Dataset

### Input Datasets

#### Adding Single Inlet

```java
// Using string URN
dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)");

// Using DatasetUrn object for type safety
DatasetUrn datasetUrn = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)"
);
dataJob.addInputDataset(datasetUrn);
```

#### Adding Multiple Inlets

```java
// Chain multiple calls
dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)")
       .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)")
       .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.purchases,PROD)");
```

#### Setting All Inlets at Once

```java
List<String> inletUrns = Arrays.asList(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.orders,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:kafka,events.clicks,PROD)"
);
dataJob.setInputDatasets(inletUrns);
```

#### Removing Inlets

```java
// Remove single inlet
dataJob.removeInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)");

// Or using DatasetUrn
DatasetUrn datasetUrn = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)"
);
dataJob.removeInputDataset(datasetUrn);
```

#### Reading Inlets

```java
// Get all inlets (lazy-loaded)
List<DatasetUrn> inlets = dataJob.getInputDatasets();
for (DatasetUrn inlet : inlets) {
    System.out.println("Input: " + inlet);
}
```

### Output Datasets (Outlets)

#### Adding Single Outlet

```java
// Using string URN
dataJob.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales_summary,PROD)");

// Using DatasetUrn object
DatasetUrn datasetUrn = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales_summary,PROD)"
);
dataJob.addOutputDataset(datasetUrn);
```

#### Adding Multiple Outlets

```java
dataJob.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_summary,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.monthly_summary,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,reports/summary.parquet,PROD)");
```

#### Setting All Outlets at Once

```java
List<String> outletUrns = Arrays.asList(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_metrics,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.product_metrics,PROD)"
);
dataJob.setOutputDatasets(outletUrns);
```

#### Removing Outlets

```java
// Remove single outlet
dataJob.removeOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales_summary,PROD)");

// Or using DatasetUrn
DatasetUrn datasetUrn = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales_summary,PROD)"
);
dataJob.removeOutputDataset(datasetUrn);
```

#### Reading Outlets

```java
// Get all outlets (lazy-loaded)
List<DatasetUrn> outlets = dataJob.getOutputDatasets();
for (DatasetUrn outlet : outlets) {
    System.out.println("Output: " + outlet);
}
```

### DataJob Dependencies

DataJob dependencies model task-to-task relationships within workflows. This enables DataHub to track which jobs depend on other jobs completing first.

**Use cases:**

- Airflow task dependencies (task A → task B → task C)
- Cross-DAG dependencies (jobs in different pipelines)
- Workflow orchestration visualization

#### Adding Job Dependencies

```java
// Using string URN
dataJob.addInputDataJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,pipeline,prod),upstream_task)");

// Using DataJobUrn object for type safety
DataJobUrn upstreamJob = DataJobUrn.createFromString(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,pipeline,prod),upstream_task)"
);
dataJob.addInputDataJob(upstreamJob);
```

#### Chaining Job Dependencies

```java
// Multiple dependencies (task runs after all complete)
dataJob.addInputDataJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,pipeline,prod),task_1)")
       .addInputDataJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,pipeline,prod),task_2)")
       .addInputDataJob("urn:li:dataJob:(urn:li:dataFlow:(dagster,other_pipeline,prod),external_task)");
```

#### Removing Job Dependencies

```java
// Remove single dependency
dataJob.removeInputDataJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,pipeline,prod),task_1)");

// Or using DataJobUrn
DataJobUrn jobUrn = DataJobUrn.createFromString(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,pipeline,prod),task_1)"
);
dataJob.removeInputDataJob(jobUrn);
```

#### Reading Job Dependencies

```java
// Get all upstream job dependencies (lazy-loaded)
List<DataJobUrn> dependencies = dataJob.getInputDataJobs();
for (DataJobUrn dependency : dependencies) {
    System.out.println("Depends on: " + dependency);
}
```

#### Example: Airflow Task Dependencies

```java
// Model a typical Airflow DAG task chain
DataJob extractTask = DataJob.builder()
    .orchestrator("airflow")
    .flowId("etl_pipeline")
    .jobId("extract_data")
    .build();

DataJob validateTask = DataJob.builder()
    .orchestrator("airflow")
    .flowId("etl_pipeline")
    .jobId("validate_data")
    .build();

// validate_data depends on extract_data
validateTask.addInputDataJob(extractTask.getUrn().toString());

DataJob transformTask = DataJob.builder()
    .orchestrator("airflow")
    .flowId("etl_pipeline")
    .jobId("transform_data")
    .build();

// transform_data depends on validate_data
transformTask.addInputDataJob(validateTask.getUrn().toString());

// Save all tasks
client.entities().upsert(extractTask);
client.entities().upsert(validateTask);
client.entities().upsert(transformTask);

// Result: extract_data → validate_data → transform_data
```

### Field-Level Lineage

Field-level lineage tracks which specific columns (fields) a job consumes and produces. This provides finer granularity than dataset-level lineage.

**Use cases:**

- Track which columns are read/written by transformations
- Understand field-level dependencies
- Validate that jobs only access necessary columns

**Field URN Format:**

```
urn:li:schemaField:(DATASET_URN,COLUMN_NAME)
```

#### Adding Input Fields

```java
// Track which columns the job reads
dataJob.addInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD),order_id)");
dataJob.addInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD),customer_id)");
dataJob.addInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD),total_amount)");
```

#### Adding Output Fields

```java
// Track which columns the job writes
dataJob.addOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),order_id)");
dataJob.addOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),customer_id)");
dataJob.addOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),revenue)");
```

#### Removing Fields

```java
// Remove field lineage
dataJob.removeInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD),order_id)");
dataJob.removeOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),revenue)");
```

#### Reading Fields

```java
// Get all input fields (lazy-loaded)
List<Urn> inputFields = dataJob.getInputFields();
for (Urn field : inputFields) {
    System.out.println("Reads field: " + field);
}

// Get all output fields (lazy-loaded)
List<Urn> outputFields = dataJob.getOutputFields();
for (Urn field : outputFields) {
    System.out.println("Writes field: " + field);
}
```

#### Example: Column-Level Tracking

```java
DataJob aggregateJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("analytics")
    .jobId("aggregate_sales")
    .description("Aggregates sales data by customer")
    .name("Aggregate Sales by Customer")
    .type("BATCH")
    .build();

// Dataset-level lineage
aggregateJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)");
aggregateJob.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_sales,PROD)");

// Field-level lineage - specify exact columns used
aggregateJob.addInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD),customer_id)");
aggregateJob.addInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD),amount)");
aggregateJob.addInputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD),transaction_date)");

aggregateJob.addOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_sales,PROD),customer_id)");
aggregateJob.addOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_sales,PROD),total_sales)");
aggregateJob.addOutputField("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_sales,PROD),transaction_count)");

client.entities().upsert(aggregateJob);
```

### Fine-Grained Lineage

Fine-grained lineage captures column-to-column transformations, showing exactly which input columns produce which output columns and how they're transformed.

**Use cases:**

- Document transformation logic (e.g., "SUM(amount)")
- Track column-level impact analysis
- Understand data derivations
- Compliance and audit trails

#### Adding Fine-Grained Lineage

```java
// Basic transformation (no confidence score)
dataJob.addFineGrainedLineage(
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.orders,PROD),customer_id)",
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),customer_id)",
    "IDENTITY",
    null
);

// Transformation with confidence score (0.0 to 1.0)
dataJob.addFineGrainedLineage(
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.orders,PROD),amount)",
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),revenue)",
    "SUM",
    1.0f  // High confidence
);
```

#### Common Transformation Types

```java
// IDENTITY - direct copy
dataJob.addFineGrainedLineage(upstream, downstream, "IDENTITY", 1.0f);

// Aggregations
dataJob.addFineGrainedLineage(upstream, downstream, "SUM", 1.0f);
dataJob.addFineGrainedLineage(upstream, downstream, "COUNT", 1.0f);
dataJob.addFineGrainedLineage(upstream, downstream, "AVG", 1.0f);
dataJob.addFineGrainedLineage(upstream, downstream, "MAX", 1.0f);
dataJob.addFineGrainedLineage(upstream, downstream, "MIN", 1.0f);

// String operations
dataJob.addFineGrainedLineage(upstream, downstream, "CONCAT", 0.9f);
dataJob.addFineGrainedLineage(upstream, downstream, "UPPER", 1.0f);
dataJob.addFineGrainedLineage(upstream, downstream, "SUBSTRING", 0.95f);

// Date operations
dataJob.addFineGrainedLineage(upstream, downstream, "DATE_TRUNC", 1.0f);
dataJob.addFineGrainedLineage(upstream, downstream, "EXTRACT", 1.0f);

// Custom transformations
dataJob.addFineGrainedLineage(upstream, downstream, "CUSTOM_FUNCTION", 0.8f);
```

#### Removing Fine-Grained Lineage

```java
// Remove specific transformation
dataJob.removeFineGrainedLineage(
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.orders,PROD),amount)",
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD),revenue)",
    "SUM",
    null  // queryUrn (optional)
);
```

#### Reading Fine-Grained Lineage

```java
// Get all fine-grained lineage (lazy-loaded)
List<FineGrainedLineage> lineages = dataJob.getFineGrainedLineages();
for (FineGrainedLineage lineage : lineages) {
    System.out.println("Upstreams: " + lineage.getUpstreams());
    System.out.println("Downstreams: " + lineage.getDownstreams());
    System.out.println("Transformation: " + lineage.getTransformOperation());
    System.out.println("Confidence: " + lineage.getConfidenceScore());
}
```

#### Example: Complex Aggregation

```java
DataJob salesAggregation = DataJob.builder()
    .orchestrator("airflow")
    .flowId("analytics")
    .jobId("daily_sales_summary")
    .name("Daily Sales Summary")
    .type("BATCH")
    .build();

// Dataset-level lineage
salesAggregation.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.transactions,PROD)");
salesAggregation.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_summary,PROD)");

// Fine-grained transformations
String inputDataset = "urn:li:dataset:(urn:li:dataPlatform:postgres,sales.transactions,PROD)";
String outputDataset = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_summary,PROD)";

// Date is copied directly
salesAggregation.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",transaction_date)",
    "urn:li:schemaField:(" + outputDataset + ",date)",
    "IDENTITY",
    1.0f
);

// Revenue is SUM of amounts
salesAggregation.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",amount)",
    "urn:li:schemaField:(" + outputDataset + ",total_revenue)",
    "SUM",
    1.0f
);

// Transaction count
salesAggregation.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",transaction_id)",
    "urn:li:schemaField:(" + outputDataset + ",transaction_count)",
    "COUNT",
    1.0f
);

// Average order value
salesAggregation.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",amount)",
    "urn:li:schemaField:(" + outputDataset + ",avg_order_value)",
    "AVG",
    1.0f
);

client.entities().upsert(salesAggregation);
```

#### Example: Multi-Column Derivation

```java
// Model a transformation where output depends on multiple input columns
DataJob enrichmentJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("enrichment")
    .jobId("enrich_customer_data")
    .build();

String inputDataset = "urn:li:dataset:(urn:li:dataPlatform:postgres,crm.customers,PROD)";
String outputDataset = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers_enriched,PROD)";

// full_name = CONCAT(first_name, ' ', last_name)
// Both first_name and last_name contribute to full_name
enrichmentJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",first_name)",
    "urn:li:schemaField:(" + outputDataset + ",full_name)",
    "CONCAT",
    1.0f
);

enrichmentJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",last_name)",
    "urn:li:schemaField:(" + outputDataset + ",full_name)",
    "CONCAT",
    1.0f
);

// email_domain = SUBSTRING(email, POSITION('@', email) + 1)
enrichmentJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + inputDataset + ",email)",
    "urn:li:schemaField:(" + outputDataset + ",email_domain)",
    "SUBSTRING",
    1.0f
);

client.entities().upsert(enrichmentJob);
```

#### Confidence Scores

Confidence scores (0.0 to 1.0) indicate how certain you are about the transformation:

- **1.0** - Exact, deterministic transformation (e.g., IDENTITY, SUM)
- **0.9-0.99** - High confidence (e.g., simple string operations)
- **0.7-0.89** - Medium confidence (e.g., complex transformations with some uncertainty)
- **0.5-0.69** - Low confidence (e.g., ML-derived lineage, heuristic-based)
- **< 0.5** - Very uncertain (generally not recommended)

```java
// High confidence - exact transformation known
dataJob.addFineGrainedLineage(source, target, "UPPER", 1.0f);

// Medium confidence - inferred from SQL parsing
dataJob.addFineGrainedLineage(source, target, "CASE_WHEN", 0.85f);

// Low confidence - ML-predicted transformation
dataJob.addFineGrainedLineage(source, target, "INFERRED", 0.6f);
```

### Complete Lineage Example

This example demonstrates all four types of lineage working together:

```java
// Create upstream validation job
DataJob validateJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("analytics_pipeline")
    .cluster("prod")
    .jobId("validate_transactions")
    .name("Validate Transaction Data")
    .type("BATCH")
    .build();

validateJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)")
           .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,validated.transactions,PROD)");

client.entities().upsert(validateJob);

// Create main transformation job with comprehensive lineage
DataJob transformJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("analytics_pipeline")
    .cluster("prod")
    .jobId("aggregate_sales")
    .description("Aggregates daily sales data from multiple validated sources")
    .name("Aggregate Daily Sales")
    .type("BATCH")
    .build();

// 1. Dataset-level lineage - Which tables are read/written
transformJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,validated.transactions,PROD)")
            .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)")
            .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_sales,PROD)");

// 2. DataJob dependencies - This job depends on the validation job
transformJob.addInputDataJob(validateJob.getUrn().toString());

// 3. Field-level lineage - Which specific columns are accessed
String transactionsDataset = "urn:li:dataset:(urn:li:dataPlatform:snowflake,validated.transactions,PROD)";
String customersDataset = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)";
String outputDataset = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_sales,PROD)";

// Input fields
transformJob.addInputField("urn:li:schemaField:(" + transactionsDataset + ",transaction_id)")
            .addInputField("urn:li:schemaField:(" + transactionsDataset + ",customer_id)")
            .addInputField("urn:li:schemaField:(" + transactionsDataset + ",amount)")
            .addInputField("urn:li:schemaField:(" + transactionsDataset + ",transaction_date)")
            .addInputField("urn:li:schemaField:(" + customersDataset + ",customer_id)")
            .addInputField("urn:li:schemaField:(" + customersDataset + ",customer_name)");

// Output fields
transformJob.addOutputField("urn:li:schemaField:(" + outputDataset + ",date)")
            .addOutputField("urn:li:schemaField:(" + outputDataset + ",customer_name)")
            .addOutputField("urn:li:schemaField:(" + outputDataset + ",total_revenue)")
            .addOutputField("urn:li:schemaField:(" + outputDataset + ",transaction_count)");

// 4. Fine-grained lineage - Specific column-to-column transformations
// Date column (identity transformation)
transformJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + transactionsDataset + ",transaction_date)",
    "urn:li:schemaField:(" + outputDataset + ",date)",
    "IDENTITY",
    1.0f
);

// Customer name (join + identity)
transformJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + customersDataset + ",customer_name)",
    "urn:li:schemaField:(" + outputDataset + ",customer_name)",
    "IDENTITY",
    1.0f
);

// Total revenue (aggregation)
transformJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + transactionsDataset + ",amount)",
    "urn:li:schemaField:(" + outputDataset + ",total_revenue)",
    "SUM",
    1.0f
);

// Transaction count (aggregation)
transformJob.addFineGrainedLineage(
    "urn:li:schemaField:(" + transactionsDataset + ",transaction_id)",
    "urn:li:schemaField:(" + outputDataset + ",transaction_count)",
    "COUNT",
    1.0f
);

// Add other metadata
transformJob.addTag("critical")
            .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER);

// Save to DataHub
client.entities().upsert(transformJob);

// Result: Creates comprehensive lineage showing:
// - Job dependency: validate_transactions → aggregate_sales
// - Dataset flow: raw.transactions → validated.transactions → analytics.daily_sales
//                 raw.customers → analytics.daily_sales
// - Column-level: transaction_date → date (IDENTITY)
//                 amount → total_revenue (SUM)
//                 transaction_id → transaction_count (COUNT)
//                 customer_name → customer_name (IDENTITY via JOIN)
```

### Lineage Flow Visualization

The comprehensive lineage example above creates this multi-level lineage graph:

```
Job-to-Job Level:
┌────────────────────────┐         ┌──────────────────────┐
│ Validate Transactions  │────────→│  Aggregate Sales Job │
└────────────────────────┘         └──────────────────────┘

Dataset Level:
┌─────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐
│ raw.transactions    │───→│ validated.transactions  │───→│                         │
└─────────────────────┘    └─────────────────────────┘    │  analytics.daily_sales  │
                                                           │                         │
┌─────────────────────┐                                   │                         │
│ raw.customers       │──────────────────────────────────→│                         │
└─────────────────────┘                                   └─────────────────────────┘

Column Level (Fine-Grained):
validated.transactions.transaction_date ──[IDENTITY]──→ daily_sales.date
validated.transactions.amount           ──[SUM]──────→ daily_sales.total_revenue
validated.transactions.transaction_id   ──[COUNT]────→ daily_sales.transaction_count
raw.customers.customer_name             ──[IDENTITY]──→ daily_sales.customer_name
```

### ETL Pipeline Example

Model a complete Extract-Transform-Load pipeline:

```java
// Extract job
DataJob extractJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("etl_pipeline")
    .jobId("extract")
    .build();

extractJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:mysql,prod.orders,PROD)")
          .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_raw,PROD)");

client.entities().upsert(extractJob);

// Transform job
DataJob transformJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("etl_pipeline")
    .jobId("transform")
    .build();

transformJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_raw,PROD)")
            .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_clean,PROD)");

client.entities().upsert(transformJob);

// Load job
DataJob loadJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("etl_pipeline")
    .jobId("load")
    .build();

loadJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_clean,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)");

client.entities().upsert(loadJob);

// Creates end-to-end lineage:
// mysql.orders → [Extract] → s3.raw → [Transform] → s3.clean → [Load] → snowflake.analytics
```

### Updating Lineage

```java
// Load existing job
DataJobUrn urn = DataJobUrn.createFromString(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_pipeline,prod),my_task)"
);
DataJob dataJob = client.entities().get(urn);

// Add new inlet (e.g., requirements changed)
dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.new_source,PROD)");

// Remove old outlet (e.g., deprecated table)
dataJob.removeOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,old.deprecated_table,PROD)");

// Apply changes
client.entities().update(dataJob);
```

### Lineage Best Practices

1. **Be Complete** - Define both inputs and outputs for accurate lineage
2. **Use Correct URNs** - Ensure dataset URNs match existing datasets in DataHub
3. **Update When Changed** - Keep lineage current as pipelines evolve
4. **Document Transformations** - Use descriptions to explain what the job does
5. **Model All Jobs** - Include every step in your pipeline for complete lineage
6. **Use Typed URNs** - Prefer DatasetUrn/DataJobUrn objects over strings for compile-time safety
7. **Layer Your Lineage** - Start with dataset-level, add field-level and fine-grained as needed
8. **Track Dependencies** - Use DataJob dependencies to model task orchestration
9. **Be Precise with Transformations** - Use accurate transformation types in fine-grained lineage
10. **Set Confidence Scores** - Use appropriate confidence scores to indicate lineage quality

### Common Patterns

#### Multiple Sources to Single Destination

```java
// Data aggregation job
dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.orders,PROD)")
       .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.customers,PROD)")
       .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.products,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales_summary,PROD)");
```

#### Single Source to Multiple Destinations

```java
// Data fanout job
dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.raw,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,archive/events,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,events.processed,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:elasticsearch,events.searchable,PROD)");
```

#### Cross-Platform Lineage

```java
// ETL across different platforms
dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:mysql,production.transactions,PROD)")
       .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.user_activity,PROD)")
       .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,raw/reference_data,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_360,PROD)")
       .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:bigquery,reporting.customer_metrics,PROD)");
```

## Complete Example

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.DataJob;
import com.linkedin.common.OwnershipType;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DataJobExample {
    public static void main(String[] args) {
        // Create client
        DataHubClientV2 client = DataHubClientV2.builder()
            .server("http://localhost:8080")
            .build();

        try {
            // Build data job with all metadata
            DataJob dataJob = DataJob.builder()
                .orchestrator("airflow")
                .flowId("customer_analytics")
                .cluster("prod")
                .jobId("process_events")
                .description("Processes customer events from Kafka to warehouse")
                .name("Process Customer Events")
                .type("BATCH")
                .build();

            // Add tags
            dataJob.addTag("critical")
                   .addTag("etl")
                   .addTag("pii");

            // Add owners
            dataJob.addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER)
                   .addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER);

            // Add glossary terms
            dataJob.addTerm("urn:li:glossaryTerm:DataProcessing")
                   .addTerm("urn:li:glossaryTerm:CustomerData");

            // Set domain
            dataJob.setDomain("urn:li:domain:Analytics");

            // Add custom properties
            dataJob.addCustomProperty("schedule", "0 2 * * *")
                   .addCustomProperty("retries", "3")
                   .addCustomProperty("timeout", "7200");

            // Upsert to DataHub
            client.entities().upsert(dataJob);

            System.out.println("Successfully created data job: " + dataJob.getUrn());

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
```

## Updating Existing DataJobs

### Load and Modify

```java
// Load existing data job
DataJobUrn urn = DataJobUrn.createFromString(
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,prod),my_task)"
);
DataJob dataJob = client.entities().get(urn);

// Add new metadata (creates patches)
dataJob.addTag("new-tag")
       .addOwner("urn:li:corpuser:new_owner", OwnershipType.TECHNICAL_OWNER);

// Apply patches
client.entities().update(dataJob);
```

### Incremental Updates

```java
// Just add what you need
dataJob.addTag("critical");
client.entities().update(dataJob);

// Later, add more
dataJob.addCustomProperty("priority", "high");
client.entities().update(dataJob);
```

## Builder Options Reference

| Method                  | Required | Description                                                                                                     |
| ----------------------- | -------- | --------------------------------------------------------------------------------------------------------------- |
| `orchestrator(String)`  | ✅ Yes   | Orchestrator (e.g., "airflow", "dagster")                                                                       |
| `flowId(String)`        | ✅ Yes   | Flow/DAG identifier                                                                                             |
| `jobId(String)`         | ✅ Yes   | Job/task identifier                                                                                             |
| `cluster(String)`       | No       | Cluster name (e.g., "prod", "dev"). Default: "prod"                                                             |
| `description(String)`   | No       | Job description. **Requires both `name()` and `type()` to be set**                                              |
| `name(String)`          | No       | Display name shown in UI. **Required if using `description()`, `type()`, or `customProperties()`**              |
| `type(String)`          | No       | Job type (e.g., "BATCH", "STREAMING"). **Required if using `description()`, `name()`, or `customProperties()`** |
| `customProperties(Map)` | No       | Map of custom key-value properties. **Requires both `name()` and `type()` to be set**                           |

**Important:** The DataJobInfo aspect requires both `name` and `type` fields. If you provide any of `description`, `name`, `type`, or `customProperties` in the builder, you must provide both `name` and `type`. Otherwise, you'll get an `IllegalArgumentException` at build time.

## Common Patterns

### Creating Multiple DataJobs

```java
String[] tasks = {"extract", "transform", "load"};
for (String taskName : tasks) {
    DataJob dataJob = DataJob.builder()
        .orchestrator("airflow")
        .flowId("etl_pipeline")
        .cluster("prod")
        .jobId(taskName)
        .build();

    dataJob.addTag("etl")
           .addCustomProperty("team", "data-engineering");

    client.entities().upsert(dataJob);
}
```

### Batch Metadata Addition

```java
DataJob dataJob = DataJob.builder()
    .orchestrator("airflow")
    .flowId("my_dag")
    .jobId("my_task")
    .build();

List<String> tags = Arrays.asList("critical", "production", "etl");
tags.forEach(dataJob::addTag);

client.entities().upsert(dataJob);  // Emits all tags in one call
```

### Conditional Metadata

```java
if (isCritical(dataJob)) {
    dataJob.addTag("critical")
           .addTerm("urn:li:glossaryTerm:BusinessCritical");
}

if (processesFinancialData(dataJob)) {
    dataJob.addTag("financial")
           .addOwner("urn:li:corpuser:compliance_team", OwnershipType.DATA_STEWARD);
}
```

## DataJob vs DataFlow

**DataFlow** represents a pipeline or DAG (e.g., an Airflow DAG):

- URN: `urn:li:dataFlow:(orchestrator,flowId,cluster)`
- Contains multiple DataJobs

**DataJob** represents a task within a pipeline:

- URN: `urn:li:dataJob:(flowUrn,jobId)`
- Belongs to one DataFlow
- Can have lineage to datasets and other DataJobs

Example hierarchy:

```
DataFlow: urn:li:dataFlow:(airflow,customer_pipeline,prod)
├── DataJob: urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_pipeline,prod),extract)
├── DataJob: urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_pipeline,prod),transform)
└── DataJob: urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_pipeline,prod),load)
```

## Orchestrator Examples

Common orchestrator values:

- `airflow` - Apache Airflow
- `dagster` - Dagster
- `prefect` - Prefect
- `dbt` - dbt (data build tool)
- `spark` - Apache Spark
- `glue` - AWS Glue
- `dataflow` - Google Cloud Dataflow
- `azkaban` - Azkaban
- `luigi` - Luigi

## Next Steps

- **[Dataset Entity](./dataset-entity.md)** - Working with dataset entities
- **[Patch Operations](./patch-operations.md)** - Deep dive into patches
- **[Migration Guide](./migration-from-v1.md)** - Upgrading from V1

## Examples

### Basic DataJob Creation

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DataJobCreateExample.java show_path_as_comment }}
```

### Comprehensive DataJob with Metadata and Lineage

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DataJobFullExample.java show_path_as_comment }}
```

### DataJob Lineage Operations

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DataJobLineageExample.java show_path_as_comment }}
```
