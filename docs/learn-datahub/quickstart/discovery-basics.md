import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import NextStepButton from '@site/src/components/NextStepButton';
import DataHubEntityCard, { SampleEntities } from '@site/src/components/DataHubEntityCard';
import DataHubLineageNode, { DataHubLineageFlow } from '@site/src/components/DataHubLineageNode';
import { SearchExercise } from '@site/src/components/TutorialExercise';
import TutorialProgress from '@site/src/components/TutorialProgress';

# Step 3: Discovery Basics (10 minutes)

<TutorialProgress
tutorialId="quickstart"
currentStep={2}
steps={[
{ title: "Setup DataHub", time: "5 min", description: "Deploy DataHub locally with Docker" },
{ title: "Ingest Your First Dataset", time: "15 min", description: "Connect sources and ingest metadata" },
{ title: "Discovery Basics", time: "10 min", description: "Find, evaluate, and understand datasets" },
{ title: "Explore Data Lineage", time: "15 min", description: "Trace dependencies and assess impact" }
]}
/>

**Discovery Implementation**: With enterprise metadata now available in DataHub, you need to demonstrate systematic data discovery capabilities. This step focuses on enabling analysts to efficiently locate and understand relevant datasets.

**Business Requirement**: Locate user engagement metrics to support executive reporting and strategic decision-making. The data exists within the analytics infrastructure but requires systematic discovery.

**Your Objective**: Implement and demonstrate DataHub's discovery features to enable self-service data access across the organization.

## What You'll Master

By the end of this step, you'll be able to:

- **Find specific datasets** using strategic search techniques
- **Navigate enterprise data architecture** across multiple platforms
- **Understand data relationships** through schema exploration
- **Identify relevant data** for business requirements

## Enterprise Data Discovery Framework

This tutorial demonstrates systematic data discovery techniques used in professional data environments. These methods apply to any enterprise data catalog and are essential for effective data analysis.

### Professional Data Discovery Approach

**Requirements Analysis** → **Strategic Search** → **Asset Evaluation** → **Context Gathering** → **Access Planning**

| Step                      | Focus              | Key Actions                                    |
| ------------------------- | ------------------ | ---------------------------------------------- |
| **Requirements Analysis** | Define objectives  | Understand business questions and data needs   |
| **Strategic Search**      | Target discovery   | Use business terms and domain knowledge        |
| **Asset Evaluation**      | Quality assessment | Review schemas, documentation, and freshness   |
| **Context Gathering**     | Understand usage   | Check lineage, owners, and related assets      |
| **Access Planning**       | Prepare for use    | Verify permissions and connection requirements |

## Method 1: Strategic Search - Finding User Metrics

**Your First Lead**: The business requirement focuses on "user" metrics. Let's start there and see what data is available.

### Strategic Search: User-Related Datasets

1. **Open DataHub** at [http://localhost:9002](http://localhost:9002)

2. **Search for user data**:

   ```
   Search: "user"
   ```

3. **Analyze your results** - you should discover these datasets:

<DataHubEntityCard {...SampleEntities.userCreatedTable} />
<DataHubEntityCard {...SampleEntities.userDeletedTable} />
<DataHubEntityCard {...SampleEntities.kafkaUserEvents} />
<DataHubEntityCard {...SampleEntities.rawUserData} />

**Search Results Analysis**: This search successfully identified both datasets required for user metrics analysis, demonstrating the effectiveness of targeted search strategies in enterprise data discovery.

:::tip Real-World Search Strategy
Notice how searching for "user" found tables with "users" in the name? DataHub's search is smart - it finds variations and related terms automatically. This is exactly how you'd search in production systems.
:::

### Advanced Search Techniques

<Tabs>
<TabItem value="business-terms" label="Business-Focused Search">

<SearchExercise
title="Business-Focused Search Patterns"
difficulty="beginner"
timeEstimate="3 min"
searches={[
{
query: "fct_users",
description: "Find analytics tables (fact tables)",
expected: "Examples: fct_users_created, fct_users_deleted"
},
{
query: "logging events",
description: "Find event data",
expected: "Example: logging_events"
},
{
query: "kafka sample",
description: "Look for sample Kafka topics",
expected: "Example: SampleKafkaDataset (if available)"
},
{
query: "warehouse analytics",
description: "Find processed analytics data",
expected: "Warehouse analytics tables and views"
}
]}

>

**Why this works**: This enterprise follows standard naming conventions (`fct_` for fact tables, descriptive names for events).

</SearchExercise>

</TabItem>
<TabItem value="platform-filters" label="Platform-Based Filtering">

**Filter by enterprise platforms:**

- **Hive**: Click to see only warehouse tables (`fct_users_created`, `fct_users_deleted`, `logging_events`)
- **Kafka**: Real-time streaming data (`SampleKafkaDataset`)
- **HDFS**: Data lake storage (`SampleHdfsDataset`)

**Pro Tip**: For user analytics, focus on the **Hive** platform first for processed data!

</TabItem>
<TabItem value="advanced-operators" label="Power User Techniques">

**Advanced search operators:**

```
# Find all fact tables
name:fct_*

# Find user-related data
user OR users

# Exclude test data
user NOT test NOT sample
```

**Learn More**: Check out the [complete search documentation](../../how/search.md) for all available operators and techniques.

</TabItem>
</Tabs>

## Method 2: Browse by Organization

Sometimes browsing is more effective than searching, especially when exploring unfamiliar data.

### Browse by Platform

1. **Click "Browse" in the top navigation**

2. **Explore by data platform:**

   - **Demo Data**: Sample retail datasets
   - **PostgreSQL**: Operational databases
   - **Snowflake**: Data warehouse tables
   - **dbt**: Transformed analytics models

3. **Drill down into a platform:**
   - Click on "Demo Data"
   - You'll see all datasets from that platform
   - Notice the hierarchical organization

### Browse by Domain (if configured)

If your organization uses domains:

1. **Look for domain groupings** like:

   - Marketing Analytics
   - Customer Operations
   - Financial Reporting
   - Product Analytics

2. **Each domain contains** related datasets regardless of platform

## Method 3: Explore Dataset Details

Let's dive deep into a specific dataset to understand what information DataHub provides.

### Find the Customer Dataset

1. **Search for "customer"** or browse to find a customer-related table

2. **Click on a dataset** (e.g., "customers" or "user_profiles")

3. **Explore the dataset page** - you'll see several tabs:

### Schema Tab - Understanding Your Data

The Schema tab shows the structure of your dataset:

**Column Information:**

- **Name**: The column identifier
- **Type**: Data type (string, integer, timestamp, etc.)
- **Description**: Business meaning (if available)
- **Nullable**: Whether the field can be empty

**Key things to look for:**

```
Primary keys (usually ID fields)
Foreign keys (relationships to other tables)
Date fields (for time-based analysis)
Categorical fields (for grouping/segmentation)
Numeric fields (for calculations/metrics)
```

### Properties Tab - Metadata & Context

**Dataset Properties:**

- **Owner**: Who's responsible for this data
- **Created**: When the dataset was first created
- **Last Modified**: When data was last updated
- **Tags**: Classification labels
- **Custom Properties**: Business-specific metadata

**Platform Details:**

- **Database/Schema**: Where the data lives
- **Table Type**: Table, view, or materialized view
- **Row Count**: Approximate number of records

### Documentation Tab - Business Context

Look for:

- **Dataset description**: What this data represents
- **Column descriptions**: Business meaning of each field
- **Usage notes**: How this data should be used
- **Data quality notes**: Known issues or limitations

## Understanding Data Relationships

### Related Datasets

At the bottom of any dataset page, look for:

**"Frequently Co-occurring"**: Datasets often used together
**"Similar Datasets"**: Tables with similar schemas
**"Related by Lineage"**: Connected through data pipelines

### Column-Level Relationships

In the Schema tab:

- **Foreign key indicators** show relationships to other tables
- **Similar columns** across datasets are highlighted
- **Column lineage** shows data transformation history

## Practical Exercise: Customer Analysis Scenario

Let's complete the original task - finding customer segmentation data:

### Step 1: Search Strategy

```
1. Search for "customer segment"
2. Filter results to "Datasets" only
3. Look for tables with names like:
   - customer_segments
   - user_cohorts
   - customer_analytics
```

### Step 2: Evaluate Options

For each potential dataset, check:

- **Schema**: Does it have the fields you need?
- **Freshness**: Is the data recent enough?
- **Owner**: Can you contact them with questions?
- **Documentation**: Is the business logic clear?

### Step 3: Understand the Data

Click into the most promising dataset and review:

- **Column definitions**: What does each field mean?
- **Sample data**: What do actual values look like?
- **Lineage**: Where does this data come from?

## Discovery Best Practices

### For Data Consumers

1. **Start broad, then narrow**: Begin with keyword searches, then use filters
2. **Check multiple sources**: The same business concept might exist in different systems
3. **Read the documentation**: Don't assume column meanings from names alone
4. **Contact owners**: When in doubt, reach out to dataset owners
5. **Bookmark frequently used datasets**: Save time on repeat searches

### For Data Producers

1. **Add clear descriptions**: Help others understand your data
2. **Tag appropriately**: Use consistent classification schemes
3. **Document business logic**: Explain calculations and transformations
4. **Keep metadata current**: Update descriptions when data changes

## Understanding Data Relationships

Now that you've discovered the key datasets, let's see how they connect in the data pipeline:

### User Metrics Data Pipeline

<DataHubLineageFlow {...{
title: "User Analytics Discovery Flow",
nodes: [
{
name: 'logging_events',
type: 'Table',
entityType: 'Dataset',
platform: 'Kafka',
health: 'Good',
columns: [
{ name: 'user_id', type: 'string' },
{ name: 'event_type', type: 'string' },
{ name: 'timestamp', type: 'timestamp' },
{ name: 'session_id', type: 'string' }
],
tags: ['Raw-Data', 'Real-time'],
glossaryTerms: ['User Events', 'Raw Data']
},
{
name: 'user_analytics',
type: 'Job',
entityType: 'DataJob',
platform: 'Spark',
health: 'Good',
tags: ['ETL', 'Processing']
},
{
name: 'fct_users_created',
type: 'Table',
entityType: 'Dataset',
platform: 'Hive',
health: 'Good',
columns: [
{ name: 'user_id', type: 'string' },
{ name: 'created_date', type: 'date' },
{ name: 'signup_source', type: 'string' },
{ name: 'user_tier', type: 'string' }
],
tags: ['Analytics', 'Business-Ready'],
glossaryTerms: ['User Creation', 'Analytics Table']
}
]
}} />

**Data Flow Analysis**:

- **Source**: `logging_events` captures real-time user interactions
- **Processing**: `user_analytics` job transforms raw events into structured metrics
- **Output**: `fct_users_created` and `fct_users_deleted` provide business-ready analytics

This lineage view shows you the complete data journey - from raw user events through processing to the final analytics tables. Understanding these relationships is crucial for data quality and impact analysis.

## Common Discovery Patterns

<Tabs>
<TabItem value="exploratory" label="Exploratory Analysis">

**Scenario**: "I need to understand what customer data we have"

**Approach**:

1. Search broadly: "customer"
2. Browse by platform to see all sources
3. Compare schemas across datasets
4. Identify the most comprehensive source

</TabItem>
<TabItem value="specific-need" label="Specific Data Need">

**Scenario**: "I need customer email addresses for a campaign"

**Approach**:

1. Search specifically: "email"
2. Filter to datasets only
3. Check column details for email fields
4. Verify data freshness and quality

</TabItem>
<TabItem value="impact-analysis" label="Understanding Dependencies">

**Scenario**: "What would break if I change this table?"

**Approach**:

1. Navigate to the dataset
2. Check the Lineage tab
3. Identify downstream consumers
4. Contact owners of dependent systems

</TabItem>
</Tabs>

## Success Checkpoint

**You've successfully completed Step 3 when you can:**

- Find datasets using both search and browse methods
- Understand what information is available in dataset pages
- Read and interpret schema information
- Identify dataset relationships and dependencies

**What you've learned:**

- Multiple ways to discover data in DataHub
- How to evaluate datasets for your analysis needs
- Where to find business context and documentation
- How to understand data relationships

<NextStepButton
to="first-lineage.md"
tutorialId="quickstart"
currentStep={2}

> Next: Explore Data Lineage
> </NextStepButton>
