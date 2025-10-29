import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TutorialProgress from '@site/src/components/TutorialProgress';
import NextStepButton from '@site/src/components/NextStepButton';
import DataHubEntityCard, { SampleEntities } from '@site/src/components/DataHubEntityCard';
import OSDetectionTabs from '@site/src/components/OSDetectionTabs';

# Step 2: First Data Ingestion (10 minutes)

<TutorialProgress
tutorialId="quickstart"
currentStep={1}
steps={[
{ title: "Setup DataHub", time: "5 min" },
{ title: "First Data Ingestion", time: "10 min" },
{ title: "Discovery Basics", time: "10 min" },
{ title: "Your First Lineage", time: "5 min" }
]}
/>

**The Implementation Challenge**: You have an empty DataHub instance that needs to be populated with enterprise metadata. Before analysts can discover and use data effectively, you must establish connections to the organization's data systems.

**Your Objective**: Connect multiple data platforms to DataHub and ingest comprehensive metadata that enables self-service data discovery across the organization.

## What You'll Accomplish

By the end of this step, you'll have:

- **Enterprise analytics data** from multiple systems ingested into DataHub
- **Multi-platform connectivity** established (Kafka streams, Hive warehouse, HDFS storage)
- **Comprehensive metadata** including schemas, lineage, and business context
- **Self-service foundation** enabling analysts to discover and understand data independently

## Understanding Data Ingestion

DataHub ingestion connects to your data systems and extracts comprehensive metadata through a standardized process:

### Metadata Ingestion Workflow

**1. Connection** → **2. Discovery** → **3. Extraction** → **4. Transformation** → **5. Loading**

| Phase              | Description          | What Happens                                                          |
| ------------------ | -------------------- | --------------------------------------------------------------------- |
| **Connection**     | Secure system access | DataHub establishes authenticated connections to source systems       |
| **Discovery**      | Schema scanning      | Identifies databases, tables, views, and data structures              |
| **Extraction**     | Metadata collection  | Pulls schema definitions, statistics, and lineage information         |
| **Transformation** | Standardization      | Converts metadata into DataHub's unified format                       |
| **Loading**        | Storage & indexing   | Stores metadata in DataHub's knowledge graph for search and discovery |

**What gets ingested:**

- **Schema information**: Table and column definitions
- **Data statistics**: Row counts, data types, sample values
- **Lineage**: How data flows between systems
- **Usage patterns**: Query history and access patterns (when available)

## Connecting Enterprise Data Systems

**The Situation**: This tutorial uses a representative enterprise data architecture with data scattered across multiple systems - just like most real companies. Let's get it all connected to DataHub.

**What You're About to Ingest**: This enterprise data architecture includes:

<div className="techflow-data-architecture">

<OSDetectionTabs>
<TabItem value="windows" label="Windows">

```cmd
# Connect sample data ecosystem to DataHub
datahub docker ingest-sample-data

# If datahub command not found:
python -m datahub docker ingest-sample-data
```

</TabItem>
<TabItem value="macos" label="macOS">

```bash
# Connect sample data ecosystem to DataHub
datahub docker ingest-sample-data

# If datahub command not found:
python3 -m datahub docker ingest-sample-data
```

</TabItem>
<TabItem value="linux" label="Linux">

```bash
# Connect sample data ecosystem to DataHub
datahub docker ingest-sample-data

# If datahub command not found:
python3 -m datahub docker ingest-sample-data

# If permission issues:
sudo datahub docker ingest-sample-data
```

</TabItem>
</OSDetectionTabs>

**Enterprise Data Landscape:**

<div className="data-systems-overview">

| System               | Platform | What's Inside                                     | Business Purpose                      |
| -------------------- | -------- | ------------------------------------------------- | ------------------------------------- |
| **Real-time Events** | Kafka    | `SampleKafkaDataset` - Live user activity streams | Track user behavior as it happens     |
| **Data Warehouse**   | Hive     | `fct_users_created`, `fct_users_deleted`          | Monthly user metrics for analytics    |
| **Event Logs**       | Hive     | `logging_events` - Detailed activity logs         | Source data for user analytics        |
| **Data Lake**        | HDFS     | `SampleHdfsDataset` - Raw data storage            | Historical data backup and processing |

</div>

</div>

**Your Mission**: This ingestion will give you access to the complete enterprise data ecosystem. Pay special attention to the `fct_users_created` and `fct_users_deleted` tables - these contain the user metrics data.

:::tip Real-World Context
This mirrors what you'd find at most tech companies: streaming data (Kafka), processed analytics (Hive), and data lake storage (HDFS). You're learning with realistic, production-like data architecture!
:::

**Watch the Magic Happen**: As the ingestion runs, you'll see DataHub discovering these key datasets:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard {...SampleEntities.userCreatedTable} />
  <DataHubEntityCard {...SampleEntities.kafkaUserEvents} />
</div>

DataHub automatically extracts:

- **Table schemas** with column definitions and data types
- **Data lineage** showing how tables connect across platforms
- **Ownership information** (John Doe owns most of this sample data)
- **Documentation** and business context

**What happens during ingestion:**

```
Starting ingestion...
Extracting metadata from demo source...
Found 12 datasets
Found 156 columns
Found 8 lineage relationships
Found 3 dashboards
Found 2 data pipelines
Ingestion completed successfully!
```

## Option 2: Connect a Real Database (Advanced)

If you want to connect your own database, here's how to create an ingestion recipe:

<Tabs>
<TabItem value="postgres" label="PostgreSQL">

Create a file called `postgres-recipe.yml`:

```yaml
source:
  type: postgres
  config:
    host_port: localhost:5432
    database: retail_db
    username: postgres
    password: password
    # Optional: specific schemas to ingest
    schema_pattern:
      allow: ["public", "analytics"]

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

Run the ingestion:

```bash
datahub ingest -c postgres-recipe.yml
```

</TabItem>
<TabItem value="mysql" label="MySQL">

Create a file called `mysql-recipe.yml`:

```yaml
source:
  type: mysql
  config:
    host_port: localhost:3306
    database: retail_db
    username: root
    password: password

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

Run the ingestion:

```bash
datahub ingest -c mysql-recipe.yml
```

</TabItem>
<TabItem value="csv" label="CSV Files">

For CSV files in a directory:

```yaml
source:
  type: csv-enricher
  config:
    # Path to your CSV files
    filename: "/path/to/csv/files/*.csv"

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

Run the ingestion:

```bash
datahub ingest -c csv-recipe.yml
```

</TabItem>
</Tabs>

## Mission Status: Did We Connect Enterprise Data?

**The Moment of Truth**: Let's see if you successfully connected the enterprise data systems to DataHub.

### 1. Check Your Ingestion Results

Look for these success indicators in your terminal:

```
Ingestion completed successfully
Processed 5 datasets (SampleKafkaDataset, fct_users_created, fct_users_deleted, logging_events, SampleHdfsDataset)
Processed 15+ columns across all tables
Discovered lineage relationships between tables
No errors encountered
```

**Success Indicator**: If you see "Ingestion completed successfully", you have successfully connected a multi-platform data architecture to DataHub.

### 2. Explore Enterprise Data in DataHub

1. **Refresh DataHub** at [http://localhost:9002](http://localhost:9002)

2. **Check the home page transformation**:

   - Dataset count jumped from 0 to 5+ datasets
   - Recent activity shows "SampleKafkaDataset", "fct_users_created", etc.
   - You can see the enterprise data platforms: Kafka, Hive, HDFS

3. **Quick victory lap** - click "Browse" in the top navigation:
   - **Hive platform**: You should see `fct_users_created` and `fct_users_deleted` (the user metrics datasets)
   - **Kafka platform**: Real-time streaming data (`SampleKafkaDataset`)
   - **HDFS platform**: Data lake storage (`SampleHdfsDataset`)

**Pro Tip**: Notice how DataHub automatically organized everything by platform? This is how you'll navigate complex data ecosystems in real companies.

**Your Ingested Enterprise Data Assets:**

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="SampleKafkaDataset"
    type="Topic"
    platform="Kafka"
    description="Real-time user behavior events from web and mobile applications"
    owners={[
      { name: 'Data Engineering', type: 'Technical Owner' },
      { name: 'Analytics Team', type: 'Business Owner' }
    ]}
    tags={['Real-time', 'Events', 'User-Behavior']}
    glossaryTerms={['Event Stream', 'User Analytics']}
    health="Good"
  />
  
  <DataHubEntityCard 
    name="fct_users_created"
    type="Table"
    platform="Hive"
    description="Daily aggregated user creation metrics for business reporting"
    owners={[
      { name: 'Analytics Team', type: 'Business Owner' }
    ]}
    tags={['Analytics', 'Daily-Batch', 'User-Metrics']}
    glossaryTerms={['User Metrics', 'Fact Table']}
    health="Good"
  />
  
  <DataHubEntityCard 
    name="SampleHdfsDataset"
    type="Dataset"
    platform="HDFS"
    description="Raw data lake storage for unprocessed enterprise data"
    owners={[
      { name: 'Data Engineering', type: 'Technical Owner' }
    ]}
    tags={['Raw-Data', 'Data-Lake', 'Storage']}
    glossaryTerms={['Data Lake', 'Raw Storage']}
    health="Good"
  />
</div>

### 3. Your First Dataset Deep-Dive: Exploring User Metrics Data

**Time to investigate!** Let's look at the user metrics data. Click on `fct_users_created` (you'll find it under the Hive platform).

**What You'll Discover**:

**Schema Tab** - The data structure:

- `user_id`: The key field for tracking individual users
- `created_date`: When each user was created (perfect for monthly analysis!)
- You'll see this is a proper fact table with clean, analytics-ready data

**Properties Tab** - Business context:

- **Owner**: John Doe (jdoe@linkedin.com) - now you know who to contact with questions!
- **Platform**: Hive (enterprise data warehouse)
- **Custom Properties**: You might see metadata like `prop1: fakeprop` - this is where business teams add context

**Lineage Tab** - The data story:

- **Upstream**: This table is built from `logging_events` (the raw event data)
- **Downstream**: You'll see connections to other analytics tables
- This shows you the complete data pipeline from raw events to business metrics

**Mission Progress**: You've just found the user metrics data! The `fct_users_created` table has user creation data with timestamps - perfect for monthly analysis.

:::tip Real-World Learning
This exploration pattern is exactly what you'd do at any company: find the table, understand its structure, identify the owner, and trace its lineage. You're learning production data analysis skills!
:::

**Want to Learn More?** Check out the [full dataset documentation](/docs/generated/metamodel/entities/dataset.md) to understand all the metadata DataHub captures.

## Understanding the Ingestion Process

Let's break down what just happened:

### 1. Connection & Discovery

```
DataHub Connector → Data Source
├── Authenticates using provided credentials
├── Discovers available schemas/databases
└── Lists all tables and views
```

### 2. Metadata Extraction

```
For each table/view:
├── Extract schema (columns, types, constraints)
├── Collect statistics (row counts, data distribution)
├── Identify relationships (foreign keys, joins)
└── Gather usage information (if available)
```

### 3. Lineage Detection

```
DataHub analyzes:
├── SQL queries in views and stored procedures
├── ETL pipeline definitions
├── Data transformation logic
└── Cross-system data flows
```

### 4. Storage & Indexing

```
Metadata is stored in:
├── MySQL (primary metadata storage)
├── OpenSearch (search index)
└── Kafka (real-time event stream)
```

## Ingestion Best Practices

**For production environments:**

1. **Start small**: Begin with a few important datasets
2. **Use scheduling**: Set up regular ingestion to keep metadata fresh
3. **Monitor performance**: Large databases may need configuration tuning
4. **Secure credentials**: Use environment variables or secret management
5. **Test first**: Always test ingestion recipes in development

## Troubleshooting Common Issues

<Tabs>
<TabItem value="connection-failed" label="Connection Failed">

**Error:** `Failed to connect to database`

**Common causes:**

- Incorrect host/port
- Wrong credentials
- Database not accessible from Docker container
- Firewall blocking connection

**Solutions:**

```bash
# Test connection manually
telnet your-db-host 5432

# For local databases, use host.docker.internal instead of localhost
host_port: host.docker.internal:5432
```

</TabItem>
<TabItem value="no-data" label="No Data Ingested">

**Error:** `Ingestion completed but no datasets found`

**Common causes:**

- Schema/database doesn't exist
- User lacks permissions
- Pattern filters too restrictive

**Solutions:**

```yaml
# Check permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;

# Broaden patterns
schema_pattern:
  allow: [".*"]  # Allow all schemas
```

</TabItem>
<TabItem value="slow-ingestion" label="Slow Ingestion">

**Issue:** Ingestion taking very long

**Solutions:**

```yaml
# Disable profiling for large tables
profiling:
  enabled: false

# Limit table discovery
table_pattern:
  allow: ["important_table_.*"]
```

</TabItem>
</Tabs>

## Implementation Checkpoint: Verify Success

**You've successfully completed the metadata ingestion when:**

- **Enterprise data is live**: 5+ datasets visible in DataHub (Kafka, Hive, HDFS platforms)
- **Analytics tables discovered**: You can see `fct_users_created` and `fct_users_deleted` in the Hive platform
- **Data exploration complete**: You've clicked into a dataset and seen schema, properties, and lineage
- **Owner identified**: You know John Doe owns the user analytics data

**Implementation Success**: You've successfully connected a multi-platform data architecture to DataHub, establishing comprehensive metadata visibility across the organization's data ecosystem.

**What you've accomplished:**

- **Enterprise integration**: Connected Kafka streams, Hive warehouse, and HDFS storage systems
- **Automated metadata discovery**: Extracted schemas, lineage, and ownership information
- **Business enablement**: Created the foundation for self-service data discovery
- **Production-ready skills**: Implemented the same processes used in enterprise environments

**Next Phase**: With metadata ingestion complete, you can now enable systematic data discovery and analysis across the organization.

<NextStepButton
to="discovery-basics.md"
tutorialId="quickstart"
currentStep={1}

> Next: Discover and Explore Your Data
> </NextStepButton>
