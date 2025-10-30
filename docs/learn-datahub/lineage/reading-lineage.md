import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TutorialProgress from '@site/src/components/TutorialProgress';
import DataHubEntityCard, { SampleEntities } from '@site/src/components/DataHubEntityCard';
import DataHubLineageNode, { DataHubLineageFlow, SampleLineageFlows } from '@site/src/components/DataHubLineageNode';
import ProcessFlow, { DataHubWorkflows } from '@site/src/components/ProcessFlow';

# Reading Lineage Graphs (15 minutes)

<TutorialProgress
tutorialId="lineage"
currentStep={0}
steps={[
{ title: "Reading Lineage Graphs", time: "15 min", description: "Navigate complex lineage graphs like a data flow expert" },
{ title: "Performing Impact Analysis", time: "15 min", description: "Systematically assess downstream effects before making changes" },
{ title: "Lineage Troubleshooting", time: "10 min", description: "Debug missing connections and improve lineage accuracy" }
]}
/>

**The Expert's Challenge**: You've mastered basic lineage in the quickstart, but now you're facing a complex production scenario. The customer dashboard is showing inconsistent numbers, and you need to trace through a multi-hop data pipeline spanning 5 different systems to find the root cause.

**Your Mission**: Learn to read complex lineage graphs like a seasoned data engineer, understanding every connection, transformation, and dependency in your data ecosystem.

## What You'll Master

By the end of this step, you'll be able to:

- **Navigate multi-hop lineage** across complex data architectures
- **Interpret different node types** (datasets, jobs, applications)
- **Understand transformation logic** through lineage connections
- **Identify critical paths** in your data infrastructure

## The Lineage Reading Framework

Professional data engineers follow a systematic approach to lineage analysis:

<ProcessFlow
title="5-Hop Lineage Analysis Method"
steps={[
{
title: "Start at Target",
description: "Begin with dataset of interest",
details: ["Open lineage view", "Identify current dataset", "Note business context"]
},
{
title: "Trace Upstream",
description: "Follow data backwards",
details: ["Identify transformations", "Check data sources", "Document dependencies"]
},
{
title: "Analyze Hops",
description: "Examine each connection",
details: ["Understand business logic", "Check quality gates", "Note critical points"]
},
{
title: "Impact Assessment",
description: "Evaluate change effects",
details: ["Identify affected systems", "Assess risk levels", "Plan mitigation"]
},
{
title: "Validate Understanding",
description: "Confirm analysis",
details: ["Review with data owners", "Test assumptions", "Document findings"]
}
]}
type="horizontal"
animated={true}
/>

## Level 1: Understanding Node Types

Every element in a lineage graph tells a specific story:

<Tabs>
<TabItem value="datasets" label="Data Assets">

**Tables, Views, and Files**:

- **Raw Tables**: Source system data (often rectangular nodes)
- **Analytical Views**: Processed, business-ready data
- **Materialized Views**: Pre-computed results for performance
- **File Assets**: CSV, Parquet, JSON files in data lakes

**Visual Cues in DataHub**:

<div style={{display: 'flex', gap: '12px', flexWrap: 'wrap', margin: '16px 0'}}>
  <DataHubLineageNode name="customer_data" type="Table" platform="Snowflake" health="Good" />
  <DataHubLineageNode name="events_raw" type="Table" platform="BigQuery" health="Good" />
  <DataHubLineageNode name="user_metrics" type="Table" platform="Hive" health="Warning" />
</div>

- **Platform logos**: Each node shows the actual platform logo and type
- **Health indicators**: Color-coded dots show data quality status
- **Node highlighting**: Selected or problematic nodes are visually emphasized

**Reading Strategy**: Start with the dataset causing issues, then trace backward to find the source.

</TabItem>
<TabItem value="transformations" label="Processing Jobs">

**Data Processing Elements**:

- **ETL Jobs**: Extract, Transform, Load processes
- **Python Scripts**: Custom data processing logic
- **dbt Models**: Data transformation workflows
- **Spark Jobs**: Large-scale data processing

**Connection Patterns**:

- **Solid lines**: Direct data dependencies
- **Dashed lines**: Indirect or inferred relationships
- **Arrows**: Direction of data flow (always follows the arrows!)

**Analysis Technique**: Jobs between datasets show _how_ data is transformed, not just _that_ it flows.

</TabItem>
<TabItem value="applications" label="Consuming Systems">

**Business Applications**:

- **BI Dashboards**: Looker, Tableau, PowerBI reports
- **ML Models**: Training and inference pipelines
- **Applications**: Customer-facing features
- **Automated Reports**: Scheduled business reports

**Business Impact Indicators**:

- **User-facing systems**: High business impact if broken
- **Internal tools**: Important for operations but lower external impact
- **Experimental systems**: Can often tolerate temporary issues

</TabItem>
</Tabs>

## Level 2: Multi-Hop Navigation

Real production lineage often spans multiple systems and transformations:

### The 6-Hop Analysis Method

**Scenario**: Customer dashboard shows wrong revenue numbers. Let's trace it:

<DataHubLineageFlow {...{
title: "6-Hop Revenue Data Investigation",
nodes: [
{
name: 'orders_raw',
type: 'Table',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'order_id', type: 'string' },
{ name: 'customer_id', type: 'string' },
{ name: 'order_date', type: 'date' },
{ name: 'total_amount', type: 'number' },
{ name: 'status', type: 'string' }
],
tags: ['Source', 'Transactional'],
glossaryTerms: ['Order Data']
},
{
name: 'etl_processor',
type: 'Job',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Critical',
tags: ['ETL', 'Critical Path']
},
{
name: 'daily_sales',
type: 'Table',
entityType: 'Dataset',
platform: 'Hive',
health: 'Warning',
columns: [
{ name: 'sale_date', type: 'date' },
{ name: 'product_id', type: 'string' },
{ name: 'revenue', type: 'number' },
{ name: 'quantity', type: 'number' },
{ name: 'discount', type: 'number' }
],
tags: ['Aggregated', 'Daily'],
glossaryTerms: ['Sales Data', 'Revenue']
},
{
name: 'revenue_agg',
type: 'View',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Warning',
columns: [
{ name: 'period', type: 'date' },
{ name: 'total_revenue', type: 'number' },
{ name: 'avg_order_value', type: 'number' },
{ name: 'order_count', type: 'number' }
],
tags: ['Mart', 'Executive'],
glossaryTerms: ['Revenue Metrics']
},
{
name: 'revenue_chart',
type: 'Chart',
entityType: 'Dataset',
platform: 'Tableau',
health: 'Warning',
columns: [
{ name: 'period', type: 'date' },
{ name: 'total_revenue', type: 'number' },
{ name: 'growth_rate', type: 'number' }
],
tags: ['Visualization', 'Executive'],
glossaryTerms: ['Revenue Chart']
},
{
name: 'executive_dashboard',
type: 'Dashboard',
entityType: 'Dataset',
platform: 'Tableau',
health: 'Critical',
tags: ['Executive', 'Critical'],
glossaryTerms: ['Executive Dashboard']
}
]
}} />

**Navigation Strategy**:

1. **Start at the problem** (executive dashboard)
2. **Follow arrows backward** (upstream direction)
3. **Document each hop**: What system, what transformation?
   - Dashboard ← Chart ← View ← Table ← Job ← Raw Table
4. **Identify the break point**: Where does data look wrong?
   - Critical ETL job failure affecting downstream data
5. **Focus investigation**: Drill into the problematic hop
   - Expand columns to see field-level transformations
   - Check tags and glossary terms for context

### Interactive Exercise: Multi-Hop Tracing

<div className="interactive-exercise">

**Your Challenge**: Find the root cause of data quality issues

**Step 1**: Open any complex dataset in your DataHub instance
**Step 2**: Click "View Lineage" to see the full graph
**Step 3**: Apply the 5-hop analysis method:

**5-Hop Lineage Analysis Example:**

<DataHubLineageFlow {...{
title: "6-Hop User Analytics Investigation",
nodes: [
{
name: 'user_events_raw',
type: 'Table',
entityType: 'Dataset',
platform: 'HDFS',
health: 'Good',
columns: [
{ name: 'event_id', type: 'string' },
{ name: 'user_id', type: 'string' },
{ name: 'event_timestamp', type: 'timestamp' },
{ name: 'event_type', type: 'string' },
{ name: 'session_id', type: 'string' }
],
tags: ['Source', 'Raw'],
glossaryTerms: ['Event Data']
},
{
name: 'kafka_ingestion',
type: 'Job',
entityType: 'DataJob',
platform: 'Kafka',
health: 'Good',
tags: ['Streaming', 'Ingestion']
},
{
name: 'events_validated',
type: 'Table',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'event_id', type: 'string' },
{ name: 'user_id', type: 'string' },
{ name: 'event_date', type: 'date' },
{ name: 'event_type', type: 'string' },
{ name: 'is_valid', type: 'boolean' }
],
tags: ['Validated', 'Cleaned'],
glossaryTerms: ['Clean Event Data']
},
{
name: 'dbt_transform',
type: 'Job',
entityType: 'DataJob',
platform: 'dbt',
health: 'Warning',
tags: ['Transformation', 'Business Logic']
},
{
name: 'user_activity_daily',
type: 'Table',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Warning',
columns: [
{ name: 'user_id', type: 'string' },
{ name: 'activity_date', type: 'date' },
{ name: 'event_count', type: 'number' },
{ name: 'session_count', type: 'number' },
{ name: 'active_minutes', type: 'number' }
],
tags: ['Aggregated', 'Daily'],
glossaryTerms: ['User Activity', 'Engagement']
},
{
name: 'rollup_aggregation',
type: 'Job',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Warning',
tags: ['Aggregation', 'Rollup']
},
{
name: 'fct_users_created',
type: 'Table',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Critical',
columns: [
{ name: 'user_id', type: 'string' },
{ name: 'first_seen_date', type: 'date' },
{ name: 'total_events', type: 'number' },
{ name: 'total_sessions', type: 'number' },
{ name: 'lifetime_minutes', type: 'number' },
{ name: 'user_status', type: 'string' }
],
tags: ['Fact Table', 'Analytics'],
glossaryTerms: ['User Metrics', 'Analytics']
}
]
}} />

**Analysis Questions for Each Hop:**

1. **Hop 1**: What was the last transformation applied?
2. **Hop 2**: What business logic was implemented?
3. **Hop 3**: What quality checks were performed?
4. **Hop 4**: How was the data originally ingested?
5. **Hop 5**: What is the ultimate source system?

**Professional Lineage Reading Strategy:**

1. **Start at the Target**: Begin with the dataset you're investigating
2. **Work Backwards**: Follow each upstream connection systematically
3. **Document Each Hop**: Note the transformation type and business purpose
4. **Identify Critical Points**: Mark systems that could cause widespread impact
5. **Validate Understanding**: Confirm your analysis with data owners when possible

**Analysis Questions**:

- Which hop has the most complex transformation?
- Where would you focus if data was missing?
- Which systems are most critical to this pipeline?

</div>

## Level 3: Understanding Transformation Logic

The connections between nodes reveal how data is processed:

### Reading Connection Types

<Tabs>
<TabItem value="direct-transforms" label="Direct Transformations">

#### One-to-One Relationships

<DataHubLineageFlow {...{
title: "Direct Data Transformation Pattern",
nodes: [
{
name: 'raw_customer_data',
type: 'Raw Source',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'first_name', type: 'string' },
{ name: 'last_name', type: 'string' },
{ name: 'email', type: 'string' },
{ name: 'created_at', type: 'timestamp' }
],
tags: ['Raw-Data', 'Source', 'Operational'],
glossaryTerms: ['Customer Data', 'Source System']
},
{
name: 'customer_analytics_table',
type: 'Analytics View',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'full_name', type: 'string' },
{ name: 'email_domain', type: 'string' },
{ name: 'days_since_signup', type: 'integer' },
{ name: 'customer_tier', type: 'string' }
],
tags: ['Analytics', 'Processed', 'Business-Ready'],
glossaryTerms: ['Customer Analytics', 'Business Intelligence']
}
]
}} />

**What this means**: Direct processing with filtering, aggregation, or enrichment. The transformation is straightforward and predictable.

#### Many-to-One Relationships

<DataHubLineageFlow {...{
title: "Data Consolidation Pattern",
nodes: [
{
name: 'orders',
type: 'Orders Data',
entityType: 'Dataset',
platform: 'MySQL',
health: 'Good',
columns: [
{ name: 'order_id', type: 'bigint' },
{ name: 'customer_id', type: 'bigint' },
{ name: 'order_total', type: 'decimal' },
{ name: 'order_date', type: 'timestamp' }
],
tags: ['Transactional', 'Orders', 'High-Volume'],
glossaryTerms: ['Order Data', 'Transaction Records']
},
{
name: 'customers',
type: 'Customer Data',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'customer_name', type: 'string' },
{ name: 'customer_segment', type: 'string' }
],
tags: ['Master-Data', 'Customers', 'Reference'],
glossaryTerms: ['Customer Master', 'Reference Data']
},
{
name: 'products',
type: 'Product Catalog',
entityType: 'Dataset',
platform: 'MongoDB',
health: 'Good',
columns: [
{ name: 'product_id', type: 'string' },
{ name: 'product_name', type: 'string' },
{ name: 'category', type: 'string' },
{ name: 'price', type: 'decimal' }
],
tags: ['Catalog', 'Products', 'Reference'],
glossaryTerms: ['Product Catalog', 'Inventory Data']
},
{
name: 'sales_analytics',
type: 'Consolidated Analytics',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'customer_name', type: 'string' },
{ name: 'total_revenue', type: 'decimal' },
{ name: 'order_count', type: 'integer' },
{ name: 'avg_order_value', type: 'decimal' },
{ name: 'favorite_category', type: 'string' }
],
tags: ['Analytics', 'Consolidated', 'Business-KPIs'],
glossaryTerms: ['Sales Analytics', 'Customer Metrics']
}
]
}} />

**What this means**: Data joining and consolidation from multiple sources. Complex business logic combines different data domains.

**Analysis Approach**: Look for SQL logic, dbt models, or ETL job definitions to understand the exact transformation rules and join conditions.

</TabItem>
<TabItem value="complex-flows" label="Complex Data Flows">

#### Fan-Out Patterns

<DataHubLineageFlow {...{
title: "Fan-Out: One Source, Multiple Uses",
nodes: [
{
name: 'raw_events',
type: 'Event Stream',
entityType: 'Dataset',
platform: 'Kafka',
health: 'Good',
columns: [
{ name: 'event_id', type: 'string' },
{ name: 'user_id', type: 'bigint' },
{ name: 'event_type', type: 'string' },
{ name: 'timestamp', type: 'timestamp' },
{ name: 'properties', type: 'json' }
],
tags: ['Streaming', 'High-Volume', 'Real-Time'],
glossaryTerms: ['Event Data', 'User Behavior']
},
{
name: 'processing_job',
type: 'Event Processor',
entityType: 'DataJob',
platform: 'Spark',
health: 'Good',
tags: ['Stream-Processing', 'Real-Time', 'Scalable']
},
{
name: 'user_analytics',
type: 'User Behavior',
entityType: 'Dataset',
platform: 'BigQuery',
health: 'Good',
columns: [
{ name: 'user_id', type: 'bigint' },
{ name: 'session_count', type: 'integer' },
{ name: 'page_views', type: 'integer' },
{ name: 'engagement_score', type: 'float' }
],
tags: ['Analytics', 'User-Behavior', 'Product'],
glossaryTerms: ['User Analytics', 'Engagement Metrics']
},
{
name: 'marketing_metrics',
type: 'Campaign Data',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'campaign_id', type: 'string' },
{ name: 'conversion_rate', type: 'float' },
{ name: 'click_through_rate', type: 'float' }
],
tags: ['Marketing', 'Campaigns', 'Performance'],
glossaryTerms: ['Marketing Metrics', 'Campaign Performance']
},
{
name: 'product_insights',
type: 'Feature Usage',
entityType: 'Dataset',
platform: 'Redshift',
health: 'Good',
columns: [
{ name: 'feature_name', type: 'string' },
{ name: 'usage_count', type: 'integer' },
{ name: 'user_adoption', type: 'float' }
],
tags: ['Product', 'Features', 'Usage-Analytics'],
glossaryTerms: ['Product Analytics', 'Feature Adoption']
}
]
}} />

**Business Meaning**: One source feeding multiple business use cases. Each downstream system serves different teams and purposes.

#### Fan-In Patterns

<DataHubLineageFlow {...{
title: "Fan-In: Multiple Sources, Single Destination",
nodes: [
{
name: 'crm_data',
type: 'CRM System',
entityType: 'Dataset',
platform: 'Salesforce',
health: 'Good',
columns: [
{ name: 'account_id', type: 'string' },
{ name: 'company_name', type: 'string' },
{ name: 'industry', type: 'string' }
],
tags: ['CRM', 'Sales', 'External'],
glossaryTerms: ['Customer Relationship', 'Sales Data']
},
{
name: 'support_tickets',
type: 'Support System',
entityType: 'Dataset',
platform: 'Zendesk',
health: 'Good',
columns: [
{ name: 'ticket_id', type: 'string' },
{ name: 'customer_id', type: 'bigint' },
{ name: 'priority', type: 'string' },
{ name: 'status', type: 'string' }
],
tags: ['Support', 'Customer-Service', 'External'],
glossaryTerms: ['Support Data', 'Customer Issues']
},
{
name: 'billing_system',
type: 'Financial Data',
entityType: 'Dataset',
platform: 'Stripe',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'subscription_id', type: 'string' },
{ name: 'mrr', type: 'decimal' },
{ name: 'payment_status', type: 'string' }
],
tags: ['Financial', 'Billing', 'Revenue'],
glossaryTerms: ['Billing Data', 'Revenue Metrics']
},
{
name: 'etl_job',
type: 'Data Consolidator',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Good',
tags: ['ETL', 'Consolidation', 'Scheduled']
},
{
name: 'customer_360',
type: 'Unified Customer View',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'company_name', type: 'string' },
{ name: 'health_score', type: 'float' },
{ name: 'support_tickets_count', type: 'integer' },
{ name: 'monthly_revenue', type: 'decimal' },
{ name: 'churn_risk', type: 'string' }
],
tags: ['Customer-360', 'Unified-View', 'Business-Critical'],
glossaryTerms: ['Customer 360', 'Unified Customer Data']
}
]
}} />

**Business Meaning**: Data consolidation from various systems into a single, comprehensive view.

**Risk Assessment**:

- **Fan-out** = High impact if source breaks (affects multiple downstream systems)
- **Fan-in** = Complex debugging if output is wrong (multiple potential failure points)

</TabItem>
<TabItem value="temporal-flows" label="Time-Based Processing">

#### Batch vs Real-Time Processing Patterns

<DataHubLineageFlow {...{
title: "Processing Schedule Comparison",
nodes: [
{
name: 'daily_sales_raw',
type: 'Daily Batch Source',
entityType: 'Dataset',
platform: 'S3',
health: 'Good',
columns: [
{ name: 'transaction_id', type: 'string' },
{ name: 'amount', type: 'decimal' },
{ name: 'date', type: 'date' }
],
tags: ['Batch', 'Daily', 'Historical'],
glossaryTerms: ['Batch Processing', 'Daily Sales']
},
{
name: 'daily_etl_job',
type: 'Scheduled Pipeline',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Good',
tags: ['Scheduled', 'Daily-6AM', 'Batch-Processing']
},
{
name: 'sales_dashboard',
type: 'Business Dashboard',
entityType: 'Dashboard',
platform: 'Tableau',
health: 'Good',
tags: ['Dashboard', 'Daily-Refresh', 'Executive'],
glossaryTerms: ['Sales Reporting', 'Business Intelligence']
},
{
name: 'real_time_events',
type: 'Live Event Stream',
entityType: 'Dataset',
platform: 'Kafka',
health: 'Good',
columns: [
{ name: 'event_id', type: 'string' },
{ name: 'user_id', type: 'bigint' },
{ name: 'action', type: 'string' },
{ name: 'timestamp', type: 'timestamp' }
],
tags: ['Streaming', 'Real-Time', 'High-Frequency'],
glossaryTerms: ['Real-Time Events', 'Live Data']
},
{
name: 'stream_processor',
type: 'Real-Time Pipeline',
entityType: 'DataJob',
platform: 'Kafka-Streams',
health: 'Good',
tags: ['Stream-Processing', 'Continuous', 'Low-Latency']
},
{
name: 'live_metrics',
type: 'Real-Time Metrics',
entityType: 'Dataset',
platform: 'Redis',
health: 'Good',
columns: [
{ name: 'metric_name', type: 'string' },
{ name: 'value', type: 'float' },
{ name: 'updated_at', type: 'timestamp' }
],
tags: ['Real-Time', 'Metrics', 'Live-Updates'],
glossaryTerms: ['Live Metrics', 'Real-Time Analytics']
}
]
}} />

**Batch Processing Indicators**:

- **Daily/Hourly jobs**: Look for time-based naming (daily_sales, hourly_events)
- **Scheduled dependencies**: Jobs that run in sequence
- **Lag indicators**: How fresh is each step in the pipeline?

**Real-Time Processing Indicators**:

- **Streaming connections**: Kafka topics, event streams
- **Near real-time**: Minimal processing delay (seconds to minutes)
- **Continuous updates**: Always-fresh data

**Performance Insight**: Understanding processing schedules helps set proper expectations for data freshness and availability.

</TabItem>
</Tabs>

## Level 4: Critical Path Analysis

Identify the most important connections in your data ecosystem:

### The Critical Path Method

**High-Impact Paths**:

- **Customer-facing dashboards** ← Highest priority
- **Revenue reporting** ← Business critical
- **Compliance reporting** ← Regulatory requirement
- **Operational monitoring** ← System health

**Dependency Mapping**:

1. **Single points of failure**: One dataset feeding many critical systems
2. **Bottleneck jobs**: Processing that everything depends on
3. **Cross-platform bridges**: Connections between different systems

### Interactive Exercise: Critical Path Identification

<TutorialExercise
type="hands-on"
title="Critical Path Analysis Challenge"
difficulty="advanced"
timeEstimate="12 minutes"
platform="DataHub Lineage"
steps={[
{
title: "Analyze the TechFlow Analytics Data Ecosystem",
description: "Study the lineage diagram below and identify critical dependencies",
expected: "Understand the flow from raw data to business-critical outputs"
},
{
title: "Calculate Critical Scores",
description: "Use the formula: (Business Impact × Downstream Count) + Failure Risk",
expected: "Rank assets by their criticality to business operations"
},
{
title: "Prioritize Monitoring Strategy",
description: "Identify the top 3 assets that need the most protection and monitoring",
expected: "Create a data reliability action plan"
}
]} />

**Scenario**: You're the Data Reliability Engineer at TechFlow Analytics. The CEO wants to know which data assets are most critical to business operations.

<DataHubLineageFlow
title="TechFlow Analytics: Critical Path Analysis"
layout="hierarchical"
showConnections={true}
connectionColors={{
    'customer_transactions': '#8b5cf6',
    'customer_metrics': '#3b82f6',
    'revenue_pipeline': '#f59e0b',
    'user_events': '#10b981'
  }}
layers={[
{
name: "sources",
title: "Source Layer",
nodes: [
{
name: 'customer_transactions',
type: 'Core Revenue Data',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'transaction_id', type: 'bigint' },
{ name: 'customer_id', type: 'bigint' },
{ name: 'amount', type: 'decimal' },
{ name: 'transaction_date', type: 'timestamp' }
],
tags: ['Revenue-Critical', 'High-Volume', 'Real-Time'],
glossaryTerms: ['Transaction Data', 'Revenue Source'],
downstreamConnections: ['revenue_pipeline', 'executive_datasource', 'sales_dashboard']
},
{
name: 'user_events',
type: 'Behavioral Data',
entityType: 'Dataset',
platform: 'Kafka',
health: 'Good',
columns: [
{ name: 'event_id', type: 'string' },
{ name: 'user_id', type: 'bigint' },
{ name: 'event_type', type: 'string' },
{ name: 'properties', type: 'json' }
],
tags: ['Behavioral', 'Streaming', 'Product-Analytics'],
glossaryTerms: ['User Behavior', 'Event Tracking'],
downstreamConnections: ['customer_metrics', 'churn_model']
}
]
},
{
name: "processing",
title: "Processing Layer",
nodes: [
{
name: 'revenue_pipeline',
type: 'Critical ETL Job',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Warning',
tags: ['Revenue-Pipeline', 'Business-Critical', 'Daily-6AM'],
glossaryTerms: ['ETL Process', 'Revenue Processing'],
downstreamConnections: ['customer_metrics', 'executive_datasource', 'churn_model']
}
]
},
{
name: "analytics",
title: "Analytics Layer",
nodes: [
{
name: 'customer_metrics',
type: 'Business KPIs',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'lifetime_value', type: 'decimal' },
{ name: 'churn_probability', type: 'float' },
{ name: 'engagement_score', type: 'float' }
],
tags: ['KPIs', 'Customer-Analytics', 'ML-Features'],
glossaryTerms: ['Customer Metrics', 'Business KPIs'],
downstreamConnections: ['executive_datasource', 'churn_model', 'customer_api', 'compliance_report']
}
]
},
{
name: "outputs",
title: "Output Layer",
subLayersLayout: 'columns',
nodes: [
{
name: 'customer_api',
type: 'Customer-Facing API',
entityType: 'Dataset',
platform: 'DynamoDB',
health: 'Good',
tags: ['Customer-Facing', 'Real-Time', 'High-Availability'],
glossaryTerms: ['Customer API', 'Real-Time Service']
},
{
name: 'compliance_report',
type: 'Regulatory Report',
entityType: 'Dashboard',
platform: 'DataHub',
health: 'Good',
tags: ['Compliance', 'Regulatory', 'Monthly'],
glossaryTerms: ['Compliance Reporting', 'Regulatory Data']
},
{
name: 'churn_model',
type: 'ML Prediction Model',
entityType: 'MLModel',
platform: 'MLflow',
health: 'Good',
tags: ['Machine-Learning', 'Churn-Prediction', 'Production'],
glossaryTerms: ['Churn Model', 'Predictive Analytics']
},
{
name: 'sales_dashboard',
type: 'Sales Team Dashboard',
entityType: 'Dashboard',
platform: 'Looker',
health: 'Good',
tags: ['Sales', 'Operational', 'Real-Time'],
glossaryTerms: ['Sales Dashboard', 'Operational Reporting']
}
],
subLayers: [
{
nodes: [
{
name: 'executive_datasource',
type: 'Published Datasource',
entityType: 'Dataset',
platform: 'Tableau',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'transaction_count', type: 'bigint' },
{ name: 'transaction_date', type: 'date' },
{ name: 'total_spend', type: 'float' }
],
tags: [],
glossaryTerms: ['Published Datasource', 'Tableau Data'],
downstreamConnections: ['executive_chart']
}
]
},
{
nodes: [
{
name: 'executive_chart',
type: 'Revenue Chart',
entityType: 'Chart',
platform: 'Tableau',
health: 'Good',
tags: ['Chart', 'Revenue-Visualization', 'Executive'],
glossaryTerms: ['Revenue Chart', 'Executive Visualization'],
downstreamConnections: ['executive_workbook']
}
]
},
{
nodes: [
{
name: 'executive_workbook',
type: 'Workbook',
entityType: 'Dashboard',
platform: 'Tableau',
health: 'Good',
tags: ['Executive', 'Revenue-Reporting', 'Daily-Brief'],
glossaryTerms: ['Executive Workbook', 'Strategic Reporting']
}
]
}
]
}
]}
/>

**Dependency Count Analysis** (visible in the diagram above):

- **customer_transactions** → feeds **3 systems** (see purple connection lines): revenue_pipeline, executive_datasource, sales_dashboard
- **customer_metrics** → feeds **4 systems** (see blue connection lines): executive_datasource, churn_model, customer_api, compliance_report
- **revenue_pipeline** → feeds **3 systems** (see orange connection lines): customer_metrics, executive_datasource, churn_model
- **user_events** → feeds **2 systems** (see green connection lines): customer_metrics, churn_model

**Your Analysis Task**:

Using the lineage diagram above, calculate the **Critical Score** for each asset using this formula:

**Critical Score = (Business Impact × Downstream Dependencies) + Failure Risk**

Where:

- **Business Impact**: 1-10 (10 = affects revenue/customers directly)
- **Downstream Dependencies**: Count of systems that depend on this asset
- **Failure Risk**: 1-10 (10 = high probability of failure)

**Analysis Framework**:

<Tabs>
<TabItem value="your-analysis" label="Your Analysis">

**Step 1**: Count the downstream dependencies for each asset by examining the lineage diagram:

| Asset                 | Business Impact (1-10) | Downstream Count | Failure Risk (1-10) | Critical Score |
| --------------------- | ---------------------- | ---------------- | ------------------- | -------------- |
| customer_transactions | \_\_\_                 | \_\_\_           | \_\_\_              | \_\_\_         |
| revenue_pipeline      | \_\_\_                 | \_\_\_           | \_\_\_              | \_\_\_         |
| customer_metrics      | \_\_\_                 | \_\_\_           | \_\_\_              | \_\_\_         |
| user_events           | \_\_\_                 | \_\_\_           | \_\_\_              | \_\_\_         |

**Step 2**: Rank your top 3 most critical assets:

1. **Most Critical**: **\*\***\_\_\_\_**\*\***
2. **Second Critical**: **\*\***\_\_\_\_**\*\***
3. **Third Critical**: **\*\***\_\_\_\_**\*\***

**Step 3**: Justify your choices:

- **Why is #1 most critical?** **\*\***\_\_\_\_**\*\***
- **What monitoring would you implement?** **\*\***\_\_\_\_**\*\***

</TabItem>
<TabItem value="expert-analysis" label="Expert Analysis">

**Correct Analysis** (Data Reliability Engineer perspective):

| Asset                     | Business Impact | Downstream Count | Failure Risk | Critical Score | Reasoning                        |
| ------------------------- | --------------- | ---------------- | ------------ | -------------- | -------------------------------- |
| **customer_transactions** | **10**          | **4**            | **6**        | **46**         | Revenue source feeding 4 systems |
| **revenue_pipeline**      | **9**           | **3**            | **8**        | **35**         | Critical ETL with Warning status |
| **customer_metrics**      | **8**           | **4**            | **5**        | **37**         | KPIs feeding multiple dashboards |
| **user_events**           | **7**           | **2**            | **4**        | **18**         | Important but fewer dependencies |

**Top 3 Critical Assets** (in priority order):

### 1. **customer_transactions** (Score: 46) - HIGHEST PRIORITY

**Why Critical**:

- Direct revenue impact (Business Impact: 10/10)
- Feeds 4 downstream systems (revenue_pipeline, customer_metrics, executive_dashboard, sales_dashboard)
- Single point of failure for all revenue reporting

**Monitoring Strategy**:

- Real-time transaction volume monitoring
- Data freshness alerts (< 5 minute SLA)
- Schema change detection
- Database connection health checks
- Automated failover to backup systems

### 2. **customer_metrics** (Score: 37) - HIGH PRIORITY

**Why Critical**:

- Core business KPIs (Business Impact: 8/10)
- Feeds executive dashboard, churn model, customer API, compliance reports
- ML model dependency creates cascading failures

**Monitoring Strategy**:

- Data quality assertions on key metrics
- Anomaly detection on metric values
- Lineage validation checks
- Model performance monitoring

### 3. **revenue_pipeline** (Score: 35) - HIGH PRIORITY

**Why Critical**:

- Already showing Warning status (Failure Risk: 8/10)
- Critical ETL processing revenue data
- Scheduled dependency (failure affects daily reporting)

**Monitoring Strategy**:

- Job execution monitoring with alerts
- Data pipeline SLA tracking
- Resource utilization monitoring
- Automated retry mechanisms
- Escalation procedures for failures

**Key Insight**: `customer_transactions` is the highest priority because it's both the revenue source AND feeds the most downstream systems. If it fails, everything breaks.

</TabItem>
<TabItem value="common-mistakes" label="Common Mistakes">

**Mistake #1: Focusing Only on Business Impact**
❌ **Wrong**: "Executive dashboard is most critical because the CEO uses it"
✅ **Correct**: "customer_transactions is most critical because it feeds the executive dashboard AND 3 other systems"

**Why**: Single points of failure with many dependencies are more critical than high-visibility endpoints.

**Mistake #2: Ignoring Current Health Status**
❌ **Wrong**: "All systems look healthy, so failure risk is low"
✅ **Correct**: "revenue_pipeline shows Warning status, indicating higher failure risk"

**Why**: Current system health is a leading indicator of future failures.

**Mistake #3: Not Considering Cascading Failures**
❌ **Wrong**: "Each system failure affects only its direct outputs"
✅ **Correct**: "customer_transactions failure cascades through revenue_pipeline to all dashboards"

**Why**: Data lineage shows how failures propagate through the entire ecosystem.

**Mistake #4: Overlooking Processing Dependencies**
❌ **Wrong**: "Dashboards are most critical because users see them"
✅ **Correct**: "The ETL jobs feeding dashboards are more critical because dashboard failures often start there"

**Why**: Processing bottlenecks are common failure points that affect multiple outputs.

**Learning Checkpoint**: Did your analysis match the expert ranking? If not, review the lineage diagram to understand the dependency patterns you missed.

</TabItem>
</Tabs>

**Success Validation**:
✅ **Beginner**: Identified customer_transactions as high priority  
✅ **Intermediate**: Correctly calculated critical scores using the formula  
✅ **Advanced**: Recognized revenue_pipeline's Warning status as a risk factor  
✅ **Expert**: Proposed specific monitoring strategies for each critical asset

## Pro Tips for Lineage Reading

<div className="pro-tips">

**Speed Techniques**:

- **Start broad, then narrow**: Use overview mode first, then zoom into problem areas
- **Follow the business logic**: Revenue flows are usually well-documented and critical
- **Use platform knowledge**: Understand your organization's data architecture patterns

**Accuracy Boosters**:

- **Verify with owners**: Lineage might miss manual processes or external dependencies
- **Check recency**: When was lineage last updated? Stale lineage can mislead
- **Cross-reference documentation**: Combine lineage with technical docs and business context

**Team Efficiency**:

- **Document your findings**: Share critical path analysis with your team
- **Create lineage maps**: Visual summaries for non-technical stakeholders
- **Establish monitoring**: Set up alerts for critical path failures

</div>

## Success Checkpoint

<div className="checkpoint">

**You've mastered lineage reading when you can:**

**Speed Test**: Trace a 5-hop lineage path in under 3 minutes
**Comprehension Test**: Identify all node types and transformation patterns
**Analysis Test**: Determine the critical path for any business process
**Communication Test**: Explain lineage findings to both technical and business stakeholders

**Final Validation**:
Choose a complex dataset in your DataHub instance and create a complete lineage analysis including:

- All upstream dependencies (at least 3 hops)
- Transformation logic at each step
- Critical path assessment
- Potential failure points

</div>

## What You've Learned

**Excellent progress!** You can now read lineage graphs like a professional data engineer:

- **Multi-hop navigation**: Trace complex data flows across systems
- **Node type recognition**: Understand datasets, jobs, and applications
- **Transformation analysis**: Interpret how data changes through processing
- **Critical path identification**: Focus on what matters most for business

:::tip Mark Your Progress
Check off "Reading Lineage Graphs" in the progress tracker above! You're ready to perform impact analysis.
:::

---

**Next**: Now that you can read lineage expertly, let's learn how to [perform systematic impact analysis](impact-analysis.md) →
