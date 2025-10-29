import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TutorialProgress from '@site/src/components/TutorialProgress';
import DataHubEntityCard, { SampleEntities } from '@site/src/components/DataHubEntityCard';
import DataHubLineageNode, { DataHubLineageFlow, SampleLineageFlows } from '@site/src/components/DataHubLineageNode';
import ProcessFlow, { DataHubWorkflows } from '@site/src/components/ProcessFlow';

# Reading Lineage Graphs (15 minutes)

:::info Tutorial Progress
**Step 1 of 3** | **15 minutes** | [Overview](overview.md) → **Reading Lineage** → [Impact Analysis](impact-analysis.md) → [Troubleshooting](troubleshooting.md)
:::

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
{...DataHubWorkflows.lineageAnalysis}
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
- **🔄 Materialized Views**: Pre-computed results for performance
- **📁 File Assets**: CSV, Parquet, JSON files in data lakes

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

- **🔄 ETL Jobs**: Extract, Transform, Load processes
- **🐍 Python Scripts**: Custom data processing logic
- **dbt Models**: Data transformation workflows
- **⚡ Spark Jobs**: Large-scale data processing

**Connection Patterns**:

- **Solid lines**: Direct data dependencies
- **Dashed lines**: Indirect or inferred relationships
- **Arrows**: Direction of data flow (always follows the arrows!)

**Analysis Technique**: Jobs between datasets show _how_ data is transformed, not just _that_ it flows.

</TabItem>
<TabItem value="applications" label="📱 Consuming Systems">

**Business Applications**:

- **BI Dashboards**: Looker, Tableau, PowerBI reports
- **🤖 ML Models**: Training and inference pipelines
- **📱 Applications**: Customer-facing features
- **📧 Automated Reports**: Scheduled business reports

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

```
← Hop 5        ← Hop 4         ← Hop 3         ← Hop 2         ← Hop 1         Current Dataset
Raw Source  →  Data Ingestion  →  Validation   →  ETL Process  →  Final Transform  →  fct_users_created
(HDFS Files)   (Kafka Stream)     (Quality Check) (Business Logic) (Aggregation)     (Analytics Table)
```

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
<TabItem value="direct-transforms" label="🔄 Direct Transformations">

**One-to-One Relationships**:

```
Raw Customer Data → Customer Analytics Table
```

**What this means**: Direct processing, usually filtering, aggregation, or enrichment

**Many-to-One Relationships**:

```
Orders + Customers + Products → Sales Analytics
```

**What this means**: Data joining and consolidation

**Analysis Approach**: Look for SQL logic, dbt models, or ETL job definitions to understand the exact transformation.

</TabItem>
<TabItem value="complex-flows" label="🌐 Complex Data Flows">

**Fan-Out Patterns**:

```
Raw Events → [Processing Job] → Multiple Analytics Tables
```

**Business Meaning**: One source feeding multiple business use cases

**Fan-In Patterns**:

```
Multiple Sources → [ETL Job] → Single Data Warehouse Table
```

**Business Meaning**: Data consolidation from various systems

**🚨 Risk Assessment**: Fan-out = high impact if source breaks; Fan-in = complex debugging if output is wrong

</TabItem>
<TabItem value="temporal-flows" label="⏰ Time-Based Processing">

**Batch Processing Indicators**:

- **Daily/Hourly jobs**: Look for time-based naming (daily_sales, hourly_events)
- **Scheduled dependencies**: Jobs that run in sequence
- **Lag indicators**: How fresh is each step in the pipeline?

**Real-Time Processing Indicators**:

- **Streaming connections**: Kafka topics, event streams
- **Near real-time**: Minimal processing delay
- **Continuous updates**: Always-fresh data

**⚡ Performance Insight**: Understand processing schedules to set proper expectations for data freshness.

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

<div className="interactive-exercise">

**Scenario**: You're responsible for data reliability at TechFlow Analytics

**Your Task**: Using lineage, identify the top 3 most critical data assets

**Analysis Framework**:

```
Asset Name: ________________________
Downstream Dependencies: ____________
Business Impact (1-10): _____________
Failure Risk (1-10): _______________
Critical Score: ____________________
```

**Success Criteria**: You can explain why these 3 assets deserve the most monitoring and protection.

</div>

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

**🤝 Team Efficiency**:

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
