import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TutorialProgress from '@site/src/components/TutorialProgress';

# Step 4: Your First Lineage (5 minutes)

<TutorialProgress
tutorialId="quickstart"
currentStep={3}
steps={[
{ title: "Setup DataHub", time: "5 min" },
{ title: "First Data Ingestion", time: "10 min" },
{ title: "Discovery Basics", time: "10 min" },
{ title: "Your First Lineage", time: "5 min" }
]}
/>

**The Final Piece**: You've located the user metrics data (`fct_users_created` and `fct_users_deleted`), but before delivering the analysis, you need to understand something crucial: _Where does this data come from?_ Is it reliable? What happens if something breaks upstream?

**Your Objective**: Use DataHub's lineage features to trace the data pipeline and understand how the organization's user metrics are created. This knowledge will make you confident in your analysis and help you spot potential issues.

## What You'll Accomplish

By the end of this step, you'll be able to:

- Navigate lineage graphs to understand data flow
- Distinguish between upstream and downstream dependencies
- Use lineage for impact analysis and troubleshooting
- Understand column-level lineage relationships

## Understanding Data Lineage

Data lineage provides a comprehensive view of data flow throughout your organization, tracking data from its origin through all transformations to final consumption points.

**Enterprise Lineage Components:**

- **Source Systems**: Original data repositories and databases
- **Transformation Layers**: ETL processes, data pipelines, and business logic
- **Intermediate Storage**: Staging areas, data warehouses, and data lakes
- **Consumption Points**: Reports, dashboards, and analytical applications
- **Data Dependencies**: Relationships between datasets and processes

**Why lineage matters:**

- **Impact Analysis**: "What breaks if I change this table?"
- **Root Cause Analysis**: "Why is this dashboard showing wrong numbers?"
- **Data Governance**: "Where does this sensitive data flow?"
- **Compliance**: "Can we trace this data back to its source?"

## Tracing Enterprise Data Pipelines

### Method 1: Following User Metrics Data Trail

Let's trace the lineage of user analytics data:

1. **Navigate to `fct_users_created`** (the table you found in discovery)

2. **Click the "Lineage" tab** to see the data story

3. **Analyze the enterprise data flow:**
   - **Upstream (left)**: `logging_events` - This is where user creation events are captured
   - **Current dataset (center)**: `fct_users_created` - The processed analytics table for user metrics
   - **Downstream (right)**: Any dashboards or reports that use this data

**What This Tells You**: The user creation data flows from raw events (`logging_events`) through processing into the analytics table (`fct_users_created`). This is a clean, reliable pipeline!

### Method 2: Global Lineage View

1. **From any dataset page**, click the **"View Lineage"** button

2. **This opens the full lineage explorer** with:
   - Interactive graph visualization
   - Zoom and pan controls
   - Filter options
   - Multi-hop lineage traversal

## Reading Lineage Graphs

Let's understand the visual elements:

### Node Types

<Tabs>
<TabItem value="datasets" label="Datasets">

**Tables/Views** (rectangular nodes):

- **Database tables**: Raw operational data
- **Analytics views**: Transformed/aggregated data
- **Materialized views**: Pre-computed results
- **Files**: CSV, Parquet, JSON data files

</TabItem>
<TabItem value="pipelines" label="Pipelines">

**Data Jobs** (circular nodes):

- **ETL jobs**: Extract, Transform, Load processes
- **dbt models**: Data transformation logic
- **Python scripts**: Custom data processing
- **Airflow DAGs**: Workflow orchestration

</TabItem>
<TabItem value="applications" label="Applications">

**Consuming Applications** (diamond nodes):

- **BI Dashboards**: Looker, Tableau, PowerBI
- **ML Models**: Training and inference pipelines
- **Applications**: Customer-facing features
- **Reports**: Automated business reports

</TabItem>
</Tabs>

### Connection Types

**Solid lines**: Direct data dependencies
**Dashed lines**: Indirect or inferred relationships
**Colored lines**: Different types of transformations

## Practical Lineage Scenarios

### Scenario 1: Impact Analysis

**Question**: "I need to update the customer table schema. What will be affected?"

<DataHubLineageFlow {...{
title: "Impact Analysis: Customer Table Schema Change",
nodes: [
{
name: 'customers',
type: 'Source Table',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'email', type: 'string' },
{ name: 'first_name', type: 'string' },
{ name: 'last_name', type: 'string' },
{ name: 'created_at', type: 'timestamp' }
],
tags: ['PII', 'Core-Data', 'High-Impact'],
glossaryTerms: ['Customer Data', 'Source System']
},
{
name: 'customer_analytics',
type: 'Analytics Table',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'full_name', type: 'string' },
{ name: 'customer_segment', type: 'string' },
{ name: 'lifetime_value', type: 'decimal' }
],
tags: ['Analytics', 'Derived', 'Business-Critical'],
glossaryTerms: ['Customer Analytics', 'Business Intelligence']
},
{
name: 'ml_features',
type: 'Feature Store',
entityType: 'Dataset',
platform: 'Spark',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'engagement_score', type: 'float' },
{ name: 'churn_probability', type: 'float' }
],
tags: ['ML-Features', 'Real-Time', 'Predictive'],
glossaryTerms: ['Machine Learning', 'Feature Engineering']
},
{
name: 'daily_report_job',
type: 'ETL Process',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Good',
tags: ['Scheduled', 'Daily', 'Reporting']
},
{
name: 'customer_dashboard',
type: 'BI Dashboard',
entityType: 'Dashboard',
platform: 'Looker',
health: 'Good',
tags: ['Executive', 'Real-Time', 'Customer-Metrics'],
glossaryTerms: ['Business Dashboard', 'Customer Insights']
},
{
name: 'churn_model',
type: 'ML Model',
entityType: 'MLModel',
platform: 'MLflow',
health: 'Good',
tags: ['Production', 'Churn-Prediction', 'Critical'],
glossaryTerms: ['Churn Model', 'Predictive Analytics']
},
{
name: 'recommendation_api',
type: 'API Service',
entityType: 'Dataset',
platform: 'Kafka',
health: 'Good',
tags: ['Real-Time', 'Customer-Facing', 'High-Volume'],
glossaryTerms: ['Recommendation Engine', 'Customer Experience']
},
{
name: 'executive_dashboard',
type: 'Executive Report',
entityType: 'Dashboard',
platform: 'Tableau',
health: 'Good',
tags: ['Executive', 'Strategic', 'Daily'],
glossaryTerms: ['Executive Reporting', 'Strategic Metrics']
}
]
}} />

**Steps to Analyze Impact**:

1. **Navigate to the `customers` table** in DataHub
2. **Click the Lineage tab** to see the full dependency graph
3. **Look at downstream dependencies** (right side of the lineage view)
4. **Identify all affected systems**:
   - Analytics tables that read from customers
   - Dashboards that display customer data
   - ML models that use customer features
   - Reports that include customer metrics

**Impact Assessment**: Any schema change to the `customers` table will potentially affect 8 downstream systems, requiring coordinated updates and testing.

### Scenario 2: Root Cause Analysis

**Question**: "The customer dashboard shows wrong numbers. Where's the problem?"

<DataHubLineageFlow {...{
title: "Root Cause Analysis: Customer Dashboard Issues",
nodes: [
{
name: 'raw_customers',
type: 'Raw Data Source',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'email', type: 'string' },
{ name: 'registration_date', type: 'timestamp' },
{ name: 'status', type: 'string' }
],
tags: ['Source', 'Raw-Data', 'Operational'],
glossaryTerms: ['Raw Customer Data', 'Source System']
},
{
name: 'etl_job',
type: 'Data Pipeline',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Warning',
tags: ['ETL', 'Transformation', 'Check-This-First'],
glossaryTerms: ['ETL Process', 'Data Pipeline']
},
{
name: 'customer_metrics',
type: 'Processed Metrics',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Warning',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'total_orders', type: 'integer' },
{ name: 'lifetime_value', type: 'decimal' },
{ name: 'last_activity', type: 'timestamp' }
],
tags: ['Metrics', 'Aggregated', 'Potentially-Stale'],
glossaryTerms: ['Customer Metrics', 'Business KPIs']
},
{
name: 'customer_dashboard',
type: 'Problem Dashboard',
entityType: 'Dashboard',
platform: 'Looker',
health: 'Critical',
tags: ['Dashboard', 'Reporting-Issues', 'Business-Critical'],
glossaryTerms: ['Customer Dashboard', 'Business Intelligence']
}
]
}} />

**Debugging Steps**:

1. **Start at the `customer_dashboard`** (the problem location)
2. **Trace upstream dependencies** (left side of lineage view)
3. **Check each step systematically**:
   - **ETL Job**: Did it run successfully? Check logs for failures
   - **Customer Metrics**: Is the data fresh? Look at last update timestamp
   - **Raw Customers**: Is source data being updated correctly?

**Root Cause Investigation Priority**:

1. **Check ETL Job first** - Most common failure point
2. **Verify data freshness** - Look for stale or missing data
3. **Validate transformations** - Ensure business logic is correct
4. **Confirm source data quality** - Check for upstream issues

**Common Issues Found**:

- ETL job failed silently due to schema changes
- Data pipeline running but processing stale data
- Transformation logic changed without proper testing
- Source system connectivity problems

### Scenario 3: Data Governance

**Question**: "This table contains PII. Where does this sensitive data flow?"

<DataHubLineageFlow {...{
title: "Data Governance: PII Data Flow Tracking",
nodes: [
{
name: 'customer_profiles',
type: 'PII Source Table',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'email', type: 'string' },
{ name: 'phone_number', type: 'string' },
{ name: 'ssn', type: 'string' },
{ name: 'address', type: 'string' }
],
tags: ['PII', 'Sensitive', 'GDPR-Regulated', 'High-Security'],
glossaryTerms: ['Personal Data', 'Sensitive Information']
},
{
name: 'anonymized_analytics',
type: 'Anonymized Data',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
columns: [
{ name: 'customer_hash', type: 'string' },
{ name: 'region_code', type: 'string' },
{ name: 'age_group', type: 'string' },
{ name: 'purchase_behavior', type: 'string' }
],
tags: ['Anonymized', 'Analytics-Safe', 'GDPR-Compliant'],
glossaryTerms: ['Anonymized Data', 'Privacy Compliant']
},
{
name: 'crm_system',
type: 'Customer Management',
entityType: 'Dataset',
platform: 'Salesforce',
health: 'Good',
columns: [
{ name: 'customer_id', type: 'bigint' },
{ name: 'email', type: 'string' },
{ name: 'phone_number', type: 'string' },
{ name: 'contact_preferences', type: 'string' }
],
tags: ['PII', 'Customer-Service', 'Access-Controlled'],
glossaryTerms: ['Customer Relationship Management', 'Contact Information']
},
{
name: 'marketing_campaigns',
type: 'Marketing System',
entityType: 'Dataset',
platform: 'HubSpot',
health: 'Warning',
columns: [
{ name: 'email', type: 'string' },
{ name: 'campaign_id', type: 'string' },
{ name: 'opt_in_status', type: 'boolean' }
],
tags: ['PII', 'Marketing', 'Consent-Required', 'Audit-This'],
glossaryTerms: ['Marketing Data', 'Email Marketing']
},
{
name: 'compliance_audit',
type: 'Audit Trail',
entityType: 'Dataset',
platform: 'DataHub',
health: 'Good',
tags: ['Audit', 'Compliance', 'Access-Logs'],
glossaryTerms: ['Compliance Audit', 'Data Access Tracking']
}
]
}} />

**Governance Investigation Steps**:

1. **Find the PII source** (e.g., `customer_profiles` table)
2. **Examine all downstream paths** using DataHub lineage
3. **Identify systems receiving sensitive data**:

   - CRM systems (legitimate business use)
   - Marketing platforms (verify consent)
   - Analytics systems (should be anonymized)
   - Third-party integrations (compliance risk)

4. **Verify proper controls**:
   - Access permissions and role-based security
   - Data anonymization where required
   - Consent management for marketing use
   - Audit trails for compliance reporting

**Compliance Checklist**:

- ✅ **Anonymized Analytics**: PII removed, GDPR compliant
- ✅ **CRM System**: Legitimate business purpose, access controlled
- ⚠️ **Marketing Campaigns**: Verify consent and opt-in status
- ✅ **Compliance Audit**: Full access tracking enabled

**Action Items**: Review marketing system access to ensure proper consent management and consider additional anonymization.

## Column-Level Lineage

For detailed analysis, DataHub can show how individual columns flow through transformations:

### Viewing Column Lineage

1. **In the Schema tab** of any dataset
2. **Click on a specific column**
3. **Select "View Column Lineage"**

This shows:

- **Source columns**: Which upstream columns contribute to this field
- **Transformation logic**: How the column is calculated or derived
- **Downstream usage**: Where this column is used in other systems

### Example: Customer Segment Column

```sql
-- Source: customers.customer_type + orders.total_spent
-- Transformation:
CASE
  WHEN total_spent > 1000 THEN 'Premium'
  WHEN total_spent > 500 THEN 'Standard'
  ELSE 'Basic'
END as customer_segment

-- Used in: marketing_campaigns, customer_dashboard, ml_features
```

## Lineage Best Practices

### For Data Consumers

1. **Always check lineage** before using unfamiliar data
2. **Trace to the source** to understand data freshness and quality
3. **Identify alternatives** by looking at similar downstream datasets
4. **Contact upstream owners** when you need data changes

### For Data Producers

1. **Document transformations** so lineage is meaningful
2. **Use consistent naming** to make lineage easier to follow
3. **Tag critical paths** to highlight important data flows
4. **Monitor downstream usage** to understand impact of changes

## Advanced Lineage Features

### Multi-Hop Lineage

**View end-to-end data journeys:**

- Set lineage depth to 3+ hops
- Trace from raw source to final application
- Understand complete data supply chains

### Lineage Filtering

**Focus on specific aspects:**

- Filter by entity type (datasets only, pipelines only)
- Filter by platform (show only Snowflake → dbt flow)
- Filter by time (show recent lineage changes)

### Lineage Search

**Find specific relationships:**

- "Show me all paths from customers to dashboards"
- "Find datasets that depend on this API"
- "Trace this column through all transformations"

## Troubleshooting Lineage Issues

<Tabs>
<TabItem value="missing-lineage" label="Missing Lineage">

**Issue**: Expected lineage connections don't appear

**Common causes**:

- Ingestion didn't capture SQL parsing
- Complex transformations not detected
- Cross-platform connections not configured

**Solutions**:

- Enable SQL parsing in ingestion config
- Add manual lineage for complex cases
- Check cross-platform lineage settings

</TabItem>
<TabItem value="incorrect-lineage" label="Incorrect Lineage">

**Issue**: Lineage shows wrong relationships

**Common causes**:

- Temporary tables confusing lineage detection
- Dynamic SQL not parsed correctly
- Naming conflicts between systems

**Solutions**:

- Review and correct automatic lineage
- Add manual lineage overrides
- Use more specific naming conventions

</TabItem>
<TabItem value="performance" label="Slow Lineage">

**Issue**: Lineage graphs load slowly

**Common causes**:

- Very deep lineage (many hops)
- Large number of connected entities
- Complex transformation logic

**Solutions**:

- Limit lineage depth
- Use filters to focus on relevant paths
- Break down complex transformations

</TabItem>
</Tabs>

## Tutorial Objectives Achieved

**You've successfully completed your DataHub journey when you can:**

- **Navigate lineage confidently**: Trace enterprise data from `logging_events` to `fct_users_created`
- **Understand data reliability**: Know that user metrics come from a clean, traceable pipeline
- **Identify data owners**: You know John Doe owns the user analytics pipeline
- **Assess data quality**: The lineage shows a proper fact table structure

**Your Achievement**: In 30 minutes, you've mastered essential DataHub skills! You can now:

- **Deploy DataHub** and connect multi-platform data architectures
- **Find specific datasets** using strategic search techniques
- **Understand data pipelines** through lineage analysis
- **Deliver confident analysis** backed by metadata insights

**Analysis Ready**: You now have everything needed to answer business questions about user creation vs. deletion metrics, plus the confidence that comes from understanding the complete data pipeline.

:::tip Mark Your Progress
Check off "Your First Lineage" in the progress tracker above! You've completed the entire DataHub Quickstart.
:::

## Tutorial Complete

You've completed the **DataHub in 30 Minutes** tutorial! You now have hands-on experience with DataHub's core capabilities:

**Deployed DataHub** locally and understand its architecture  
**Ingested metadata** from data sources  
**Discovered datasets** using search and browse features  
**Traced data lineage** to understand dependencies

## What's Next?

Now that you understand DataHub fundamentals, explore these advanced topics:

### Immediate Next Steps

- **[Data Discovery & Search](../discovery/overview.md)** - Master advanced search techniques and filters
- **[Data Lineage & Impact Analysis](../lineage/overview.md)** - Deep dive into lineage analysis and troubleshooting
- **Data Governance Fundamentals (coming soon)** - Learn about ownership, classification, and business glossaries

### For Your Organization

- **Plan your DataHub deployment** for production use
- **Identify key data sources** to ingest first
- **Establish governance processes** for metadata management
- **Train your team** on DataHub best practices

### Get Help & Stay Connected

- **[Join DataHub Slack](https://datahub.com/slack)** - Connect with the community
- **[Read the full documentation](../../)** - Comprehensive guides and references
- **[Watch DataHub tutorials](https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w)** - Video walkthroughs
- **[Report issues](https://github.com/datahub-project/datahub/issues)** - Help improve DataHub

**Happy data discovering!**
