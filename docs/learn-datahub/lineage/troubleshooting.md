import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import InteractiveDiagram from '@site/src/components/InteractiveDiagram';
import TutorialProgress from '@site/src/components/TutorialProgress';

# Lineage Troubleshooting (10 minutes)

:::info Tutorial Progress
**Step 3 of 3** | **10 minutes** | [Overview](overview.md) → [Reading Lineage](reading-lineage.md) → [Impact Analysis](impact-analysis.md) → **Troubleshooting**
:::

<TutorialProgress
tutorialId="lineage"
currentStep={2}
steps={[
{ title: "Reading Lineage Graphs", time: "15 min", description: "Navigate complex lineage graphs like a data flow expert" },
{ title: "Performing Impact Analysis", time: "15 min", description: "Systematically assess downstream effects before making changes" },
{ title: "Lineage Troubleshooting", time: "10 min", description: "Debug missing connections and improve lineage accuracy" }
]}
/>

**The Mystery**: Three weeks after the TechFlow migration, you notice something troubling. The new ML pipeline that processes customer segments isn't showing up in DataHub's lineage graph. The data team is asking questions, and you need to figure out why this critical connection is missing.

**Your Mission**: Master the art of lineage troubleshooting - from diagnosing missing connections to proactively improving lineage quality across your entire data ecosystem.

## What You'll Master

By the end of this step, you'll be able to:

- **Diagnose missing lineage** using systematic debugging techniques
- **Fix ingestion issues** that cause incomplete lineage capture
- **Handle edge cases** like manual processes and external dependencies
- **Establish monitoring** to maintain lineage quality over time

## The Lineage Troubleshooting Framework

Professional lineage debugging follows a systematic approach:

<InteractiveDiagram
nodes={[
{
id: '1',
type: 'input',
data: { label: '1. Identify the Gap\nLocate missing lineage' },
position: { x: 0, y: 100 },
className: 'source-node',
},
{
id: '2',
data: { label: '2. Check Data Sources\nVerify source connections' },
position: { x: 200, y: 100 },
className: 'process-node',
},
{
id: '3',
data: { label: '3. Validate Ingestion\nReview ingestion logs' },
position: { x: 400, y: 100 },
className: 'process-node',
},
{
id: '4',
data: { label: '4. Handle Edge Cases\nAddress complex scenarios' },
position: { x: 600, y: 100 },
className: 'process-node',
},
{
id: '5',
type: 'output',
data: { label: '5. Implement Monitoring\nSet up alerts & tracking' },
position: { x: 800, y: 100 },
className: 'success-node',
},
{
id: 'tip1',
data: { label: 'Pro Tip: Start with the\nmost critical missing link' },
position: { x: 0, y: 250 },
className: 'user-node',
},
{
id: 'tip2',
data: { label: 'Success: Comprehensive\nlineage visibility achieved' },
position: { x: 800, y: 250 },
className: 'success-node',
},
]}
edges={[
{ id: 'e1-2', source: '1', target: '2', animated: true, label: 'analyze' },
{ id: 'e2-3', source: '2', target: '3', animated: true, label: 'verify' },
{ id: 'e3-4', source: '3', target: '4', animated: true, label: 'validate' },
{ id: 'e4-5', source: '4', target: '5', animated: true, label: 'monitor' },
{ id: 'e1-tip1', source: '1', target: 'tip1', style: { strokeDasharray: '5,5' }, label: 'tip' },
{ id: 'e5-tip2', source: '5', target: 'tip2', style: { strokeDasharray: '5,5' }, label: 'outcome' },
]}
title="Lineage Troubleshooting Framework"
height="350px"
/>

## Common Lineage Issues

Understanding the most frequent problems helps you troubleshoot faster:

<div className="common-issues">

**Missing Connections** (60% of issues):

- New systems not yet configured for metadata ingestion
- Changes in connection strings or authentication
- Processing jobs that don't emit lineage metadata
- Manual data movement processes

**Incomplete Metadata** (25% of issues):

- Partial schema information from source systems
- Missing column-level lineage in transformations
- Outdated metadata from infrequent ingestion runs
- Custom applications without metadata instrumentation

**Performance Problems** (10% of issues):

- Lineage graphs too complex to render efficiently
- Ingestion jobs timing out on large metadata volumes
- UI responsiveness issues with deep lineage paths
- Memory constraints during lineage computation

**Stale Information** (5% of issues):

- Metadata not refreshed after system changes
- Cached lineage information showing old connections
- Ingestion schedules not aligned with data pipeline changes
- Manual metadata updates not propagated

</div>

## Step 1: Identify the Gap

Systematic gap identification prevents wasted troubleshooting effort:

### The Gap Analysis Method

<Tabs>
<TabItem value="expected-vs-actual" label="Expected vs. Actual">

**Gap Documentation Template**:

```
Missing Connection: ________________________
Expected Source: ___________________________
Expected Target: ___________________________
Business Process: __________________________
Technical Implementation: ___________________
Last Known Working: ________________________
```

**TechFlow ML Pipeline Example**:

```
Missing Connection: Customer segments → ML training pipeline
Expected Source: customer_segments (Snowflake table)
Expected Target: ml_customer_model (MLflow model)
Business Process: Nightly model retraining using latest customer data
Technical Implementation: Python script with Snowflake connector
Last Known Working: Never appeared in DataHub lineage
```

</TabItem>
<TabItem value="impact-assessment" label="Impact Assessment">

**Missing Lineage Impact**:

**Business Impact**:

- **Incomplete dependency mapping**: Can't assess full impact of customer data changes
- **Risk management gaps**: ML model dependencies not visible to data governance
- **Troubleshooting delays**: Root cause analysis missing critical connections
- **Compliance concerns**: Audit trail incomplete for customer data usage

**Technical Impact**:

- **Change management risk**: Schema changes might break ML pipeline unknowingly
- **Monitoring gaps**: No alerts if upstream customer data quality degrades
- **Documentation inconsistency**: Technical architecture docs don't match reality
- **Team coordination issues**: ML team not notified of customer data changes

</TabItem>
<TabItem value="urgency-prioritization" label="Urgency Prioritization">

**Troubleshooting Priority Matrix**:

| Business Impact | Technical Complexity | Priority   | Action Timeline     |
| --------------- | -------------------- | ---------- | ------------------- |
| High            | Low                  | Critical   | Fix within 24 hours |
| High            | High                 | Important  | Fix within 1 week   |
| Medium          | Low                  | Standard   | Fix within 2 weeks  |
| Medium          | High                 | 🔵 Planned | Fix within 1 month  |
| Low             | Any                  | ⚪ Backlog | Fix when convenient |

**TechFlow ML Pipeline**: High business impact (compliance risk) + Medium complexity = Important (1 week timeline)

</TabItem>
</Tabs>

## Step 2: Check Data Sources

Most lineage issues stem from ingestion configuration problems:

### Ingestion Diagnostics Checklist

<div className="diagnostics-checklist">

**Source System Verification**:

- [ ] **System connectivity**: Can DataHub reach the source system?
- [ ] **Authentication**: Are credentials valid and permissions sufficient?
- [ ] **Metadata availability**: Does the source system expose lineage information?
- [ ] **Recent changes**: Have there been system updates or migrations?

**Ingestion Configuration**:

- [ ] **Recipe accuracy**: Is the ingestion recipe configured correctly?
- [ ] **Scheduling**: Is the ingestion running on the expected schedule?
- [ ] **Scope coverage**: Are all relevant databases/schemas included?
- [ ] **Lineage extraction**: Is lineage extraction enabled in the recipe?

**Execution Status**:

- [ ] **Recent runs**: Has ingestion executed successfully recently?
- [ ] **Error logs**: Are there any ingestion failures or warnings?
- [ ] **Data volume**: Is the expected amount of metadata being ingested?
- [ ] **Processing time**: Are ingestion jobs completing within expected timeframes?

</div>

### Interactive Diagnostics Exercise

<div className="interactive-exercise">

**Your Challenge**: Diagnose the TechFlow ML pipeline lineage gap

**Step 1 - Source System Check**:

```
ML Pipeline System: Python + MLflow + Snowflake
Expected Metadata: Job definitions, data dependencies, model artifacts
Current Status: ________________________________
Issues Found: __________________________________
```

**Step 2 - Ingestion Configuration**:

```
Ingestion Recipe: ______________________________
Last Successful Run: ___________________________
Lineage Extraction Enabled: ____________________
Scope Includes ML Systems: _____________________
```

**Step 3 - Gap Analysis**:

```
Root Cause Hypothesis: _________________________
Confidence Level (1-10): _______________________
Next Troubleshooting Step: _____________________
```

</div>

## Step 3: Validate Ingestion

Deep-dive into ingestion mechanics to find the root cause:

### Ingestion Debugging Techniques

<Tabs>
<TabItem value="log-analysis" label="Log Analysis">

**Log Investigation Strategy**:

**Error Pattern Recognition**:

```bash
# Common error patterns to search for
grep -i "lineage" ingestion.log
grep -i "connection" ingestion.log
grep -i "timeout" ingestion.log
grep -i "permission" ingestion.log
grep -i "schema" ingestion.log
```

**Success Indicators**:

```bash
# Positive signals in logs
grep "Successfully processed" ingestion.log
grep "Lineage extracted" ingestion.log
grep "Metadata ingested" ingestion.log
```

**TechFlow ML Pipeline Investigation**:

```
Expected Log Entry: "Successfully extracted lineage from ml_training_job"
Actual Log Entry: "Warning: No lineage metadata found for Python scripts"
Root Cause: Python ML scripts don't emit DataHub-compatible lineage
```

</TabItem>
<TabItem value="metadata-validation" label="Metadata Validation">

**Metadata Completeness Check**:

**Dataset Metadata**:

- **Schema information**: Are all columns and types captured?
- **Ownership data**: Are dataset owners properly identified?
- **Custom properties**: Are business-relevant attributes included?
- **Platform details**: Is the source system correctly identified?

**Lineage Metadata**:

- **Job information**: Are transformation jobs captured as entities?
- **Input/output mapping**: Are data dependencies clearly defined?
- **Temporal information**: Are processing schedules and frequencies captured?
- **Column-level lineage**: Are field-level transformations tracked?

**Validation Queries**:

```sql
-- Check if ML pipeline datasets exist
SELECT * FROM metadata_aspect
WHERE urn LIKE '%ml_customer_model%';

-- Verify lineage relationships
SELECT * FROM metadata_aspect
WHERE aspect = 'datasetLineage'
AND urn LIKE '%customer_segments%';
```

</TabItem>
<TabItem value="configuration-tuning" label="Configuration Tuning">

**Recipe Optimization**:

**Lineage Extraction Settings**:

```yaml
# Enhanced lineage extraction configuration
source:
  type: "snowflake"
  config:
    # Enable comprehensive lineage extraction
    include_table_lineage: true
    include_view_lineage: true
    include_column_lineage: true

    # Capture custom SQL and stored procedures
    include_usage_statistics: true
    sql_parser_use_external_process: true

    # Extended metadata capture
    profiling:
      enabled: true
      include_field_null_count: true
      include_field_min_value: true
      include_field_max_value: true
```

**Custom Lineage Injection**:

```python
# For systems that don't auto-emit lineage
from datahub.emitter.mce_builder import make_lineage_mce
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create custom lineage for ML pipeline
lineage_mce = make_lineage_mce(
    upstream_urns=["urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)"],
    downstream_urn="urn:li:dataset:(urn:li:dataPlatform:mlflow,ml_customer_model,PROD)"
)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit_mce(lineage_mce)
```

</TabItem>
</Tabs>

## Step 4: Handle Edge Cases

Real-world data pipelines often include scenarios that standard ingestion can't capture:

### Common Edge Cases and Solutions

<div className="edge-cases">

**Manual Data Processes**:

- **Problem**: Excel files, manual data entry, ad-hoc scripts
- **Solution**: Custom metadata emission or documentation-based lineage
- **Implementation**: Create "virtual" datasets representing manual processes

**External System Dependencies**:

- **Problem**: Third-party APIs, vendor data feeds, external databases
- **Solution**: Proxy datasets or external system connectors
- **Implementation**: Document external dependencies as DataHub entities

**Real-time Processing**:

- **Problem**: Streaming pipelines, event-driven architectures, microservices
- **Solution**: Event-based lineage capture or instrumentation
- **Implementation**: Custom lineage emission from application code

**Complex Transformations**:

- **Problem**: Multi-step ETL, custom business logic, conditional processing
- **Solution**: Job-level lineage with detailed transformation documentation
- **Implementation**: Enhanced metadata with transformation descriptions

</div>

### Edge Case Resolution Framework

<Tabs>
<TabItem value="manual-processes" label="📝 Manual Processes">

**Documentation-Based Lineage**:

```python
# Create lineage for manual Excel process
from datahub.emitter.mce_builder import make_dataset_urn, make_lineage_mce

# Define the manual process as a "dataset"
manual_process_urn = make_dataset_urn(
    platform="manual",
    name="monthly_customer_review_excel",
    env="PROD"
)

# Create lineage from automated data to manual process
lineage_mce = make_lineage_mce(
    upstream_urns=["urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)"],
    downstream_urn=manual_process_urn
)

# Add custom properties to explain the manual process
properties = {
    "process_description": "Monthly customer review conducted by business team",
    "frequency": "Monthly",
    "owner": "customer_success_team",
    "documentation_url": "https://wiki.company.com/customer-review-process"
}
```

**Benefits**:

- Complete lineage visibility including manual steps
- Documentation of business processes in technical lineage
- Compliance and audit trail for manual data handling

</TabItem>
<TabItem value="external-systems" label="🌐 External Systems">

**Proxy Dataset Approach**:

```python
# Create proxy for external API data source
external_api_urn = make_dataset_urn(
    platform="external_api",
    name="customer_enrichment_service",
    env="PROD"
)

# Document the external dependency
external_properties = {
    "api_endpoint": "https://api.customerdata.com/v2/enrichment",
    "update_frequency": "Real-time",
    "data_provider": "CustomerData Inc.",
    "sla": "99.9% uptime",
    "contact": "support@customerdata.com"
}

# Create lineage showing external data flow
lineage_mce = make_lineage_mce(
    upstream_urns=[external_api_urn],
    downstream_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,enriched_customers,PROD)"
)
```

**Benefits**:

- Visibility into external data dependencies
- Risk assessment for third-party data sources
- Contact information for external data issues

</TabItem>
<TabItem value="application-instrumentation" label="Application Instrumentation">

**Code-Level Lineage Emission**:

```python
# Instrument ML training pipeline
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetLineageType
from datahub.metadata.schema_classes import DatasetLineageClass

def train_customer_model():
    # Your ML training code here
    input_data = load_customer_segments()
    model = train_model(input_data)
    save_model(model)

    # Emit lineage metadata
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

    lineage = DatasetLineageClass(
        upstreams=[
            {
                "dataset": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_segments,PROD)",
                "type": DatasetLineageType.TRANSFORMED
            }
        ]
    )

    model_urn = "urn:li:dataset:(urn:li:dataPlatform:mlflow,ml_customer_model,PROD)"
    emitter.emit_mcp(
        MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=model_urn,
            aspectName="datasetLineage",
            aspect=lineage
        )
    )
```

**Benefits**:

- Real-time lineage updates as code executes
- Accurate capture of dynamic data dependencies
- Integration with application deployment pipelines

</TabItem>
</Tabs>

## Step 5: Implement Monitoring

Proactive lineage quality management prevents future troubleshooting:

### Lineage Quality Monitoring Framework

<div className="monitoring-framework">

**Quality Metrics**:

- **Coverage**: Percentage of data assets with complete lineage
- **Freshness**: How recently lineage information was updated
- **Accuracy**: Validation of lineage against known data flows
- **Completeness**: Presence of both upstream and downstream connections

**Alert Conditions**:

- **Missing lineage**: New datasets without any lineage connections
- **Stale metadata**: Lineage not updated within expected timeframe
- **Broken connections**: Previously connected systems showing gaps
- **Ingestion failures**: Metadata extraction jobs failing repeatedly

**Maintenance Tasks**:

- **Regular validation**: Quarterly review of critical data lineage
- **Configuration updates**: Adjust ingestion recipes as systems evolve
- **Documentation sync**: Keep manual lineage documentation current
- **Team training**: Ensure new team members understand lineage practices

</div>

### Monitoring Implementation

<Tabs>
<TabItem value="automated-monitoring" label="Automated Monitoring">

**Lineage Quality Dashboard**:

```sql
-- Lineage coverage metrics
SELECT
    platform,
    COUNT(*) as total_datasets,
    COUNT(CASE WHEN has_upstream_lineage THEN 1 END) as with_upstream,
    COUNT(CASE WHEN has_downstream_lineage THEN 1 END) as with_downstream,
    ROUND(100.0 * COUNT(CASE WHEN has_upstream_lineage THEN 1 END) / COUNT(*), 2) as upstream_coverage_pct
FROM dataset_lineage_summary
GROUP BY platform
ORDER BY upstream_coverage_pct DESC;

-- Stale lineage detection
SELECT
    dataset_urn,
    last_lineage_update,
    DATEDIFF(CURRENT_DATE, last_lineage_update) as days_since_update
FROM dataset_metadata
WHERE DATEDIFF(CURRENT_DATE, last_lineage_update) > 7
ORDER BY days_since_update DESC;
```

**Automated Alerts**:

```python
# Lineage quality monitoring script
def check_lineage_quality():
    critical_datasets = [
        "customer_segments",
        "fct_users_created",
        "ml_customer_model"
    ]

    for dataset in critical_datasets:
        lineage_age = get_lineage_age(dataset)
        if lineage_age > 7:  # days
            send_alert(f"Stale lineage for {dataset}: {lineage_age} days old")

        if not has_upstream_lineage(dataset):
            send_alert(f"Missing upstream lineage for {dataset}")
```

</TabItem>
<TabItem value="manual-validation" label="Manual Validation">

**Quarterly Lineage Review Process**:

**Review Checklist**:

- [ ] **Critical path validation**: Verify lineage for top 10 most important datasets
- [ ] **New system integration**: Ensure recently added systems appear in lineage
- [ ] **Accuracy spot checks**: Validate 5% random sample against known data flows
- [ ] **Documentation updates**: Sync lineage with architecture documentation

**Validation Template**:

```
Dataset: ___________________________________
Expected Upstream Count: ____________________
Actual Upstream Count: ______________________
Expected Downstream Count: __________________
Actual Downstream Count: ____________________
Discrepancies Found: ________________________
Action Required: ____________________________
Validation Date: ____________________________
Reviewer: ___________________________________
```

</TabItem>
<TabItem value="team-processes" label="👥 Team Processes">

**Lineage Governance Framework**:

**Roles and Responsibilities**:

- **Data Engineers**: Ensure new pipelines emit proper lineage metadata
- **Analytics Engineers**: Validate lineage for dbt models and transformations
- **Data Platform Team**: Maintain ingestion infrastructure and monitoring
- **Data Governance**: Review lineage completeness for compliance requirements

**Process Integration**:

- **Code Review**: Include lineage validation in data pipeline code reviews
- **Deployment Gates**: Require lineage metadata before production deployment
- **Incident Response**: Use lineage for root cause analysis and impact assessment
- **Architecture Reviews**: Validate lineage against system design documents

**Training and Documentation**:

- **Onboarding**: Include lineage best practices in new team member training
- **Playbooks**: Document troubleshooting procedures for common lineage issues
- **Best Practices**: Maintain guidelines for lineage metadata emission
- **Tool Training**: Regular sessions on DataHub lineage features and capabilities

</TabItem>
</Tabs>

## Success Checkpoint

<div className="checkpoint">

**You've mastered lineage troubleshooting when you can:**

**Diagnostic Skills**:

- Systematically identify and categorize lineage gaps
- Debug ingestion issues using logs and configuration analysis
- Validate metadata completeness and accuracy
- Prioritize troubleshooting efforts based on business impact

**Technical Skills**:

- Configure ingestion recipes for optimal lineage extraction
- Implement custom lineage emission for edge cases
- Handle manual processes and external system dependencies
- Instrument applications for real-time lineage updates

**Operational Skills**:

- Establish monitoring and alerting for lineage quality
- Create validation processes for ongoing lineage accuracy
- Integrate lineage governance into team workflows
- Train teams on lineage best practices and troubleshooting

**Final Validation**:
Identify a lineage gap in your organization and resolve it using the systematic troubleshooting framework you've learned.

</div>

## Mission Accomplished: Lineage Mastery Complete!

**Congratulations!** You've completed the entire Data Lineage & Impact Analysis series and achieved expert-level proficiency:

**Reading Lineage Graphs**: Navigate any complexity with confidence
**Performing Impact Analysis**: Systematically assess and communicate change risks  
**Lineage Troubleshooting**: Diagnose and resolve any lineage quality issue

**Your New Capabilities**:

- **Lead system migrations** with comprehensive impact analysis
- **Troubleshoot data issues** using lineage-driven root cause analysis
- **Improve data governance** through complete lineage visibility
- **Mentor teams** on lineage best practices and troubleshooting techniques

**Real-World Impact**: You're now equipped to handle the most complex data lineage challenges in production environments, from multi-system migrations to compliance audits to incident response.

:::tip Mark Your Progress
Check off "Lineage Troubleshooting" in the progress tracker above! You've completed the entire lineage mastery series!
:::

---

**Ready for More?** Continue your DataHub expertise journey with:

- **Data Governance Fundamentals (coming soon)** - Master ownership, classification, and business glossary
- **Data Quality & Monitoring (coming soon)** - Learn assertions, health dashboards, and incident management
- **Data Ingestion Mastery (coming soon)** - Deep dive into recipes, stateful ingestion, and profiling
