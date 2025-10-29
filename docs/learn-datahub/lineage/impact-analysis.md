import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TutorialProgress from '@site/src/components/TutorialProgress';
import DataHubLineageNode, { DataHubLineageFlow } from '@site/src/components/DataHubLineageNode';

# Performing Impact Analysis (15 minutes)

:::info Tutorial Progress
**Step 2 of 3** | **15 minutes** | [Overview](overview.md) → [Reading Lineage](reading-lineage.md) → **Impact Analysis** → [Troubleshooting](troubleshooting.md)
:::

<TutorialProgress
tutorialId="lineage"
currentStep={1}
steps={[
{ title: "Reading Lineage Graphs", time: "15 min", description: "Navigate complex lineage graphs like a data flow expert" },
{ title: "Performing Impact Analysis", time: "15 min", description: "Systematically assess downstream effects before making changes" },
{ title: "Lineage Troubleshooting", time: "10 min", description: "Debug missing connections and improve lineage accuracy" }
]}
/>

**The Critical Decision**: The enterprise analytics migration is approved, but now you need to answer the CEO's question: _"What exactly will be affected, and how do we minimize business risk?"_ This is where impact analysis transforms from guesswork into science.

**Your Mission**: Learn to perform systematic impact analysis that quantifies risk, prioritizes changes, and creates bulletproof migration plans.

## What You'll Master

By the end of this step, you'll be able to:

- **Quantify downstream impact** with business metrics and risk scores
- **Create stakeholder reports** that clearly communicate change effects
- **Develop rollback strategies** based on lineage dependencies
- **Coordinate cross-team changes** using lineage insights

## The Impact Analysis Framework

Professional impact analysis follows a systematic 5-step process:

**Impact Analysis Process:**

1. **Scope Definition** → Define what's changing and why
2. **Downstream Mapping** → Identify all affected systems and stakeholders
3. **Risk Assessment** → Quantify business impact and technical risks
4. **Stakeholder Analysis** → Understand who needs to be involved
5. **Mitigation Planning** → Develop rollback and contingency strategies

## Step 1: Scope Definition

Before analyzing impact, clearly define what's changing:

<div className="scope-framework">

**Change Scope Template**:

```
System/Dataset: ________________________
Change Type: ___________________________
Timeline: ______________________________
Technical Details: _____________________
Business Justification: ________________
```

**Common Change Types**:

- **System Migration**: Moving from one platform to another
- **Schema Changes**: Adding, removing, or modifying columns
- **Performance Optimization**: Changing processing logic or infrastructure
- **Security Updates**: Access control or data classification changes
- **Deprecation**: Retiring old systems or datasets

### Impact Analysis in Action

Here's a real-world example showing how changes cascade through your data ecosystem:

<DataHubLineageFlow {...{
title: "Customer Analytics Migration - Impact Analysis",
nodes: [
{
name: 'customer_data',
type: 'Table',
entityType: 'Dataset',
platform: 'Hive',
health: 'Good',
tags: ['Source', 'Migration-Target'],
glossaryTerms: ['Customer Data']
},
{
name: 'analytics_etl',
type: 'Job',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Warning',
tags: ['ETL', 'Requires-Update']
},
{
name: 'customer_metrics',
type: 'Table',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Critical',
tags: ['Analytics', 'High-Impact'],
glossaryTerms: ['Customer Metrics']
},
{
name: 'ml_features',
type: 'View',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Critical',
tags: ['ML', 'Production'],
glossaryTerms: ['ML Features']
},
{
name: 'recommendation_model',
type: 'Model',
entityType: 'Dataset',
platform: 'MLflow',
health: 'Critical',
tags: ['Production', 'Customer-Facing'],
glossaryTerms: ['Recommendation Model']
}
]
}} />

**Impact Assessment**: This migration affects 15+ downstream systems, including production ML models serving 1M+ customers daily. The health indicators show critical dependencies that require careful coordination.

</div>

### TechFlow Migration Example

Let's apply this to our scenario:

<Tabs>
<TabItem value="scope-definition" label="📋 Scope Definition">

**System/Dataset**: `customer_analytics_pipeline` (Hive tables)
**Change Type**: Platform migration (Hive → Snowflake)
**Timeline**: 48-hour maintenance window, next weekend
**Technical Details**:

- Migrate 5 core tables: `customers`, `orders`, `customer_metrics`, `daily_summaries`, `customer_segments`
- Preserve all existing schemas and data
- Update connection strings in downstream systems

**Business Justification**:

- 10x performance improvement for customer analytics
- $50K/month cost savings
- Enable real-time customer insights

</TabItem>
<TabItem value="risk-factors" label="⚠️ Risk Factors">

**High-Risk Elements**:

- **Customer-facing dashboards**: Sales team uses these daily
- **Automated reports**: CEO gets weekly customer metrics
- **ML pipelines**: Customer segmentation models depend on this data
- **API endpoints**: Mobile app queries customer data directly

**Timing Risks**:

- **Weekend migration**: Limited support staff available
- **Monday morning**: Sales team needs dashboards for weekly planning
- **Month-end**: Customer reporting deadline approaching

**Technical Risks**:

- **Data format differences**: Snowflake vs. Hive SQL variations
- **Performance changes**: Query patterns may need optimization
- **Connection failures**: Downstream systems need configuration updates

</TabItem>
</Tabs>

## Step 2: Downstream Mapping

Use DataHub's lineage to systematically map all affected systems:

### The Downstream Discovery Method

**Starting Point**: Your changing dataset (`customer_analytics_pipeline`)

**Discovery Process**:

1. **Open the dataset** in DataHub
2. **Navigate to Lineage tab**
3. **Switch to downstream view** (right side of lineage graph)
4. **Document each downstream connection**:

<div className="downstream-mapping">

**Downstream Impact Template**:

| System             | Type         | Business Impact             | Technical Owner  | Update Required     |
| ------------------ | ------------ | --------------------------- | ---------------- | ------------------- |
| Customer Dashboard | BI Tool      | High - Sales team daily use | @sarah-analytics | Connection string   |
| Weekly Reports     | Automated    | High - CEO visibility       | @john-reporting  | SQL query updates   |
| ML Pipeline        | Data Science | Medium - Model retraining   | @alex-ml         | Data source config  |
| Mobile API         | Application  | High - Customer app         | @dev-team        | Database connection |
| Data Warehouse     | Storage      | Low - Archive only          | @data-ops        | Monitoring updates  |

</div>

### Interactive Exercise: Downstream Mapping

<div className="interactive-exercise">

**Your Challenge**: Map downstream impact for TechFlow's user analytics

**Step 1**: Open `fct_users_created` in your DataHub instance
**Step 2**: Navigate to the Lineage tab
**Step 3**: Identify all downstream connections
**Step 4**: Fill out the impact template:

```
Downstream System 1: ___________________
Business Impact: _______________________
Technical Owner: _______________________
Update Required: _______________________

Downstream System 2: ___________________
Business Impact: _______________________
Technical Owner: _______________________
Update Required: _______________________
```

**Success Criteria**: You've identified at least 3 downstream systems and assessed their business impact.

</div>

## Step 3: Risk Assessment

Transform your downstream map into quantified risk scores:

### The Risk Scoring Matrix

<Tabs>
<TabItem value="business-impact" label="💼 Business Impact Scoring">

**Impact Scale (1-5)**:

- **5 - Critical**: Customer-facing, revenue-impacting, or regulatory
- **4 - High**: Executive reporting, key business processes
- **3 - Medium**: Team productivity, internal analytics
- **2 - Low**: Development tools, experimental systems
- **1 - Minimal**: Archive, backup, or deprecated systems

**Business Impact Factors**:

- **User Count**: How many people depend on this system?
- **Revenue Impact**: Does this directly affect sales or billing?
- **Compliance**: Are there regulatory or audit requirements?
- **Operational Criticality**: Is this needed for daily operations?

</TabItem>
<TabItem value="technical-complexity" label="⚙️ Technical Complexity">

**Complexity Scale (1-5)**:

- **5 - Very Complex**: Custom code, multiple integrations, legacy systems
- **4 - Complex**: Requires specialized knowledge, multiple teams
- **3 - Moderate**: Standard configurations, documented processes
- **2 - Simple**: Well-understood, single team ownership
- **1 - Trivial**: Automated, self-service, or minimal changes

**Technical Factors**:

- **Integration Complexity**: How many systems need updates?
- **Code Changes**: Are application changes required?
- **Testing Requirements**: How extensive is validation needed?
- **Rollback Difficulty**: How easy is it to undo changes?

</TabItem>
<TabItem value="risk-calculation" label="📊 Risk Calculation">

**Risk Score Formula**:

```
Risk Score = Business Impact × Technical Complexity × Urgency Factor
```

**Urgency Factors**:

- **1.5x**: Tight deadline (< 1 week)
- **1.2x**: Normal timeline (1-4 weeks)
- **1.0x**: Flexible timeline (> 1 month)

**Risk Categories**:

- **20-25**: 🔴 **Critical Risk** - Executive approval required
- **15-19**: 🟡 **High Risk** - Detailed mitigation plan needed
- **10-14**: 🟢 **Medium Risk** - Standard change process
- **5-9**: 🔵 **Low Risk** - Routine change management
- **1-4**: ⚪ **Minimal Risk** - Proceed with standard testing

</TabItem>
</Tabs>

### Risk Assessment Exercise

<div className="risk-assessment">

**TechFlow Customer Analytics Migration Risk Assessment**:

| Downstream System | Business Impact | Technical Complexity | Risk Score | Category    |
| ----------------- | --------------- | -------------------- | ---------- | ----------- |
| Sales Dashboard   | 5 (Critical)    | 3 (Moderate)         | 22.5       | 🔴 Critical |
| CEO Reports       | 4 (High)        | 2 (Simple)           | 12         | 🟢 Medium   |
| ML Pipeline       | 3 (Medium)      | 4 (Complex)          | 18         | 🟡 High     |
| Mobile API        | 5 (Critical)    | 3 (Moderate)         | 22.5       | 🔴 Critical |
| Archive System    | 1 (Minimal)     | 1 (Trivial)          | 1.5        | ⚪ Minimal  |

**Analysis**: 2 Critical Risk systems require executive approval and detailed rollback plans.

</div>

## Step 4: Stakeholder Analysis

Identify who needs to be involved in the change:

### Stakeholder Mapping Framework

<div className="stakeholder-framework">

**Stakeholder Categories**:

**🎯 Primary Stakeholders** (Directly affected):

- **Data Consumers**: Teams using the affected data
- **System Owners**: Technical teams responsible for downstream systems
- **Business Users**: People whose work depends on the data

**🤝 Secondary Stakeholders** (Coordination required):

- **Infrastructure Teams**: Platform and DevOps support
- **Security Teams**: Access control and compliance
- **Project Management**: Timeline and resource coordination

**📢 Communication Stakeholders** (Keep informed):

- **Executive Leadership**: High-level impact awareness
- **Customer Support**: Potential user impact preparation
- **Documentation Teams**: Update procedures and guides

</div>

### Communication Strategy

<Tabs>
<TabItem value="technical-communication" label="👨‍💻 Technical Teams">

**Technical Impact Report Template**:

```markdown
## System Change Impact: Customer Analytics Migration

### Technical Changes Required

- **Connection Updates**: Update database connection strings
- **Query Modifications**: Adapt SQL for Snowflake syntax
- **Testing Requirements**: Validate data accuracy and performance
- **Rollback Plan**: Revert connection strings if issues occur

### Timeline

- **Preparation**: This week - update configurations
- **Migration**: Weekend - 48-hour window
- **Validation**: Monday morning - verify all systems

### Support Contacts

- **Migration Lead**: @data-engineering-team
- **Emergency Contact**: @on-call-engineer
```

</TabItem>
<TabItem value="business-communication" label="👔 Business Stakeholders">

**Business Impact Summary Template**:

```markdown
## Customer Analytics Platform Upgrade

### What's Changing

We're upgrading our customer analytics platform to improve performance and reduce costs.

### Business Benefits

- **10x faster** customer reports and dashboards
- **$50K monthly savings** in infrastructure costs
- **Real-time insights** for better customer service

### What You Need to Know

- **When**: Next weekend (48-hour maintenance window)
- **Impact**: Brief downtime Saturday evening, normal service by Monday
- **Your Action**: No action required - all systems will work as before

### Questions?

Contact: @data-team or @project-manager
```

</TabItem>
<TabItem value="executive-summary" label="📊 Executive Summary">

**Executive Impact Brief Template**:

```markdown
## Executive Brief: Customer Analytics Migration

### Strategic Impact

- **Business Value**: $600K annual savings + 10x performance improvement
- **Risk Assessment**: 2 critical systems identified, mitigation plans in place
- **Timeline**: 48-hour weekend migration, normal operations by Monday

### Risk Mitigation

- **Rollback Plan**: 4-hour recovery time if issues occur
- **Testing Strategy**: Comprehensive validation before go-live
- **Support Coverage**: 24/7 engineering support during migration

### Success Metrics

- **Zero customer impact**: No service disruptions
- **Performance targets**: 10x improvement in dashboard load times
- **Cost savings**: $50K monthly reduction starting next month

### Approval Required

Proceed with migration: [ ] Yes [ ] No
Executive Sponsor: **\*\***\_\_\_\_**\*\***
```

</TabItem>
</Tabs>

## Step 5: Mitigation Planning

Develop comprehensive plans to minimize risk:

### The Mitigation Strategy Framework

<div className="mitigation-planning">

**Risk Mitigation Categories**:

**🛡️ Preventive Measures** (Avoid problems):

- **Comprehensive testing**: Validate all connections before go-live
- **Staged rollout**: Migrate non-critical systems first
- **Communication plan**: Ensure all stakeholders are prepared
- **Documentation updates**: Keep all procedures current

**🚨 Detective Measures** (Catch problems early):

- **Monitoring alerts**: Set up notifications for system failures
- **Health checks**: Automated validation of data flow
- **User feedback channels**: Quick reporting of issues
- **Performance monitoring**: Track system response times

**🔧 Corrective Measures** (Fix problems quickly):

- **Rollback procedures**: Detailed steps to revert changes
- **Emergency contacts**: 24/7 support team availability
- **Escalation paths**: Clear decision-making authority
- **Communication templates**: Pre-written status updates

</div>

### Rollback Strategy Development

**Critical Success Factor**: Every change needs a tested rollback plan.

<Tabs>
<TabItem value="rollback-planning" label="🔄 Rollback Planning">

**Rollback Decision Matrix**:

| Issue Type         | Rollback Trigger          | Recovery Time | Decision Authority    |
| ------------------ | ------------------------- | ------------- | --------------------- |
| Data Corruption    | Any data inconsistency    | 2 hours       | Data Engineering Lead |
| Performance Issues | >50% slower than baseline | 4 hours       | Technical Manager     |
| System Failures    | Any critical system down  | 1 hour        | On-call Engineer      |
| User Complaints    | >10 user reports          | 6 hours       | Product Manager       |

**Rollback Procedure Template**:

```bash
# Emergency Rollback: Customer Analytics Migration
# Decision Authority: [Name] [Contact]
# Estimated Time: 4 hours

1. Stop new data processing
2. Revert connection strings to original Hive system
3. Restart downstream applications
4. Validate data flow restoration
5. Notify stakeholders of rollback completion
```

</TabItem>
<TabItem value="testing-strategy" label="🧪 Testing Strategy">

**Pre-Migration Testing Checklist**:

**Data Validation**:

- [ ] Row counts match between old and new systems
- [ ] Sample data comparison (10% random sample)
- [ ] Schema validation (all columns present and correct types)
- [ ] Data freshness verification (latest timestamps match)

**System Integration Testing**:

- [ ] All downstream connections work with new system
- [ ] Query performance meets or exceeds baseline
- [ ] Authentication and authorization function correctly
- [ ] Monitoring and alerting systems recognize new platform

**User Acceptance Testing**:

- [ ] Key dashboards load correctly with new data source
- [ ] Reports generate successfully with expected data
- [ ] API endpoints return correct responses
- [ ] Mobile app functions normally with new backend

</TabItem>
<TabItem value="success-metrics" label="📈 Success Metrics">

**Migration Success Criteria**:

**Technical Metrics**:

- **Zero data loss**: 100% data integrity maintained
- **Performance improvement**: >5x faster query response times
- **Uptime target**: 99.9% availability during migration
- **Error rate**: <0.1% failed requests

**Business Metrics**:

- **User satisfaction**: <5 user complaints about system changes
- **Productivity impact**: No measurable decrease in team efficiency
- **Cost savings**: Achieve projected $50K monthly reduction
- **Timeline adherence**: Complete migration within 48-hour window

**Validation Timeline**:

- **Immediate** (0-4 hours): System connectivity and basic functionality
- **Short-term** (1-7 days): Performance validation and user feedback
- **Medium-term** (1-4 weeks): Cost savings realization and stability
- **Long-term** (1-3 months): Full business value achievement

</TabItem>
</Tabs>

## Real-World Impact Analysis Exercise

<div className="real-world-exercise">

**Your Challenge**: Perform a complete impact analysis for a system change

**Scenario**: TechFlow wants to add a new `customer_lifetime_value` column to the `customers` table. This requires updating the ETL job and potentially affects all downstream systems.

**Your Task**: Complete the 5-step impact analysis:

**Step 1 - Scope Definition**:

```
System/Dataset: customers table
Change Type: Schema addition (new column)
Timeline: 2-week implementation
Technical Details: Add CLV calculation to nightly ETL
Business Justification: Enable customer segmentation for marketing
```

**Step 2 - Downstream Mapping**:
Use DataHub to identify all systems that consume the `customers` table and document them.

**Step 3 - Risk Assessment**:
Score each downstream system using the Business Impact × Technical Complexity formula.

**Step 4 - Stakeholder Analysis**:
Identify who needs to be involved and create appropriate communication plans.

**Step 5 - Mitigation Planning**:
Develop testing strategy and rollback procedures.

**Success Criteria**: You've created a comprehensive impact analysis that could be presented to stakeholders for approval.

</div>

## Success Checkpoint

<div className="checkpoint">

**You've mastered impact analysis when you can:**

**Planning Skills**:

- Complete the 5-step impact analysis framework for any system change
- Quantify risk using business impact and technical complexity scores
- Create stakeholder-appropriate communication plans
- Develop comprehensive rollback strategies

**Analysis Skills**:

- Map downstream dependencies using DataHub lineage
- Assess business impact across different user types and use cases
- Identify critical path dependencies and single points of failure
- Prioritize changes based on risk scores and business value

**Communication Skills**:

- Present technical impact to business stakeholders clearly
- Create executive summaries that enable informed decision-making
- Coordinate cross-team changes using lineage insights
- Document mitigation plans that teams can execute confidently

**Final Validation**:
Choose a real system change in your organization and perform a complete impact analysis using the framework you've learned.

</div>

## What You've Accomplished

🎉 **Outstanding work!** You've transformed from basic lineage viewing to expert-level impact analysis:

- **Systematic approach**: You can now analyze any system change methodically
- **Risk quantification**: You understand how to score and prioritize risks
- **Stakeholder management**: You can communicate impact to any audience
- **Mitigation planning**: You're prepared for both success and failure scenarios

:::tip Mark Your Progress
Check off "Performing Impact Analysis" in the progress tracker above! You're ready to troubleshoot lineage issues.
:::

---

**Next**: Complete your lineage mastery by learning [lineage troubleshooting techniques](troubleshooting.md) →
