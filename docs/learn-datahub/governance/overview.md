import DataHubEntityCard from '@site/src/components/DataHubEntityCard';
import DataHubLineageNode, { DataHubLineageFlow } from '@site/src/components/DataHubLineageNode';

# Data Governance Fundamentals

<TutorialProgress
tutorialId="governance"
currentStep="governance-overview"
steps={[
{ id: 'governance-overview', title: 'Overview', time: '5 min', description: 'Professional data governance journey introduction' },
{ id: 'ownership-management', title: 'Ownership Management', time: '12 min', description: 'Establish clear data ownership and accountability' },
{ id: 'data-classification', title: 'Data Classification', time: '15 min', description: 'Implement PII detection and sensitivity labeling' },
{ id: 'business-glossary', title: 'Business Glossary', time: '12 min', description: 'Create standardized business terminology' },
{ id: 'governance-policies', title: 'Governance Policies', time: '11 min', description: 'Automate governance enforcement at scale' }
]}
/>

## Professional Data Governance Journey

**Time Required**: 50 minutes | **Skill Level**: Intermediate

### Your Challenge: Establishing Data Governance at Scale

You're a **Data Governance Lead** at a growing technology company. Your organization has hundreds of datasets across multiple platforms, but lacks consistent ownership, classification, and business context. Leadership wants to implement proper data governance to ensure compliance, reduce risk, and improve data quality.

**The Business Impact**: Without proper governance, your company faces:

- **Compliance Risks**: Inability to track PII and sensitive data
- **Data Quality Issues**: No clear ownership for data problems
- **Business Confusion**: Teams can't understand what data means
- **Operational Inefficiency**: Time wasted searching for the right data

### What You'll Learn

This tutorial series walks you through implementing comprehensive data governance using DataHub's governance features:

#### Chapter 1: Ownership Management (12 minutes)

**Business Challenge**: No clear accountability for data quality and maintenance
**Your Journey**:

- Assign technical and business owners to critical datasets
- Set up ownership notifications and responsibilities
- Create ownership hierarchies for different data domains
  **Organizational Outcome**: Clear accountability and faster issue resolution

#### Chapter 2: Data Classification (15 minutes)

**Business Challenge**: Sensitive data scattered across systems without proper labeling
**Your Journey**:

- Implement PII detection and classification
- Apply sensitivity labels (Public, Internal, Confidential, Restricted)
- Set up automated classification rules
  **Organizational Outcome**: Compliance readiness and risk reduction

#### Chapter 3: Business Glossary (12 minutes)

**Business Challenge**: Business terms used inconsistently across teams and systems
**Your Journey**:

- Create standardized business definitions
- Link glossary terms to datasets and columns
- Establish term hierarchies and relationships
  **Organizational Outcome**: Consistent business language and improved data understanding

#### Chapter 4: Governance Policies (11 minutes)

**Business Challenge**: Manual governance processes that don't scale
**Your Journey**:

- Set up automated governance policies
- Configure approval workflows for sensitive data
- Implement data access controls and monitoring
  **Organizational Outcome**: Scalable governance that grows with your organization

### DataHub Governance in Action

See how proper governance transforms your data assets from unmanaged to enterprise-ready:

<div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '16px', margin: '20px 0'}}>
  <DataHubEntityCard 
    name="customer_profiles"
    type="Table"
    platform="Snowflake"
    description="Customer personal information with comprehensive governance controls"
    owners={[
      { name: 'Sarah Chen', type: 'Business Owner' },
      { name: 'Mike Rodriguez', type: 'Technical Owner' }
    ]}
    tags={['PII', 'Customer-Data', 'Confidential']}
    glossaryTerms={['Customer Profile', 'Personal Information']}
    health="Good"
  />
  
  <DataHubEntityCard 
    name="financial_transactions"
    type="Table"
    platform="PostgreSQL"
    description="Payment and transaction data with strict access controls"
    owners={[
      { name: 'Finance Team', type: 'Business Owner' },
      { name: 'Data Engineering', type: 'Technical Owner' }
    ]}
    tags={['Financial', 'Restricted', 'Audit-Required']}
    glossaryTerms={['Financial Transaction', 'Payment Data']}
    health="Good"
  />
</div>

**Governance Benefits Demonstrated**:

- **Clear Ownership**: Every dataset has assigned business and technical owners
- **Proper Classification**: Tags indicate sensitivity levels and compliance requirements
- **Business Context**: Glossary terms provide standardized definitions
- **Quality Assurance**: Health indicators show data reliability

### Governance in Practice: End-to-End Data Flow

See how governance controls are applied throughout a complete data pipeline:

<DataHubLineageFlow {...{
title: "Enterprise Governance Pipeline",
nodes: [
{
name: 'raw_customer_data',
type: 'Table',
entityType: 'Dataset',
platform: 'Postgres',
health: 'Good',
tags: ['PII', 'Source', 'Restricted'],
glossaryTerms: ['Customer Data', 'Personal Information']
},
{
name: 'data_validation',
type: 'Job',
entityType: 'DataJob',
platform: 'Airflow',
health: 'Good',
tags: ['Quality-Check', 'Governance']
},
{
name: 'customer_analytics',
type: 'Table',
entityType: 'Dataset',
platform: 'Snowflake',
health: 'Good',
tags: ['Analytics', 'Confidential', 'Governed'],
glossaryTerms: ['Customer Analytics', 'Business Metrics']
},
{
name: 'executive_dashboard',
type: 'Dashboard',
entityType: 'Dataset',
platform: 'Tableau',
health: 'Good',
tags: ['Executive', 'Public', 'Approved'],
glossaryTerms: ['Executive Dashboard']
}
]
}} />

**Governance Flow Analysis**:

- **Source Control**: Raw data properly classified as PII/Restricted with clear ownership
- **Processing Governance**: Validation jobs ensure quality and compliance during transformation
- **Output Classification**: Analytics data appropriately tagged and documented for business use
- **Access Control**: Executive dashboards have appropriate sensitivity levels for broad access

### Interactive Learning Experience

Each chapter includes:

- **Real Governance Scenarios**: Based on actual enterprise challenges
- **Hands-on Exercises**: Using DataHub's sample data and governance features
- **Best Practice Guidance**: Industry-standard approaches to data governance
- **Measurable Outcomes**: Clear success metrics for each governance initiative

### Prerequisites

- Completed [DataHub Quickstart](../quickstart/overview.md)
- Basic understanding of data management concepts
- Access to DataHub instance with sample data

### Ready to Begin?

Start your data governance journey by establishing clear ownership and accountability for your organization's data assets.

<NextStepButton href="./ownership-management.md" />
