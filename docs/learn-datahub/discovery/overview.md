import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';
import DataHubEntityCard from '@site/src/components/DataHubEntityCard';
import DataHubLineageNode, { DataHubLineageFlow } from '@site/src/components/DataHubLineageNode';
import TutorialProgress from '@site/src/components/TutorialProgress';

# Data Discovery & Search (45 minutes)

<TutorialProgress
tutorialId="discovery"
steps={[
{ title: "Advanced Search Techniques", time: "15 min", description: "Master operators, filters, and saved searches" },
{ title: "Understanding Dataset Profiles", time: "20 min", description: "Interpret profiles, statistics, and data quality" },
{ title: "Collaborative Discovery", time: "10 min", description: "Document, tag, and share knowledge effectively" }
]}
/>

:::tip Prerequisites
Complete the [DataHub Quickstart](../quickstart/overview.md) tutorial first to have DataHub running with sample data.
:::

## What You'll Master

Transform from basic DataHub user to discovery expert by mastering advanced search techniques, understanding dataset profiles, and leveraging collaborative features.

**Learning Outcomes:**

- **Advanced Search Mastery**: Use operators, filters, and saved searches like a pro
- **Dataset Intelligence**: Read and interpret automatically generated data profiles
- **Collaborative Discovery**: Leverage social features to crowdsource data knowledge
- **Search Strategy**: Develop systematic approaches for different discovery scenarios

**Enterprise Data Discovery Framework:**

<DataHubLineageFlow {...{
title: "5-Step Professional Discovery Process",
nodes: [
{
name: 'define_objectives',
type: 'Requirements',
entityType: 'Dataset',
platform: 'Business',
health: 'Good',
columns: [
{ name: 'business_question', type: 'string' },
{ name: 'analysis_scope', type: 'string' },
{ name: 'success_criteria', type: 'string' }
],
tags: ['Requirements', 'Planning'],
glossaryTerms: ['Business Requirement', 'Analysis Objective']
},
{
name: 'search_strategy',
type: 'Discovery',
entityType: 'DataJob',
platform: 'DataHub',
health: 'Good',
tags: ['Search', 'Keywords']
},
{
name: 'filter_results',
type: 'Filtering',
entityType: 'Dataset',
platform: 'DataHub',
health: 'Good',
columns: [
{ name: 'platform_filter', type: 'string' },
{ name: 'domain_filter', type: 'string' },
{ name: 'entity_type', type: 'string' }
],
tags: ['Filtering', 'Refinement'],
glossaryTerms: ['Search Filters']
},
{
name: 'evaluate_quality',
type: 'Assessment',
entityType: 'Dataset',
platform: 'DataHub',
health: 'Warning',
columns: [
{ name: 'data_freshness', type: 'timestamp' },
{ name: 'schema_match', type: 'boolean' },
{ name: 'lineage_depth', type: 'number' }
],
tags: ['Quality-Check', 'Validation'],
glossaryTerms: ['Data Quality', 'Asset Evaluation']
},
{
name: 'plan_access',
type: 'Implementation',
entityType: 'Dataset',
platform: 'Enterprise',
health: 'Good',
columns: [
{ name: 'permissions', type: 'string' },
{ name: 'connection_info', type: 'string' },
{ name: 'usage_patterns', type: 'string' }
],
tags: ['Access-Control', 'Integration'],
glossaryTerms: ['Access Planning', 'Data Integration']
}
]
}} />

**Discovery Navigation Strategy**:

1. **Start with Business Need** (requirements gathering)
2. **Apply Search Strategy** (targeted discovery)
3. **Filter and Refine**: What platforms, what domains?
   - Platform filters → Focus on relevant data systems
   - Domain filters → Narrow to business area
   - Entity type → Tables, dashboards, or pipelines
4. **Evaluate Data Quality**: Is this the right data?
   - Check data freshness and update patterns
   - Review schema compatibility with analysis needs
   - Assess lineage depth and data reliability
5. **Plan Integration**: How to access and use
   - Verify permissions and access controls
   - Gather connection details and usage patterns
   - Check tags and glossary terms for context

**Professional Approach**: This 5-step discovery method mirrors the systematic approach used in lineage analysis - ensuring you find the right data efficiently while understanding its full context.

## Interactive Tutorial Structure

This hands-on tutorial uses **real search scenarios** you'll encounter daily:

<div className="tutorial-progress">

| Step | Scenario                                              | Time   | Interactive Elements                      |
| ---- | ----------------------------------------------------- | ------ | ----------------------------------------- |
| 1    | [Advanced Search Techniques](advanced-search.md)      | 15 min | Live search examples, Interactive filters |
| 2    | [Understanding Dataset Profiles](dataset-profiles.md) | 20 min | Profile interpretation, Quality analysis  |
| 3    | [Collaborative Discovery](collaborative-discovery.md) | 10 min | Documentation exercises, Tagging practice |

</div>

**Total Time: 45 minutes**

## Professional Discovery Scenarios

Throughout this tutorial, you'll work through practical scenarios:

:::info Scenario 1: Targeted Data Discovery
**Use Case**: Locate customer segmentation data for a marketing campaign without knowing exact table names.

**Focus Areas**: Exploratory search, filtering, schema analysis
:::

:::info Scenario 2: Root Cause Analysis  
**Use Case**: Investigate suspicious dashboard numbers by tracing data lineage to identify the source of the issue.

**Focus Areas**: Lineage navigation, data quality assessment, root cause analysis
:::

:::info Scenario 3: Knowledge Sharing at Scale
**Use Case**: Document insights and context so others can reliably discover and apply the dataset.

**Focus Areas**: Documentation, tagging, collaborative features
:::

## Interactive Learning Features

This tutorial leverages Docusaurus's interactive capabilities:

<Tabs>
<TabItem value="hands-on" label="Hands-On Exercises">

**Live Search Practice**: Try real searches in your DataHub instance
**Interactive Filters**: Step-by-step filter application
**Profile Analysis**: Guided interpretation of data statistics
**Collaboration Simulation**: Practice documentation and tagging

</TabItem>
<TabItem value="checkpoints" label="Progress Checkpoints">

**Knowledge Checks**: Quick quizzes to verify understanding
**Practical Validation**: Confirm you can perform key tasks
**Scenario Completion**: Solve real discovery challenges
**Skill Assessment**: Rate your confidence with each technique

</TabItem>
<TabItem value="resources" label="Learning Resources">

**Cheat Sheets**: Quick reference for search operators
**Best Practices**: Pro tips from experienced users
**Troubleshooting**: Common issues and solutions
**Advanced Techniques**: Power user shortcuts

</TabItem>
</Tabs>

## Prerequisites Check

Before starting, ensure you have:

<div className="checklist">

- [ ] **DataHub running locally** at http://localhost:9002
- [ ] **Sample data ingested** (from quickstart tutorial)
- [ ] **Basic familiarity** with DataHub navigation
- [ ] **15 minutes** of focused time per section

</div>

:::tip Quick Setup Verification
Test your setup by searching for "customer" in DataHub. You should see several results from the sample data.
:::

## Learning Path Integration

**Coming from:** [DataHub Quickstart](../quickstart/overview.md) - You understand basic navigation and have sample data

**Going to:** Choose your next path based on your role:

- **Data Engineers**: [Data Ingestion Mastery](../ingestion/overview.md)
- **Analysts**: [Data Lineage & Impact Analysis](../lineage/overview.md)
- **Governance Teams**: [Data Governance Fundamentals](../governance/overview.md)

## Success Metrics

By the end of this tutorial, you'll be able to:

<div className="success-metrics">

**Speed**: Find relevant datasets in under 2 minutes
**Accuracy**: Identify the right data source for your analysis needs  
**Insight**: Quickly assess data quality and freshness
**Collaboration**: Effectively document and share data knowledge

</div>

## Interactive Demo Preview

Here's a taste of what you'll learn:

<Tabs>
<TabItem value="basic-search" label="Basic Search">

```
Search: "customer"
Results: 47 datasets found
```

</TabItem>
<TabItem value="advanced-search" label="Advanced Search">

```
Search: name:customer* AND platform:postgres AND hasOwners:true
Results: 3 highly relevant datasets found
Filters: PostgreSQL, Has Documentation, Updated Last 7 Days
```

</TabItem>
<TabItem value="expert-search" label="Expert Search">

```
Search: (customer OR user) AND (segment* OR cohort*) AND NOT test*
Saved Search: "Customer Segmentation Data"
Smart Filters: Production Only, High Quality, Well Documented
Results: 1 perfect match found in 15 seconds
```

</TabItem>
</Tabs>

---

**Ready to become a DataHub discovery expert?** Let's start with [Advanced Search Techniques](advanced-search.md) →
