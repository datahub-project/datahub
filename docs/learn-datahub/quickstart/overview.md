import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TutorialProgress from '@site/src/components/TutorialProgress';
import DataHubLineageNode, { DataHubLineageFlow } from '@site/src/components/DataHubLineageNode';

# Chapter 1: DataHub Foundation (30 minutes)

:::tip Professional Development Journey
This tutorial follows realistic challenges that data professionals face when implementing metadata management in production environments.
:::

## The Business Challenge

**Your Role**: You're a data professional tasked with implementing metadata management for a growing technology organization. Data is distributed across multiple systems without centralized discovery or governance.

**The Business Challenge**: Executive leadership requires user engagement metrics for strategic decision-making. The data exists across various systems, but there's no efficient way to locate, validate, and understand data relationships.

**What You'll Accomplish**:

- **Deploy DataHub** as the central metadata management platform
- **Connect enterprise data systems** across streaming, analytics, and storage platforms
- **Implement systematic data discovery** to reduce time-to-insight
- **Establish data lineage tracking** for quality assurance and impact analysis

**Business Outcome**: Enable self-service data discovery while establishing enterprise-grade metadata governance.

## Tutorial Structure

This tutorial is designed to be completed in sequence. **Track your progress** as you go:

<TutorialProgress
tutorialId="quickstart"
currentStep={-1}
steps={[
{
title: "Setup DataHub",
time: "5 min",
description: "Deploy DataHub locally using Docker"
},
{
title: "First Data Ingestion",
time: "10 min",
description: "Connect sample data systems and ingest metadata"
},
{
title: "Discovery Basics",
time: "10 min",
description: "Locate business-critical metrics using systematic discovery"
},
{
title: "Your First Lineage",
time: "5 min",
description: "Trace data flow and understand dependencies"
}
]}
/>

**Total Time: 30 minutes** | **Your Progress: Tracked above**

## Prerequisites

Before starting, ensure you have:

- **Docker Desktop** installed and running
- **Python 3.9+** installed
- **Basic familiarity** with databases and data concepts
- **15 minutes** of uninterrupted time

## The Business Scenario

**Organizational Context**: You're implementing DataHub for a technology company experiencing rapid growth. Data teams are struggling with:

- **Discovery bottlenecks**: Analysts spend 60% of their time finding relevant data
- **Quality uncertainty**: No systematic way to validate data reliability
- **Compliance gaps**: Difficulty tracking data lineage for regulatory requirements
- **Knowledge silos**: Critical data knowledge trapped with individual team members

**Your Implementation Goal**: Establish DataHub as the central metadata platform to solve these enterprise challenges.

**Enterprise Data Architecture**: You'll work with a realistic multi-platform data ecosystem:

- **Analytics Layer**: User behavior metrics and business KPIs
- **Streaming Platform**: Real-time event data from Kafka
- **Data Warehouse**: Processed analytical datasets in Hive
- **Data Lake**: Raw data storage in HDFS

**Why This Matters**: This architecture represents common enterprise patterns where data teams need centralized metadata management to maintain productivity and compliance.

### DataHub Integration Architecture

DataHub acts as the central metadata hub connecting your entire data ecosystem:

<DataHubLineageFlow {...{
title: "Enterprise DataHub Architecture",
nodes: [
{
name: 'source_systems',
type: 'Sources',
entityType: 'Dataset',
platform: 'Multi-Platform',
health: 'Good',
columns: [
{ name: 'cloud_databases', type: 'string' },
{ name: 'data_warehouses', type: 'string' },
{ name: 'streaming_platforms', type: 'string' },
{ name: 'data_lakes', type: 'string' }
],
tags: ['Source-Systems', 'Enterprise'],
glossaryTerms: ['Data Sources']
},
{
name: 'datahub_core',
type: 'Platform',
entityType: 'DataJob',
platform: 'DataHub',
health: 'Good',
tags: ['Metadata-Hub', 'Core-Platform']
},
{
name: 'user_interfaces',
type: 'Applications',
entityType: 'Dataset',
platform: 'DataHub',
health: 'Good',
columns: [
{ name: 'web_application', type: 'string' },
{ name: 'cli_tools', type: 'string' },
{ name: 'api_access', type: 'string' },
{ name: 'integrations', type: 'string' }
],
tags: ['User-Interface', 'Access-Layer'],
glossaryTerms: ['User Access']
}
]
}} />

**Key Integration Points**:

- **Automated Discovery**: DataHub connectors extract metadata from your existing systems
- **Unified View**: All metadata is standardized and searchable through a single interface
- **Real-time Updates**: Changes in source systems are reflected immediately in DataHub
- **API Access**: Programmatic access enables integration with your existing workflows

## Learning Outcomes

After completing this tutorial, you'll be able to:

- **Deploy DataHub** in a local development environment
- **Connect data sources** and understand ingestion concepts
- **Find datasets** using DataHub's search and discovery features
- **Read data lineage** to understand data dependencies
- **Navigate the DataHub UI** confidently for daily data work

## What's Next?

This tutorial provides the foundation for more advanced DataHub concepts. After completion, consider exploring:

- **[Data Discovery & Search](../discovery/overview.md)** - Master advanced search techniques
- **[Data Lineage & Impact Analysis](../lineage/overview.md)** - Deep dive into lineage and impact analysis
- **Data Governance Fundamentals (coming soon)** - Learn ownership, classification, and glossaries

## Need Help?

If you encounter issues during this tutorial:

- Check the [Troubleshooting Guide](../../troubleshooting/quickstart.md)
- Visit the [DataHub Slack Community](https://datahub.com/slack)
- Review the [Full Quickstart Documentation](../../quickstart.md)

---

**Ready to get started?** Let's begin with [Setting up DataHub](setup.md) →
