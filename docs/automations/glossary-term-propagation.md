# Glossary Term Propagation Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in open beta in Acryl Cloud. Reach out to your Acryl representative to get access.

:::

## Introduction

Glossary Term Propagation is an automation feature that propagates classification labels (Glossary Terms) across column and assets based on downstream lineage and sibling relationships.
This automation simplifies metadata management by ensuring consistent term classification and reducing manual effort in categorizing data assets, aiding Data Governance & Compliance, and enhancing Data Discovery.

## Capabilities

- **Column-Level Glossary Term Propagation**: Automatically propagate Glossary Terms to all downstream lineage columns and sibling columns.
- **Asset-Level Glossary Term Propagation**: Automatically propagate Glossary Terms to all downstream lineage assets & sibling assets.
- **Select Terms & Term Groups**: Select specific Glossary Terms & Term Groups to propagate, e.g. to propagate only sensitive or important labels. 

Note that Asset-level propagation is currently only support for **Datasets** (Tables, Views, Topics, etc), and not for other asset types including
Charts, Dashboards, Data Pipelines, Data Tasks.

## Enabling Glossary Term Propagation

1. **Navigate to Automations**: Go to 'Govern' > 'Automations' in the navigation bar.

<p align="center">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
</p>


2. **Create An Automation**: Select 'Glossary Term Propagation' from the automation types.

<p align="center">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/glossary-term-propagation/automation-type.png"/>
</p>

3. **Configure Automation**: Complete the required fields and select 'Save and Run' to activate the automation.

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/glossary-term-propagation/automation-form.png"/>
</p>

## Propagating for Existing Assets

In DataHub Cloud, you can back-fill historical data to ensure existing Glossary Terms are consistently propagated across downstream relationships. To begin, access the Automation created in Step 3, click the 3-dot "more" menu, and choose "Initialize." This will kick off the backfill process.

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

and then click "Initialize".

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>


## Viewing Propagated Glossary Terms

Once enabled, propagated Glossary Terms will display a thunderbolt icon, indicating the origin of the term and any intermediate lineage hops used in propagation.

<p align="center">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/glossary-term-propagation/view-propagated-terms.png"/>
</p>