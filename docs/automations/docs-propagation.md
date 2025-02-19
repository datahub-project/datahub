# Documentation Propagation Automation

:::info

This feature is currently in open beta in Acryl Cloud. Reach out to your Acryl representative to get access.

:::

## Introduction

Documentation Propagation is an automation automatically propagates column and asset (coming soon) descriptions based on downstream column-level lineage and sibling relationships. 
It simplifies metadata management by ensuring consistency and reducing the manual effort required for documenting data assets to aid
in Data Governance & Compliance along with Data Discovery. 

This feature is enabled by default in Open Source DataHub.

## Capabilities

### Open Source
- **Column-Level Docs Propagation**: Automatically propagate documentation to downstream columns and sibling columns that are derived or dependent on the source column.
- **(Coming Soon) Asset-Level Docs Propagation**: Propagate descriptions to sibling assets. 

### DataHub Cloud (Acryl)
- Includes all the features of Open Source.
- **Propagation Rollback (Undo)**: Offers the ability to undo any propagation changes, providing a safety net against accidental updates.
- **Historical Backfilling**: Automatically backfills historical data for newly documented columns to maintain consistency across time.

### Comparison of Features

| Feature                         | Open Source | DataHub Cloud |
|---------------------------------|-------------|---------------|
| Column-Level Docs Propagation   | ✔️           | ✔️             |
| Asset-Level Docs Propagation    | ✔️           | ✔️             |
| Downstream Lineage + Siblings   | ✔️           | ✔️             |
| Historical Backfilling          | ❌           | ✔️             |

## Enabling Documentation Propagation

### In Open Source

Notice that the user must have the `Manage Ingestion` permission to view and enable the feature.

1. **Navigate to Settings**: Click on the 'Settings' gear in top navigation bar.

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/oss/settings-nav-link.png"/>
</p>

2. **Navigate to Features**: Click on the 'Features' tab in the left-hand navigation bar.

<p align="left">
  <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/oss/features-settings-link.png"/>
</p>

3. **Enable Documentation Propagation**: Locate the 'Documentation Propagation' section and toggle the feature to enable it for column-level and asset-level propagation. 
Currently, Column Level propagation is supported, with asset level propagation coming soon. 

<p align="left">
  <img width="40%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/oss/docs-propagation/feature-flags.png"/>
</p>


### In DataHub Cloud

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

<p align="left">
  <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
</p>

2. **Create An Automation**: Click on 'Create' and select 'Column Documentation Propagation'.

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/docs-propagation/automation-type.png"/>
</p>

3. **Configure Automation**: Fill in the required fields, such as the name, description, and category. Finally, click 'Save and Run' to start the automation

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/docs-propagation/automation-form.png"/>
</p>

## Propagating for Existing Assets (DataHub Cloud Only)

In DataHub Cloud, you can back-fill historical data for existing assets to ensure that all existing column descriptions are propagated to downstreams
when you start the automation. Note that it may take some time to complete the initial back-filling process, depending on the number of assets and the complexity of your lineage.

To do this, navigate to the Automation you created in Step 3 above, click the 3-dot "more" menu:

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

and then click "Initialize".

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

This one-time step will kick off the back-filling process for existing descriptions. If you only want to begin propagating
descriptions going forward, you can skip this step. 

## Viewing Propagated Descriptions

Once the automation is enabled, you'll be able to recognize propagated descriptions as those with the thunderbolt icon next to them:

The tooltip will provide additional information, including where the description originated and any intermediate hops that were 
used to propagate the description.

<p align="left">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/oss/docs-propagation/view-propagated-docs.png"/>
</p>