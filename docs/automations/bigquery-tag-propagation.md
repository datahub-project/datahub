import FeatureAvailability from '@site/src/components/FeatureAvailability';

# BigQuery Propagation Automation

<FeatureAvailability saasOnly />

## Introduction

BigQuery Propagation is an automation that allows you to sync DataHub Tags, Table and Column descriptions, and Column Glossary Terms back to BigQuery. This automation is available in DataHub Cloud (Acryl) only.

## Capabilities

- Automatically Add DataHub Tags as BigQuery Labels to tables
- Automatically Add DataHub Table descriptions to BigQuery Tables
- Automatically Add DataHub Column descriptions to BigQuery Columns
- Automatically Add DataHub Glossary Terms as Policy Tags to BigQuery Columns
- Automatically remove Policy Tags/Table Labels when they are removed in DataHub

## Enabling BigQuery Sync

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

<p align="left">
  <img width="20%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
</p>

2. **Create An Automation**: Click on 'Create' and select 'BigQuery Tag Propagation'.

<p align="left">
  <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/automation-type.png"/>
</p>

3. **Configure Automation**:

    1. **Select a Propagation Action**

    <p align="left">
      <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/automation-form.png"/>
    </p>

    | Propagation Type | DataHub Entity | BigQuery Entity | Note |
    | -------- | ------- | ------- | ------- |
    | Table Tags as Labels | [Table Tag](https://datahubproject.io/docs/tags/) | [BigQuery Label](https://cloud.google.com/bigquery/docs/labels-intro) | - |
    | Column Glossary Terms as Policy Tags | [Glossary Term on Table Column](https://datahubproject.io/docs/0.14.0/glossary/business-glossary/) | [Policy Tag](https://cloud.google.com/bigquery/docs/best-practices-policy-tags) | <ul><li>Assigned Policy tags are created under DataHub taxonomy.</li></ul><ul><li>Only the latest assigned glossary term set as policy tag. BigQuery only supports one assigned policy tag.</li></ul> <ul><li>Policy Tags are not synced to DataHub as glossary term from BigQuery.</li></ul>
    | Table Descriptions | [Table Description](https://datahubproject.io/docs/api/tutorials/descriptions/) | Table Description | - |
    | Column Descriptions | [Column Description](https://datahubproject.io/docs/api/tutorials/descriptions/) | Column Description | - |

    :::note

    You can limit propagation based on specific Tags and Glossary Terms. If none are selected, then ALL Tags or Glossary Terms will be automatically propagated to BigQuery tables and columns. (The recommended approach is to not specify a filter to avoid inconsistent states.)

    :::

    :::note

    You can only have one Policy Tag on a table field in BigQuery. Therefore, the last set Glossary Term will be the policy tag on a field.

    :::

    2. **Fill in the required fields to connect to BigQuery, along with the name, description, and category**

    <p align="left">
      <img width="30%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/connection_config.png"/>
    </p>

    3. **Finally, click 'Save and Run' to start the automation**

## Propagating for Existing Assets

To ensure that all existing table Tags and Column Glossary Terms are propagated to BigQuery, you can back-fill historical data for existing assets. Note that the initial back-filling process may take some time, depending on the number of BigQuery assets you have.

To do so, follow these steps:

1. Navigate to the Automation you created in Step 3 above
2. Click the 3-dot "More" menu

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

3. Click "Initialize"

<p align="left">
  <img width="15%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

This one-time step will kick off the back-filling process for existing descriptions. If you only want to begin propagating descriptions going forward, you can skip this step.

## Viewing Propagated Tags

You can view propagated Tags (and corresponding DataHub URNs) inside the BigQuery UI to confirm the automation is working as expected.

<p align="left">
  <img width="50%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/bigquery-propagation/labels.png"/>
</p>
