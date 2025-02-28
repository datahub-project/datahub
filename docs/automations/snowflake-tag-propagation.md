import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Snowflake Tag Propagation Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in open beta in Acryl Cloud. Reach out to your Acryl representative to get access.

:::


## Introduction

Snowflake Tag Propagation is an automation that allows you to sync DataHub Glossary Terms and Tags on
both columns and tables back to Snowflake. This automation is available in DataHub Cloud (Acryl) only.

## Capabilities

- Automatically Add DataHub Glossary Terms to Snowflake Tables and Columns
- Automatically Add DataHub Tags to Snowflake Tables and Columns
- Automatically Remove DataHub Glossary Terms and Tags from Snowflake Tables and Columns when they are removed in DataHub

## Prerequisites

### Permissions Required for Tag Management

- `CREATE TAG`: Required to create new tags in Snowflake.
Ensure the user or role has this privilege on the specific schema or database where tags will be created.
- `APPLY TAG`: Required to assign tags to Snowflake objects such as tables, columns, or other database objects.
This permission must be granted at the database, schema, or object level depending on the scope.


### Permissions Required for Object Access

- `USAGE` on the database and schema: Allows access to the database and schema to view and apply changes.
- `SELECT` on the objects (tables, views, etc.): Enables the automation to read metadata and verify existing tags.

### Example Permission Grant Statements

To grant the necessary permissions for a specific role (DATAHUB_AUTOMATION_ROLE), you can use the following SQL commands:

```sql
-- Tag management permissions
GRANT CREATE TAG ON SCHEMA your_database.your_schema TO ROLE DATAHUB_AUTOMATION_ROLE;
GRANT APPLY TAG ON SCHEMA your_database.your_schema TO ROLE DATAHUB_AUTOMATION_ROLE;

-- Object access for metadata operations
GRANT USAGE ON DATABASE your_database TO ROLE DATAHUB_AUTOMATION_ROLE;
GRANT USAGE ON SCHEMA your_database.your_schema TO ROLE DATAHUB_AUTOMATION_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA your_database.your_schema TO ROLE DATAHUB_AUTOMATION_ROLE;

-- Future privileges for tagging
GRANT SELECT ON FUTURE TABLES IN SCHEMA your_database.your_schema TO ROLE DATAHUB_AUTOMATION_ROLE;
GRANT APPLY TAG ON FUTURE TABLES IN SCHEMA your_database.your_schema TO ROLE DATAHUB_AUTOMATION_ROLE;
```


## Enabling Snowflake Tag Sync

1. **Navigate to Automations**: Click on 'Govern' > 'Automations' in the navigation bar.

<p align="center">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png"/>
</p>

2. **Create An Automation**: Click on 'Create' and select 'Snowflake Tag Propagation'.

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/automation-type.png"/>
</p>

3. **Configure Automation**: Fill in the required fields to connect to Snowflake, along with the name, description, and category.
   Note that you can limit propagation based on specific Tags and Glossary Terms. If none are selected, then ALL Tags or Glossary Terms will be automatically
   propagated to Snowflake tables and columns. Finally, click 'Save and Run' to start the automation

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/automation-form.png"/>
</p>

## Propagating for Existing Assets

You can back-fill historical data for existing assets to ensure that all existing column and table Tags and Glossary Terms are propagated to Snowflake.
Note that it may take some time to complete the initial back-filling process, depending on the number of Snowflake assets you have.

To do so, navigate to the Automation you created in Step 3 above, click the 3-dot "More" menu

<p align="left">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

and then click "Initialize".

<p align="left">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

This one-time step will kick off the back-filling process for existing descriptions. If you only want to begin propagating
descriptions going forward, you can skip this step.

## Viewing Propagated Tags

You can view propagated Tags (and corresponding DataHub URNs) inside the Snowflake UI to confirm the automation is working as expected.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/view-snowflake-tags.png"/>
</p>
