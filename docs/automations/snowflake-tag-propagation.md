import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Snowflake Tag Propagation Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in open beta in DataHub Cloud. Reach out to your DataHub Cloud representative to get access.

:::

## Introduction

Snowflake Tag Propagation is an automation that allows you to sync DataHub Glossary Terms and Tags on
both columns and tables back to Snowflake. This automation is available in DataHub Cloud only.

This automation is bidirectional - it works in unison with the Snowflake Ingestion Source, to ensure that Tags which are ingested can
be synced back to Snowflake properly, and those which are synced to Snowflake can correctly be ingested back into DataHub. 

## Capabilities

- Automatically Add DataHub Glossary Terms to Snowflake Tables and Columns as Snowflake Tags
- Automatically Add DataHub Tags to Snowflake Tables and Columns as Snowflake Tags
- Automatically Remove DataHub Glossary Terms and Tags from Snowflake Tables and Columns when they are removed in DataHub
- Any tags that were previously provisioned by DataHub will be ingested as their original DataHub Tags or Glossary Terms.
- Any tags that were ingested from Snowflake into DataHub will be able to sync back into Snowflake as Tags (so long as you configure them to sync back)

## Caveats

- Currently, renaming a Tag or Glossary Term in DataHub will not rename the corresponding Snowflake Tag. The existing tag will continue to be used and applied. You can manually change or remove the old tag in Snowflake if needed.

### Tag Provisioning in Snowflake

Tags in Snowflake are associated with a single Database or Schema. To avoid creating duplicate tags in every Database or Schema,
DataHub will automatically sync new tags into a Database named `DATAHUB` and a schema within named `SYNCED_TAGS`.

By default, we'll attempt to provision this Database and Schema if they do not exist, however this requires that the service
account being used for the Automation has the privileges to create databases and schemas. If you do not want to grant
these privileges, you are free to create the `DATAHUB` database and `SYNCED_TAGS` schema manually prior to enabling the automation.


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

You can back-fill historical data for existing assets to ensure that all current column and table Glossary Terms are propagated to Snowflake.
Note that it may take some time to complete the initial back-filling process, depending on the number of Snowflake assets you have.

To do so, navigate to the Automation you created in Step 3 above, click the 3-dot "More" menu

<p align="left">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png"/>
</p>

and then click "Initialize".

<p align="left">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png"/>
</p>

This one-time step will kick off the back-filling process for existing terms. If you only want to begin propagating
terms going forward, you can skip this step.

:::info

The back-filling of tags will be available in a future release.

:::

## Viewing Propagated Tags

You can view propagated Tags (and corresponding DataHub URNs) inside the Snowflake UI to confirm the automation is working as expected.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/snowflake-tag-propagation/view-snowflake-tags.png"/>
</p>

## FAQ

### Can I provide a custom name for the default `DATAHUB` database or `SYNCED_TAGS` schema where Tags are created?

Not yet. In a future release, we may add support for this but as of today this is not changeable via the UI.

### Can I create Tags in the same database and schema where the corresponding tables / columns are defined?

Currently, no. Based on demand, we may add support for this but as of today this is not changeable via the UI. 
For now, all Tags that are provisioned by DataHub in Snowflake will exist under the special `DATAHUB.SYNCED_TAGS` schema. 

### If I create a Tag on DataHub and apply it to a table or column, will it be synced back to Snowflake?

Yes. When you create a new Tag in DataHub and apply it to a table or column, it will be synced back to Snowflake as a new Tag within the
`DATAHUB.SYNCED_TAGS` schema.

### If I ingest my existing Tags from Snowflake into DataHub, will they be synced back to Snowflake after adding them to a table or column in DataHub?

Yes. When ingesting, we mint a new DataHub Tag from the Snowflake Tag. When that Tag is applied to columns or tables within DataHub, it will be synced back to Snowflake as the original Snowflake Tag. 

### If I create a Tag on DataHub, and apply it to a table or column, and then it syncs back to Snowflake, and finally I add the tag to a table or column in _Snowflake_, will ingestion reflect the new relationship properly? 

Yes. When you add a Tag that was created by DataHub to a table or column in Snowflake, it will be ingested back into DataHub as the same Tag or Glossary Term. This allows you to update the Tag associations
in either Snowflake or DataHub, although we recommend choosing one source of truth system to maintain sanity. 

### If I rename a Tag or Glossary Term in DataHub, will the new name be reflected in Snowflake?

No - Snowflake does not support renaming Tags. Thus, any name change that occurs on DataHub will not be reflected in Snowflake. 

### I've enabled the automation, but I don't see tags being created. Why not?

A few things to try: 

1. Verify your Snowflake connection details & credentials are correct.
2. Ensure that the service account provided has the ability to create databases and schemas in Snowflake, IF you did not create the `DATAHUB` database and `SYNCED_TAGS` schema manually.
3. If you have specific Tags or Glossary Terms selected in the automation configuration, ensure that you are testing applying those Tags and Glossary Terms only. 