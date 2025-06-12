import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Databricks Metadata Sync Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in **Public Beta** in DataHub Cloud. Reach out to your DataHub Cloud representative if you face any issues configuring or validating the capabilities outlined below. 

:::

## Overview

Databricks Metadata Sync is an automation feature that enables seamless synchronization of DataHub Tags and Descriptions with Databricks Unity Catalog. This automation ensures consistent metadata governance across both platforms, automatically propagating DataHub governance artifacts to Unity Catalog tables, columns, catalogs, and schemas. Typically, this will be used in conjunction with the [Databricks ingestion source](https://docs.datahub.com/docs/generated/ingestion/sources/databricks), which enables ingesting Tags & descriptions from Databricks into DataHub. 

This automation is exclusively available in DataHub Cloud.

## Use Cases

- Maintain consistent metadata across DataHub and Databricks
- Improve data discovery by propagating descriptions back to Databricks
- Unity data governance by managing Tag application directly within DataHub

## Sync Capabilities

The Databricks Metadata Sync automation provides comprehensive metadata synchronization with the following features:

- **Automated Tag Propagation**: Seamlessly sync DataHub Tags to Unity Catalog tables, columns, catalogs, and schemas
- **Description Synchronization**: Automatically propagate DataHub descriptions to Unity Catalog objects as comments
- **Bidirectional Updates**: Maintain consistency by automatically removing Tags and descriptions from Unity Catalog when they are removed in DataHub
- **Selective Propagation**: Configure specific Tags for propagation, or sync all Tags
- **Historical Backfill**: Initialize Tags and Descriptions for assets on Databricks with current DataHub Tags & Descriptions.

> **A note about legacy Hive Metastore**: Bi-directional sync for _descriptions_ is supported for Hive Metastore Schemas & Tables, but Tag sync is _not_. This is because Databricks does not support applying of Tags to these assets on Hive Metastore.  

## Prerequisites

Before enabling Databricks Metadata Sync, ensure the following permissions and configurations are in place:

### Required Unity Catalog Permissions

#### Basic Access Permissions (Required for Both Tags and Descriptions)

- **USE CATALOG**: Access to the Unity Catalog containing target objects
- **USE SCHEMA**: Permission to access schemas within the catalog

#### Permissions for Tag Synchronization

Based on Unity Catalog requirements, to add tags to objects you need:

- **APPLY TAG**: Required on each object where tags will be applied (catalogs, schemas, tables, columns)
- **USE SCHEMA**: Required on the object's parent schema (already covered in basic access)
- **USE CATALOG**: Required on the object's parent catalog (already covered in basic access)

_Note: If using governed tags, you may also need ASSIGN permission on the tag policy._

#### Permissions for Description Synchronization

- **MODIFY**: Required to update comments/descriptions on Unity Catalog objects (catalogs, schemas, tables, columns)

### Example Permission Configuration

Configure the necessary permissions for your DataHub automation service principal based on your sync requirements:

#### For Tags Only

```sql
-- Basic access permissions
GRANT USE CATALOG ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT USE SCHEMA ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;

-- Tag application permissions
GRANT APPLY TAG ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT APPLY TAG ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
GRANT APPLY TAG ON ALL TABLES IN SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
```

#### For Descriptions Only

```sql
-- Basic access permissions
GRANT USE CATALOG ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT USE SCHEMA ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;

-- Description modification permissions
GRANT MODIFY ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT MODIFY ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
GRANT MODIFY ON ALL TABLES IN SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
```

#### For Both Tags and Descriptions

```sql
-- Basic access permissions
GRANT USE CATALOG ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT USE SCHEMA ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;

-- Tag application permissions
GRANT APPLY TAG ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT APPLY TAG ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
GRANT APPLY TAG ON ALL TABLES IN SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;

-- Description modification permissions
GRANT MODIFY ON CATALOG your_catalog TO `datahub-automation@your-domain.com`;
GRANT MODIFY ON SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
GRANT MODIFY ON ALL TABLES IN SCHEMA your_catalog.your_schema TO `datahub-automation@your-domain.com`;
```

### Connection Requirements

Ensure your DataHub instance has:

- Valid Databricks workspace credentials
- Network connectivity to your Databricks Unity Catalog environment
- Appropriate service principal or user authentication configured
- Databricks warehouse id for executing operations

## Configuration Guide

### Step 1: Access Automations

Navigate to the Automations section in your DataHub Cloud interface:

1. Select **Automations** from the dropdown menu, Under the **Govern** section

<p align="center">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png" alt="Navigate to Automations"/>
</p>

### Step 2: Create Databricks Automation

Configure the automation:

1. Click the **Create** button
2. Select **Databricks Metadata Sync** from the available automation types

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/databricks-metadata-sync/automation-selection.png" alt="Select Databricks Metadata Sync"/>
</p>

### Step 3: Configure Sync Options

Choose the types of information to synchronize:

#### Select Action

Choose between:

- **Tags**: Sync Tags for Tables, Columns, Catalogs, & Schemas (Unity Catalog only)
- **Descriptions**: Sync descriptions for Tables, Columns, Catalogs & Schemas as comments (Unity Catalog & legacy Hive Metastore) 

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/databricks-metadata-sync/select-action.png" alt="Select Sync Action"/>
</p>

#### Configure Tag Selection (if Tags selected)

When syncing Tags, you can choose:

- **All Tags**: Propagate all DataHub Tags to Unity Catalog
- **Specific Tags**: Select only specific Tags for synchronization

### Step 4: Configure Connection Settings

Complete the Databricks connection configuration:

#### Required Connection Details

- **Workspace URL**: Your Databricks workspace URL (e.g., `https://abcsales.cloud.databricks.com`)
- **Warehouse ID**: The SQL warehouse ID used for executing for metadata operations (e.g., `fab3e5fg0bcbfc56`)
- **Token**: Databricks personal access token or service principal token

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/databricks-metadata-sync/connection-config.png" alt="Connection Configuration"/>
</p>

#### Test Connection

Click **Test Connection** to verify your configuration before proceeding.

### Step 5: Configure Automation Details

Provide automation metadata:

- **Name**: Descriptive name for your automation (e.g., "Databricks Metadata Sync")
- **Description**: Details about the automation's purpose and scope
- **Category**: Select an appropriate category for organization

Click **Save and Run** to activate the automation and begin real-time synchronization.

## Historical Data Synchronization

### Initializing Existing Assets

For environments with existing DataHub metadata, you can perform a one-time backfill to ensure all current Tags and Descriptions from DataHub are propagated to Unity Catalog. Depending on the number of assets, this might take a while! 

#### Initialization Process

1. Navigate to your created Databricks Metadata Sync automation
2. Click the three-dot **More** menu next to the automation

<p align="left">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-more-menu.png" alt="Automation More Menu"/>
</p>

3. Select **Initialize** from the dropdown menu

<p align="left">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automation-initialize.png" alt="Initialize Automation"/>
</p>

:::note Initialization Timeline

The initialization process duration depends on the volume of Unity Catalog assets in your environment. Large catalogs with extensive metadata may require significant processing time.

:::

## Validating the Integration

### Viewing Synced Metadata

Confirm successful metadata syncing by examining Unity Catalog objects:

1. **Access Databricks UI**: Navigate to your Databricks workspace
2. **Browse Catalog**: Open the Unity Catalog explorer
3. **Inspect Objects**: Select tables or columns to view applied tags and comments


## Troubleshooting

### Common Issues and Solutions

#### Permission Errors

- Verify service principal has all required Unity Catalog permissions
- Confirm catalog and schema access rights
- Check tag creation and application privileges

#### Connection Issues

- Validate Databricks workspace URL format
- Ensure access token is valid and not expired
- Verify warehouse ID is correct and accessible
- Check network connectivity between DataHub and Databricks

#### Synchronization Failures

- Check Unity Catalog object permissions
- Verify target objects exist and are accessible
- Ensure warehouse is running and available

### Support Resources

For additional assistance with Databricks Metadata Sync, contact your DataHub Cloud representative. 

## FAQ

1. **Where should I manage Tags & Descriptions?**

In general, we recommend centrally authoring Tags and Descriptions within DataHub. This allows you to maintain a clear and consistent governance posture across _all_ of your data sources and data products - there is always data outside of Databricks! Authoring this critical information in DataHub also improves the experience for your data practicioners trying to find the right data. 

This automation is intended to enable this style of management, allowing you to "push down" metadata from the central catalog into Databricks, where your data is stored and queried. 

2. **How does DataHub represent key-value tags from Databricks?**

During ingestion from [Databricks](https://docs.datahub.com/docs/generated/ingestion/sources/databricks), DataHub can ingest tags and descriptions that were originally authored within Databricks. DataHub converts key-value formatted tags in Databricks into DataHub tags of the format: `key:value`. For example, if you have a tag with key `has_pii` and value `true` in Databricks, this will be ingested as a single combined tag named `has_pii: true` in DataHub.

After ingestion into DataHub, you can apply this tag to tables or columns and sync it back to Databricks using this automation. Any tag with the format `key:value` that is applied on DataHub will be synced back to Databricks in proper key, value form. 

If you apply a tag without a separator colon in DataHub (e.g. `has_pii`), it will be synced back to Databricks with the key being `has_pii` and value being empty. 


3. **I updated a table description in _Databricks_, but I don't see it reflecting after ingestion into DataHub. Why not?**

This is usually because you've already overridden the description inside DataHub for this table. DataHub assumes that _it_ will be the source of truth for documentation, which means that any edits that have taken place in the DataHub UI (or via API) will take precedent over changes provided in Databricks. When you change the description in DataHub, the description change will overwrite the latest description in Databricks if this automation is enabled. 

But fear not - you can always view the original underlying Databricks description underneath the DataHub description in the DataHub UI, even when it changes. 


4. **Can I sync DataHub Structured Properties or Glossary Terms back to Databricks as Tags?**

Currently, no. Sync back is limited to Tags, to keep the concepts aligned more simply across both platforms. Reach out if you'd benefit from this capability! 

