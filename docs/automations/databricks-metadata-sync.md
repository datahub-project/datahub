import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Databricks Metadata Sync Automation

<FeatureAvailability saasOnly />

:::info

This feature is currently in open beta in DataHub Cloud. Reach out to your DataHub Cloud representative to get access.

:::

## Overview

Databricks Metadata Sync is an automation feature that enables seamless synchronization of DataHub Tags and Descriptions with Databricks Unity Catalog. This automation ensures consistent metadata governance across both platforms, automatically propagating DataHub governance artifacts to Unity Catalog tables, columns, catalogs, and schemas.

This automation is exclusively available in DataHub Cloud and provides real-time synchronization capabilities for enhanced data governance workflows.

## Key Capabilities

The Databricks Metadata Sync automation provides comprehensive metadata synchronization with the following features:

- **Automated Tag Propagation**: Seamlessly sync DataHub Tags to Unity Catalog tables, columns, catalogs, and schemas
- **Description Synchronization**: Automatically propagate DataHub descriptions to Unity Catalog objects as comments
- **Bidirectional Updates**: Maintain consistency by automatically removing Tags and descriptions from Unity Catalog when they are removed in DataHub
- **Selective Propagation**: Configure specific Tags for propagation, or sync all Tags
- **Historical Backfill**: Initialize existing assets with current DataHub metadata

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
- Databricks warehouse access for metadata operations

## Configuration Guide

### Step 1: Access Automations

Navigate to the Automations section in your DataHub Cloud interface:

1. Click on **Govern** in the main navigation
2. Select **Automations** from the dropdown menu

<p align="center">
  <img width="20%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/automations-nav-link.png" alt="Navigate to Automations"/>
</p>

### Step 2: Create Databricks Automation

Initiate the automation setup:

1. Click the **Create** button
2. Select **Databricks Metadata Sync** from the available automation types

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/databricks-metadata-sync/automation-selection.png" alt="Select Databricks Metadata Sync"/>
</p>

### Step 3: Configure Sync Options

Choose the types of information to synchronize:

#### Select Action

Choose between:

- **Tags**: Sync Tags for Tables, Columns, Catalogs, & Schemas (Unity Catalog)
- **Descriptions**: Sync descriptions for Tables, Columns, Catalogs & Schemas as comments

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/automation/saas/databricks-metadata-sync/select-action.png" alt="Select Sync Action"/>
</p>

#### Configure Tag Selection (if Tags selected)

When syncing Tags, you can choose:

- **All tags**: Propagate all DataHub Tags to Unity Catalog
- **Tags in a specific set**: Select only specific Tags for synchronization

### Step 4: Configure Connection Settings

Complete the Databricks connection configuration:

#### Required Connection Details

- **Workspace URL**: Your Databricks workspace URL (e.g., `https://abcsales.cloud.databricks.com`)
- **Warehouse ID**: The SQL warehouse ID for metadata operations (e.g., `fab3e5fg0bcbfc56`)
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

For environments with existing DataHub metadata, you can perform a one-time backfill to ensure all current Tags and descriptions are propagated to Unity Catalog.

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

The initialization process duration depends on the volume of Unity Catalog assets in your environment. Large catalogs with extensive metadata may require significant processing time. Monitor the automation status for completion updates.

:::

## Verification and Monitoring

### Viewing Propagated Metadata

Confirm successful metadata propagation by examining Unity Catalog objects:

1. **Access Databricks UI**: Navigate to your Databricks workspace
2. **Browse Catalog**: Open the Unity Catalog explorer
3. **Inspect Objects**: Select tables or columns to view applied tags and comments

### Monitoring Automation Status

Track automation performance through the DataHub Automations dashboard:

- **Execution History**: Review recent synchronization activities
- **Error Logs**: Identify and resolve any propagation issues
- **Performance Metrics**: Monitor sync frequency and success rates

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

- Review automation logs for specific error messages
- Check Unity Catalog object permissions
- Verify target objects exist and are accessible
- Ensure warehouse is running and available

### Support Resources

For additional assistance with Databricks Metadata Sync:

- Contact your DataHub Cloud representative
- Review DataHub Cloud documentation
- Submit support tickets through your designated support channel
