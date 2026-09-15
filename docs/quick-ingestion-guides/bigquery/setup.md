---
title: BigQuery Setup
---

# BigQuery Ingestion Guide: Setup & Prerequisites

To configure ingestion from BigQuery, you'll need a [Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) configured with the proper permission sets and an associated [Service Account Key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

This setup guide will walk you through the steps you'll need to take via your Google Cloud Console.

## BigQuery Prerequisites

If you do not have an existing Service Account and Service Account Key, please work with your BigQuery Admin to ensure you have the appropriate permissions and/or roles to continue with this setup guide.

When creating and managing new Service Accounts and Service Account Keys, we have found the following permissions and roles to be required:

- Create a Service Account: `iam.serviceAccounts.create` permission
- Assign roles to a Service Account: `serviceusage.services.enable` permission
- Set permission policy to the project: `resourcemanager.projects.setIamPolicy` permission
- Generate Key for Service Account: Service Account Key Admin (`roles/iam.serviceAccountKeyAdmin`) IAM role

:::note
Please refer to the BigQuery [Permissions](https://cloud.google.com/iam/docs/permissions-reference) and [IAM Roles](https://cloud.google.com/iam/docs/understanding-roles) references for details
:::

## BigQuery Setup

1. To set up a new Service Account follow [this guide](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

2. When you are creating a Service Account, assign the following predefined Roles:
   - [BigQuery Job User](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser)
   - [BigQuery Metadata Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer)
   - [BigQuery Resource Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.resourceViewer) -> This role is for Table-Level Lineage and Usage extraction
   - [Logs View Accessor](https://cloud.google.com/logging/docs/access-control#logging.viewAccessor) -> This role is for Table-Level Lineage and Usage extraction
   - [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) -> This role is for Profiling
   - [BigQuery Read Session User](https://cloud.google.com/bigquery/docs/access-control#bigquery.readSessionUser) -> This role is for Profiling

:::note
You can always add/remove roles to Service Accounts later on. Please refer to the BigQuery [Manage access to projects, folders, and organizations](https://cloud.google.com/iam/docs/granting-changing-revoking-access) guide for more details.
:::

### Permissions for DataHub Cloud Assertions (Observe)

If you plan to use DataHub Cloud's [Freshness](/docs/managed-datahub/observe/freshness-assertions.md), [Volume](/docs/managed-datahub/observe/volume-assertions.md), [Column](/docs/managed-datahub/observe/column-assertions.md), or [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md) Assertions, the required permissions depend on which source type and assertion type you select.

#### Freshness & Volume Assertions

| Source Type                                                      | Required Role(s)                                                                                                                                                                                              | Notes                                                                                                                                                                                                   |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Platform API**                                                 | [BigQuery Metadata Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) (`roles/bigquery.metadataViewer`)                                                                   | Free API call, no query costs. Subject to BigQuery [API rate limits](https://cloud.google.com/bigquery/quotas#api_quotas_and_limits) — stagger custom schedules across many assertions to avoid bursts. |
| **Information Schema**                                           | [BigQuery Metadata Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer) + [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) | Uses `__TABLES__` system table.                                                                                                                                                                         |
| **Audit Log**                                                    | `logging.logEntries.list` + `logging.privateLogEntries.list`                                                                                                                                                  | Via [Logs View Accessor](https://cloud.google.com/logging/docs/access-control#logging.viewAccessor) role.                                                                                               |
| **Query** / **Last Modified Column** / **High Watermark Column** | [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer)                                                                                                             | Runs SQL queries against the table.                                                                                                                                                                     |
| **DataHub Operation** / **DataHub Dataset Profile**              | _(none)_                                                                                                                                                                                                      | Uses DataHub metadata only, no BigQuery access needed.                                                                                                                                                  |

#### Column (Field) Assertions

| Source Type                                 | Required Role(s)                                                                                  | Notes                                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **All Rows Query** / **Changed Rows Query** | [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) | Runs SQL queries against the table to evaluate column-level conditions.                   |
| **DataHub Dataset Profile**                 | _(none)_                                                                                          | Uses column metrics from DataHub profiling runs. Only available for certain metric types. |

#### Custom SQL Assertions

| Required Role(s)                                                                                                                                                                                | Notes                                                                                                        |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| [BigQuery Job User](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser) + [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) | Executes user-defined SQL queries. Ensure the Service Account can access all tables referenced in the query. |

3. To filter projects based on the `project_labels` configuration, first visit [cloudresourcemanager.googleapis.com](https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview) and enable the `Cloud Resource Manager API`

4. Create and download a [Service Account Key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). We will use this to set up authentication within DataHub.

The key file looks like this:

```json
{
  "type": "service_account",
  "project_id": "project-id-1234567",
  "private_key_id": "d0121d0000882411234e11166c6aaa23ed5d74e0",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----",
  "client_email": "test@suppproject-id-1234567.iam.gserviceaccount.com",
  "client_id": "113545814931671546333",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%suppproject-id-1234567.iam.gserviceaccount.com"
}
```

## Next Steps

Once you've confirmed all of the above in BigQuery, it's time to [move on](configuration.md) to configure the actual ingestion source within the DataHub UI.
