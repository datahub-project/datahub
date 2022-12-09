---
title: Setup
---
# BigQuery Ingestion Guide: Setup & Prerequisites

To configure ingestion from BigQuery, you'll need a [Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) configured with the proper permission sets, and an associated [Service Account Key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

This setup guide will walk you through the steps you'll need to take via your Google Cloud Console.

## BigQuery Prerequisites

If you do not have an existing Service Account and Service Account Key, please work with your BigQuery Admin to ensure you have the appropriate permissions and/or roles to continue with this setup guide.

When creating and managing new Service Accounts and Service Account Keys, we have found the following permissions and roles to be required:

* Create a Service Account: `iam.serviceAccounts.create` permission
* Assign roles to a Service Account: `serviceusage.services.enable` permission
* Set permission policy to the project: `resourcemanager.projects.setIamPolicy` permission
* Generate Key for Service Account: Service Account Key Admin (`roles/iam.serviceAccountKeyAdmin`) IAM role

:::note
Please refer to the BigQuery [Permissions](https://cloud.google.com/iam/docs/permissions-reference) and [IAM Roles](https://cloud.google.com/iam/docs/understanding-roles) references for details
:::

## BigQuery Setup

1. To set up a new Service Account follow [this guide](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

2. When you are creating a Service Account, assign the following predefined Roles:
   - [BigQuery Job User](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser)
   - [BigQuery Metadata Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer)
   - [BigQuery Resource Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.resourceViewer) -> This role is for Table-Level Lineage and Usage extraction
   - [Logs View Accessor](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) -> This role is for Table-Level Lineage and Usage extraction
   - [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) -> This role is for Profiling

:::note
You can always add/remove roles to Service Accounts later on. Please refer to the BigQuery [Manage access to projects, folders, and organizations](https://cloud.google.com/iam/docs/granting-changing-revoking-access) guide for more details.
:::

3. Create and download a [Service Account Key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). We will use this to set up authentication within DataHub.

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

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
