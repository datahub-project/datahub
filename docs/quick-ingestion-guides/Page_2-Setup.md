# DataHub [BigQuery] UI Ingestion Guide: Setup & Prerequesites

In order to configure ingestion from [BigQuery], you'll first have to ensure you have a [Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts)
with the proper permissions set.

## [BigQuery] Prerequesites
1. To set up a Service Account follow [this guide](https://cloud.google.com/iam/docs/creating-managing-service-accounts)
2. Assign the following predefined roles to the service account [following this guide](https://cloud.google.com/iam/docs/manage-access-service-accounts): 
   - [BigQuery Job User](https://cloud.google.com/bigquery/docs/access-control#bigquery.jobUser)
   - [BigQuery Metadata Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.metadataViewer)
   - [BigQuery Resource Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.resourceViewer) -> This role only needs if Lineage and Usage generation enabled
   - [BigQuery Data Viewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) -> This role only needs if Profiling is enabled
3. [Create a Service Account Key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) and download it as the content of the key is needed to set up the authentication

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
Once you've confirmed all of the above in [BigQuery], it's time to [move on](Page_3-Configuration.md) to configuring the actual ingestion source within the DataHub UI

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
