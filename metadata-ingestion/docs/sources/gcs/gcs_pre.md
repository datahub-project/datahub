### Overview

The `gcs` module ingests metadata from Gcs into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This connector ingests Google Cloud Storage datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub.
To specify the group of files that form a dataset, use `path_specs` configuration in ingestion recipe. This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
and uses DataHub S3 Data Lake integration source under the hood. Refer section [Path Specs](https://docs.datahub.com/docs/generated/ingestion/sources/s3/#path-specs) from S3 connector for more details.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

The GCS source supports three authentication methods:

1. **HMAC Keys** (`hmac`, default): Long-lived key-based authentication using Google Cloud Storage HMAC keys. Suitable for simple setups and service accounts outside GCP.
2. **GKE Workload Identity** (`workload_identity`): Keyless authentication using [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). The recommended option when DataHub runs inside GKE with Workload Identity enabled — no credential config required.
3. **Workload Identity Federation** (`workload_identity_federation`): Keyless, token-based authentication for workloads running _outside_ GCP (AWS, Azure, on-premises). Exchanges an external identity token for short-lived GCP credentials via GCP's STS endpoint.

#### HMAC Authentication

1. Create a service account with "Storage Object Viewer" role — [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create).
2. Ensure you meet the [requirements to generate an HMAC key](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin).
3. Create an HMAC key for the service account — [Create HMAC keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).

#### GKE Workload Identity

This is the simplest option for DataHub running inside GKE. No credential files or secrets are needed — the pod's Kubernetes Service Account is used automatically.

1. Enable Workload Identity on your GKE cluster — [Enable Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#enable).
2. Create a Google Service Account (GSA) with "Storage Object Viewer" role.
3. Bind your Kubernetes Service Account (KSA) to the GSA:

   ```bash
   gcloud iam service-accounts add-iam-policy-binding GSA_EMAIL \
     --role roles/iam.workloadIdentityUser \
     --member "serviceAccount:PROJECT_ID.svc.id.goog[NAMESPACE/KSA_NAME]"
   kubectl annotate serviceaccount KSA_NAME \
     --namespace NAMESPACE \
     iam.gke.io/gcp-service-account=GSA_EMAIL
   ```

4. Set `auth_type: workload_identity` in your recipe. No `credential` or WIF config fields are needed.

#### Workload Identity Federation

Use this when DataHub runs outside GCP and you want keyless authentication without distributing service account key files.

1. Set up a Workload Identity Pool and Provider in Google Cloud — [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation).
2. Grant the external identity permission to impersonate a GCP service account with "Storage Object Viewer" role.
3. Download or generate the WIF credential configuration JSON from the Google Cloud Console.
4. Supply the configuration to the connector via one of three options: a file path (`gcp_wif_configuration`), inline dict (`gcp_wif_configuration_json`), or JSON string (`gcp_wif_configuration_json_string`). The JSON string option is useful for injecting from a secrets manager.
