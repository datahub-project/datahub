### Overview

The `gcs` module ingests metadata from Gcs into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This connector ingests Google Cloud Storage datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub.
To specify the group of files that form a dataset, use `path_specs` configuration in ingestion recipe. This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
and uses DataHub S3 Data Lake integration source under the hood. Refer section [Path Specs](https://docs.datahub.com/docs/generated/ingestion/sources/s3/#path-specs) from S3 connector for more details.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

The GCS source supports two authentication methods:

1. **HMAC Keys** (default): Service account key-based authentication using Google Cloud Storage HMAC keys.
2. **Workload Identity Federation**: Keyless, token-based authentication. Recommended for Kubernetes/GKE workloads (avoids storing service account keys), cross-cloud authentication (AWS/Azure → GCP), and environments requiring automatic credential rotation.

#### HMAC Authentication

1. Create a service account with "Storage Object Viewer" role — [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create).
2. Ensure you meet the [requirements to generate an HMAC key](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin).
3. Create an HMAC key for the service account — [Create HMAC keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).

#### Workload Identity Federation

1. Set up a Workload Identity Pool and Provider in Google Cloud — [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation).
2. Configure the credential (file path, inline JSON, or JSON string) that the connector will use to obtain short-lived access tokens. For Kubernetes (GKE), see [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).
