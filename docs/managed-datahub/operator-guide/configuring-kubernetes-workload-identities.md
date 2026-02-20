# Cloud Workload Identity Configuration for DataHub Executor Worker

:::note
This guide extends the [Configuring Remote Executor](./setting-up-remote-ingestion-executor.md) documentation with detailed instructions for configuring cloud-native workload identity on Kubernetes platforms.
:::

This guide covers configuring cloud workload identity for the DataHub Executor Worker on GKE, EKS, and AKS. Using workload identity allows your executor pods to authenticate to **any cloud resource** without storing passwords or long-lived credentials, following cloud security best practices.

## Overview

Cloud workload identity enables pods to authenticate as cloud service accounts without explicit credentials:

- **GCP**: GKE Workload Identity binds Kubernetes service accounts to GCP service accounts
- **AWS**: IAM Roles for Service Accounts (IRSA) allows pods to assume IAM roles
- **Azure**: Azure AD Workload Identity federates with Kubernetes service accounts

**Use Cases:** Once configured, the executor can access any cloud resource the service account has permissions for:
- Data warehouses (BigQuery, Redshift, Synapse, Snowflake on cloud)
- Databases (Cloud SQL, RDS, Aurora, Azure SQL)
- Cloud storage (GCS, S3, Azure Blob Storage)
- Other cloud services and APIs

The examples below show configuring access to specific services (BigQuery, RDS, Azure SQL), but the same pattern applies to any cloud resource you want to access without storing credentials in DataHub.

## Table of Contents
- [GCP: GKE Workload Identity](#gcp-gke-workload-identity)
- [AWS: IAM Roles for Service Accounts (IRSA)](#aws-iam-roles-for-service-accounts-irsa)
- [Azure: Azure AD Workload Identity](#azure-azure-ad-workload-identity)

---

## GCP: GKE Workload Identity

### Prerequisites
- GKE cluster with Workload Identity enabled ([enable if needed](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#enable))
- GCP project with appropriate permissions to create service accounts and IAM bindings

### 1. Get Your GKE Cluster Information

First, identify your GKE cluster and verify Workload Identity is enabled:

```bash
# Via gcloud CLI - check if Workload Identity is enabled
gcloud container clusters describe my-gke-cluster \
  --region=us-central1 \
  --format="value(workloadIdentityConfig.workloadPool)"

# Expected output: my-gcp-project.svc.id.goog
```

Or check via the [GKE Console](https://console.cloud.google.com/kubernetes):
1. Navigate to **Kubernetes Engine > Clusters**
2. Click on your cluster name
3. Under **Security**, verify **Workload Identity** is **Enabled**
4. Note your GCP project ID

If Workload Identity is not enabled, see [Google's documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#enable) for enabling it on your cluster.

### 2. Create GCP Service Account

```bash
# Create GCP service account
gcloud iam service-accounts create datahub-executor \
  --project=my-gcp-project \
  --display-name="DataHub Executor Service Account"
```

### 3. Grant Cloud Resource Permissions

Grant the GCP service account permissions to access the cloud resources you need. In this example, we grant access to **BigQuery** for metadata ingestion, but you can grant permissions to any GCP service (Cloud SQL, GCS, Pub/Sub, etc.) using the same pattern:

```bash
# Grant BigQuery Data Viewer role (read-only access to data)
gcloud projects add-iam-policy-binding my-gcp-project \
  --member="serviceAccount:datahub-executor@my-gcp-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# Grant BigQuery Metadata Viewer role (read metadata like schemas, tables)
gcloud projects add-iam-policy-binding my-gcp-project \
  --member="serviceAccount:datahub-executor@my-gcp-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.metadataViewer"

# Grant BigQuery Job User role (run queries for profiling)
gcloud projects add-iam-policy-binding my-gcp-project \
  --member="serviceAccount:datahub-executor@my-gcp-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

For more restrictive access, you can grant permissions at the dataset level:

```bash
# Grant access to a specific dataset
bq show --format=prettyjson my-dataset > /tmp/dataset.json

# Add the service account to the access list in the JSON
# Then update the dataset
bq update --source /tmp/dataset.json my-dataset
```

### 4. Create IAM Policy Binding

```bash
# Bind the GCP service account to the Kubernetes service account
gcloud iam service-accounts add-iam-policy-binding \
  datahub-executor@my-gcp-project.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:my-gcp-project.svc.id.goog[datahub/datahub-executor-sa]"
```

Note: Replace `datahub` with your Kubernetes namespace and `datahub-executor-sa` with your Kubernetes service account name.

### 5. Deploy with Helm

Create a values file (`values-gke-workload-identity.yaml`):

```yaml
serviceAccount:
  create: true
  name: "datahub-executor-sa"
  annotations:
    iam.gke.io/gcp-service-account: "datahub-executor@my-gcp-project.iam.gserviceaccount.com"

global:
  datahub:
    gms:
      secretRef: datahub-access-token-secret
      secretKey: datahub-access-token-secret-key
      url: https://your-company.acryl.io/gms
    executor:
      pool_id: "remote"
```

Deploy:

```bash
# Create secret for DataHub access token
kubectl create secret generic datahub-access-token-secret \
  --from-literal=datahub-access-token-secret-key=<DATAHUB-ACCESS-TOKEN> \
  --namespace datahub

# Install chart
helm install datahub-executor ./charts/datahub-executor-worker \
  --namespace datahub \
  --values values-gke-workload-identity.yaml
```

### 6. Verify Configuration

```bash
# Verify the pod is authenticated to GCP
kubectl exec -it deployment/datahub-executor-datahub-executor-worker -n datahub -- \
  gcloud auth list

# Test BigQuery access
kubectl exec -it deployment/datahub-executor-datahub-executor-worker -n datahub -- \
  bq ls --project_id=my-gcp-project
```

---

## AWS: IAM Roles for Service Accounts (IRSA)

### Prerequisites
- EKS cluster with OIDC provider configured ([configure if needed](https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html))
- AWS IAM permissions to create roles and policies
- AWS account ID

### 1. Get EKS OIDC Provider Information

First, retrieve your cluster's OIDC provider URL and verify it's configured:

```bash
# Get your cluster's OIDC provider URL
aws eks describe-cluster \
  --name my-eks-cluster \
  --region us-west-2 \
  --query "cluster.identity.oidc.issuer" \
  --output text

# Example output: https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE44288EXAMPLE

# Extract just the OIDC ID for use in IAM policies
aws eks describe-cluster \
  --name my-eks-cluster \
  --region us-west-2 \
  --query "cluster.identity.oidc.issuer" \
  --output text | cut -d '/' -f 5

# Example output: EXAMPLED539D4633E53DE44288EXAMPLE
```

Or check via the [EKS Console](https://console.aws.amazon.com/eks):
1. Navigate to **Amazon EKS > Clusters**
2. Click on your cluster name
3. Go to the **Configuration** tab
4. Under **Details**, find the **OpenID Connect provider URL**

If you don't see an OIDC provider URL, see [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html) to create an IAM OIDC provider for your cluster.

You'll also need your AWS account ID:

```bash
# Get your AWS account ID
aws sts get-caller-identity --query Account --output text
```

### 2. Create IAM Policy for Cloud Resources

Create an IAM policy granting permissions to the cloud resources you need to access. In this example, we grant access to **RDS/Aurora** for metadata discovery, but you can create policies for any AWS service (S3, Redshift, DynamoDB, etc.) following the same pattern:

Create a policy file (`rds-policy.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "rds:DescribeDBInstances",
        "rds:DescribeDBClusters",
        "rds:DescribeDBClusterSnapshots",
        "rds:ListTagsForResource"
      ],
      "Resource": "*"
    }
  ]
}
```

Create the policy:

```bash
aws iam create-policy \
  --policy-name DataHubExecutorRDSAccess \
  --policy-document file://rds-policy.json
```

**Note:** This policy grants read-only metadata access. The executor will still need database credentials (stored in Kubernetes secrets) to connect to the actual database and extract schemas.

### 3. Create IAM Role with Trust Policy

Create a trust policy file (`trust-policy.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE44288EXAMPLE"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE44288EXAMPLE:sub": "system:serviceaccount:datahub:datahub-executor-sa",
          "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE44288EXAMPLE:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
```

Create the role:

```bash
# Create IAM role
aws iam create-role \
  --role-name DataHubExecutorRole \
  --assume-role-policy-document file://trust-policy.json

# Attach RDS policy to role
aws iam attach-role-policy \
  --role-name DataHubExecutorRole \
  --policy-arn arn:aws:iam::123456789012:policy/DataHubExecutorRDSAccess
```

**Note:** Replace `123456789012` with your AWS account ID, `EXAMPLED539D4633E53DE44288EXAMPLE` with your OIDC ID, and `us-west-2` with your region in the examples above.

### 4. Deploy with Helm

Create a values file (`values-eks-irsa.yaml`):

```yaml
serviceAccount:
  create: true
  name: "datahub-executor-sa"
  annotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/DataHubExecutorRole"

global:
  datahub:
    gms:
      secretRef: datahub-access-token-secret
      secretKey: datahub-access-token-secret-key
      url: https://your-company.acryl.io/gms
    executor:
      pool_id: "remote"
```

Deploy:

```bash
# Create secret for DataHub access token
kubectl create secret generic datahub-access-token-secret \
  --from-literal=datahub-access-token-secret-key=<DATAHUB-ACCESS-TOKEN> \
  --namespace datahub

# Install chart
helm install datahub-executor ./charts/datahub-executor-worker \
  --namespace datahub \
  --values values-eks-irsa.yaml
```

### 5. Verify Configuration

```bash
# Verify the pod has AWS credentials
kubectl exec -it deployment/datahub-executor-datahub-executor-worker -n datahub -- env | grep AWS

# Test RDS access
kubectl exec -it deployment/datahub-executor-datahub-executor-worker -n datahub -- \
  aws rds describe-db-instances --region us-west-2
```

---

## Azure: Azure AD Workload Identity

### Prerequisites
- AKS cluster with Workload Identity enabled ([enable if needed](https://learn.microsoft.com/en-us/azure/aks/workload-identity-deploy-cluster))
- Azure subscription with appropriate permissions to create managed identities and role assignments
- Azure CLI installed and authenticated

### 1. Get AKS OIDC Issuer URL

First, verify that Workload Identity is enabled and retrieve the OIDC issuer URL:

```bash
# Set variables
RESOURCE_GROUP="my-resource-group"
AKS_CLUSTER_NAME="my-aks-cluster"

# Get OIDC issuer URL
export AKS_OIDC_ISSUER=$(az aks show \
  --name $AKS_CLUSTER_NAME \
  --resource-group $RESOURCE_GROUP \
  --query "oidcIssuerProfile.issuerUrl" \
  --output tsv)

echo $AKS_OIDC_ISSUER
# Expected output: https://westus2.oic.prod-aks.azure.com/00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000000/
```

Or check via the [Azure Portal](https://portal.azure.com):
1. Navigate to **Kubernetes services**
2. Click on your AKS cluster
3. Go to **Settings > Security**
4. Under **Workload Identity**, verify it's **Enabled**
5. Note the OIDC Issuer URL

If Workload Identity is not enabled, see [Azure documentation](https://learn.microsoft.com/en-us/azure/aks/workload-identity-deploy-cluster) to enable it on your cluster.

### 2. Create Azure Managed Identity

```bash
# Set variables
IDENTITY_NAME="datahub-executor-identity"
LOCATION="eastus"

# Create managed identity
az identity create \
  --name $IDENTITY_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

# Get identity details
export IDENTITY_CLIENT_ID=$(az identity show \
  --name $IDENTITY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query clientId -o tsv)

export IDENTITY_OBJECT_ID=$(az identity show \
  --name $IDENTITY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query principalId -o tsv)

export IDENTITY_RESOURCE_ID=$(az identity show \
  --name $IDENTITY_NAME \
  --resource-group $RESOURCE_GROUP \
  --query id -o tsv)

echo "Client ID: $IDENTITY_CLIENT_ID"
echo "Object ID: $IDENTITY_OBJECT_ID"
```

### 3. Grant Cloud Resource Permissions

Grant the managed identity permissions to access the Azure resources you need. In this example, we grant access to **Azure SQL Database/Synapse** for metadata ingestion, but you can grant permissions to any Azure service (Blob Storage, Data Lake, Cosmos DB, etc.) using the same pattern:

```bash
# Get your subscription ID
SUBSCRIPTION_ID=$(az account show --query id --output tsv)

# Grant Reader role to discover SQL servers and databases
az role assignment create \
  --assignee $IDENTITY_OBJECT_ID \
  --role "Reader" \
  --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP"

# For specific SQL Server access
SQL_SERVER_NAME="my-sql-server"

az role assignment create \
  --assignee $IDENTITY_OBJECT_ID \
  --role "Reader" \
  --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Sql/servers/$SQL_SERVER_NAME"
```

**Note:** You'll also need to grant the managed identity database-level permissions by connecting to the database:

```sql
-- Connect to your Azure SQL Database as an admin, then run:
CREATE USER [datahub-executor-identity] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [datahub-executor-identity];
GRANT VIEW DEFINITION TO [datahub-executor-identity];
```

### 4. Create Federated Identity Credential

```bash
# Create federated identity credential (using OIDC issuer from step 1)
az identity federated-credential create \
  --name datahub-executor-federated-credential \
  --identity-name $IDENTITY_NAME \
  --resource-group $RESOURCE_GROUP \
  --issuer $AKS_OIDC_ISSUER \
  --subject system:serviceaccount:datahub:datahub-executor-sa \
  --audiences api://AzureADTokenExchange
```

### 5. Deploy with Helm

Create a values file (`values-aks-workload-identity.yaml`):

```yaml
serviceAccount:
  create: true
  name: "datahub-executor-sa"
  annotations:
    azure.workload.identity/client-id: "<IDENTITY_CLIENT_ID>"

podLabels:
  azure.workload.identity/use: "true"

global:
  datahub:
    gms:
      secretRef: datahub-access-token-secret
      secretKey: datahub-access-token-secret-key
      url: https://your-company.acryl.io/gms
    executor:
      pool_id: "remote"
```

**Note:** The chart needs to support `podLabels` or `extraPodLabels` for Azure Workload Identity to work. If using the existing chart, use `extraPodLabels`:

```yaml
serviceAccount:
  create: true
  name: "datahub-executor-sa"
  annotations:
    azure.workload.identity/client-id: "<IDENTITY_CLIENT_ID>"

extraPodLabels:
  azure.workload.identity/use: "true"

global:
  datahub:
    gms:
      secretRef: datahub-access-token-secret
      secretKey: datahub-access-token-secret-key
      url: https://your-company.acryl.io/gms
    executor:
      pool_id: "remote"
```

Deploy:

```bash
# Create secret for DataHub access token
kubectl create secret generic datahub-access-token-secret \
  --from-literal=datahub-access-token-secret-key=<DATAHUB-ACCESS-TOKEN> \
  --namespace datahub

# Install chart
helm install datahub-executor ./charts/datahub-executor-worker \
  --namespace datahub \
  --values values-aks-workload-identity.yaml
```

### 6. Verify Configuration

```bash
# Verify the pod has the workload identity label
kubectl get pod -n datahub -l app.kubernetes.io/name=datahub-executor-worker -o jsonpath='{.items[0].metadata.labels}'

# Verify environment variables
kubectl exec -it deployment/datahub-executor-datahub-executor-worker -n datahub -- env | grep AZURE

# Test Azure SQL access
kubectl exec -it deployment/datahub-executor-datahub-executor-worker -n datahub -- \
  az sql server list --resource-group $RESOURCE_GROUP
```

---

## Troubleshooting

### GCP Workload Identity

- **Issue:** Pod cannot authenticate to GCP
  - Verify the annotation on the service account: `kubectl describe sa datahub-executor-sa -n datahub`
  - Verify IAM binding: `gcloud iam service-accounts get-iam-policy datahub-executor@my-gcp-project.iam.gserviceaccount.com`
  - Check pod metadata server: `kubectl exec -it <pod> -- curl -H "Metadata-Flavor: Google" http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/email`

### AWS IRSA

- **Issue:** Pod cannot assume IAM role
  - Verify the annotation on the service account: `kubectl describe sa datahub-executor-sa -n datahub`
  - Check environment variables: `kubectl exec -it <pod> -- env | grep AWS`
  - Verify trust policy allows the correct namespace and service account
  - Ensure OIDC provider is configured: `aws eks describe-cluster --name my-eks-cluster --query "cluster.identity.oidc.issuer"`

### Azure Workload Identity

- **Issue:** Pod cannot authenticate to Azure
  - Verify the pod has the label `azure.workload.identity/use: "true"`: `kubectl describe pod <pod> -n datahub`
  - Verify the service account annotation: `kubectl describe sa datahub-executor-sa -n datahub`
  - Check federated credential: `az identity federated-credential list --identity-name datahub-executor-identity --resource-group $RESOURCE_GROUP`
  - Verify environment variables: `kubectl exec -it <pod> -- env | grep AZURE`

---

## Security Best Practices

1. **Principle of Least Privilege**: Grant only the minimum permissions required for the executor to function
2. **Namespace Isolation**: Use separate service accounts per namespace for multi-tenant deployments
3. **Audit Logging**: Enable cloud audit logs to monitor service account usage
4. **Rotation**: Regularly review and update IAM bindings and access policies
5. **Network Policies**: Implement Kubernetes network policies to restrict pod-to-pod communication

---

## Additional Resources

- [GKE Workload Identity Documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [AWS IRSA Documentation](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
- [Azure Workload Identity Documentation](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview)
