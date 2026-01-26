---
title: Configuring Remote Executor
description: Learn how to set up, deploy, and configure Remote Executors in your environment
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Configuring Remote Executor

<FeatureAvailability saasOnly />

## Overview

This guide walks you through setting up a Remote Executor to run metadata ingestion within your private network.

:::info
For architecture details and security model, see [About Remote Executor](../remote-executor/about.md).
:::

## Quick Start Checklist

| Step | Who     | Action                                               |
| ---- | ------- | ---------------------------------------------------- |
| 1    | You     | Prepare prerequisites (Admin access, generate token) |
| 2    | You     | Request container registry access from DataHub       |
| 3    | DataHub | Grants registry access to your environment           |
| 4    | You     | Create an Executor Pool in DataHub UI                |
| 5    | You     | Deploy the executor (ECS, Kubernetes, etc.)          |
| 6    | You     | Verify the executor is running                       |
| 7    | You     | Assign ingestion sources to your Pool                |

## Understanding Remote Executors and Pools

A Remote Executor Pool provides a way to organize and manage your Remote Executors in DataHub. Here's how they work:

- A **Remote Executor Pool** (or **Pool**) is a logical grouping of one or more Remote Executors
- Each **Remote Executor** in a Pool automatically reports its status and health
- Each **Ingestion Source** can be assigned to a specific Pool
- One Pool can be designated as the **Default Pool** for new Ingestion Sources
- Multiple Pools can be created for different purposes (e.g., by region, department, or workload type)

## Prerequisites

Before deploying a Remote Executor, ensure you have the following:

### What You Need from DataHub

| Item                         | How to Get It                                                       |
| ---------------------------- | ------------------------------------------------------------------- |
| **Admin** role               | Contact your DataHub admin to assign this role                      |
| Remote Executor Access Token | **Settings > Access Tokens > Generate new token > Remote Executor** |
| Your DataHub Cloud URL       | Format: `https://<your-company>.acryl.io/gms` (must include `/gms`) |
| Container Registry Access    | Contact DataHub to set up access (see deployment steps for details) |

### What You Need in Your Environment

| Item                | Details                                                                |
| ------------------- | ---------------------------------------------------------------------- |
| Network             | A subnet/network with outbound internet access                         |
| Deployment Platform | AWS ECS, Kubernetes (EKS, GKE, AKS, etc.), or other container platform |
| Permissions         | Ability to create required resources on your chosen platform           |

## Creating an Executor Pool

Before deploying, create a Pool in DataHub Cloud:

1. Navigate to **Data Sources** in the left sidebar
2. Click the **Executors** tab
3. Click **Create**

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/pool-list-before.png?raw=true"/>
</p>

4. Configure Pool settings:
   - **Pool Identifier**: A unique identifier for this Pool (you'll use this in deployment)
   - **Description**: Purpose or details about the Pool
   - **Set as Default**: Enable this toggle to make this Pool the default for new Ingestion Sources

<p align="center">
  <img width="85%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/configure-pool-modal.png?raw=true"/>
</p>

5. Click **Create** to provision the Pool

:::note
Save the **Pool Identifier** - you'll need it for the `ExecutorPoolId` parameter during deployment.
:::

## Deploying Remote Executors

<Tabs>
<TabItem value="ecs" label="Amazon ECS">

### Deploy on Amazon ECS

#### Step 1: Request ECR Access

Share your **AWS Account ID** with DataHub so they can grant access to the private ECR registry:

- Contact your DataHub Cloud representative, or
- Use a secure sharing service like [One Time Secret](https://onetimesecret.com/)

:::note
Wait for confirmation from DataHub that ECR access has been granted before proceeding.
:::

#### Step 2: Download the CloudFormation Template

Download the [CloudFormation Template](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/remote-executor/datahub-executor.ecs.template.yaml).

#### Step 3: Deploy the Stack

**Required Parameters:**

| Parameter        | Description                                              | Example                     |
| ---------------- | -------------------------------------------------------- | --------------------------- |
| `VPCID`          | Your VPC ID                                              | `vpc-12345678`              |
| `SubnetID`       | Subnet with outbound internet access                     | `subnet-12345678`           |
| `DataHubBaseUrl` | Your DataHub Cloud URL (must include `/gms`)             | `https://acme.acryl.io/gms` |
| `ExecutorPoolId` | Pool Identifier from "Creating an Executor Pool" section | `remote`                    |

**Access Token (required - provide one of the following):**

| Parameter                             | Description                                           | Example                                                    |
| ------------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------- |
| `DataHubAccessToken`                  | Remote Executor access token (plaintext)              | `eyJ...`                                                   |
| `ExistingDataHubAccessTokenSecretArn` | ARN of existing Secrets Manager secret with the token | `arn:aws:secretsmanager:us-west-2:123456789:secret:my-tok` |

**Optional Parameters:**

| Parameter                       | Description                                                          |
| ------------------------------- | -------------------------------------------------------------------- |
| `ImageTag`                      | Executor version                                                     |
| `DesiredCount`                  | Number of executor replicas                                          |
| `TaskCpu`                       | CPU units (256, 512, 1024, 2048, 4096)                               |
| `TaskMemory`                    | Memory in MiB                                                        |
| `TaskEphemeralStorageSizeInGiB` | Ephemeral storage in GiB                                             |
| `AwsRegion`                     | AWS region for the executor                                          |
| `OptionalSecrets`               | Data source secrets (format: `NAME=ARN,NAME2=ARN2`)                  |
| `OptionalEnvVars`               | Additional environment variables (format: `NAME=VALUE,NAME2=VALUE2`) |

:::warning
The following parameters should only be changed after consulting with your DataHub representative:
`DataHubIngestionsMaxWorkers`, `DataHubMonitorsMaxWorkers`, `DataHubIngestionsSignalPollInterval`
:::

**Deploy using AWS CLI:**

```bash
aws cloudformation create-stack \
  --stack-name datahub-remote-executor \
  --template-body file://datahub-executor.ecs.template.yaml \
  --capabilities CAPABILITY_AUTO_EXPAND CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=VPCID,ParameterValue="<your-vpc>" \
    ParameterKey=SubnetID,ParameterValue="<your-subnet>" \
    ParameterKey=DataHubBaseUrl,ParameterValue="https://<your-company>.acryl.io/gms" \
    ParameterKey=ExecutorPoolId,ParameterValue="<your-pool-id>" \
    ParameterKey=DataHubAccessToken,ParameterValue="<your-token>"
```

Or use the [CloudFormation Console](https://console.aws.amazon.com/cloudformation).

#### Step 4: Configure Secrets for Data Sources (Optional)

To use secrets in your ingestion recipes without storing them in DataHub:

1. Create secrets in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name my-database-password \
  --secret-string 'your-secret-value'
```

2. Pass secrets to the CloudFormation stack using the `OptionalSecrets` parameter:

```
OptionalSecrets=DB_PASSWORD=arn:aws:secretsmanager:us-west-2:123456789012:secret:my-database-password
```

Format: `SECRET_NAME=SECRET_ARN` (comma-separated for multiple, up to 10, no spaces)

3. Reference in your ingestion recipe:

```yaml
source:
  type: postgres
  config:
    password: "${DB_PASSWORD}"
```

### Update ECS Deployment

To update your deployment (e.g., new version or configuration change):

1. Navigate to **AWS CloudFormation Console**
2. Select your Remote Executor stack
3. Click **Update**
4. Select **Replace current template** and upload the latest template
5. Update parameters as needed (e.g., `ImageTag` for version upgrade)
6. Click **Next**, review, and **Update stack**

</TabItem>
<TabItem value="k8s" label="Kubernetes">

### Deploy on Kubernetes

The [datahub-executor-worker](https://executor-helm.acryl.io/index.yaml) Helm chart deploys Remote Executors on any Kubernetes cluster (EKS, GKE, AKS, etc.).

#### Step 1: Request Registry Access

Contact your DataHub Cloud representative to set up access to the container registry. Provide:

- **AWS EKS**: The IAM principal that will pull from the ECR repository
- **Google Cloud GKE**: The cluster's IAM service account
- **Other platforms**: Contact DataHub team for specific requirements

:::note
Wait for confirmation from DataHub that registry access has been granted before proceeding.
:::

#### Step 2: Create Secrets

```bash
# Create DataHub access token secret (required)
kubectl create secret generic datahub-access-token-secret \
  --from-literal=datahub-access-token-secret-key=<your-remote-executor-token>

# Create source credentials (optional)
kubectl create secret generic datahub-secret-store \
  --from-literal=SNOWFLAKE_PASSWORD=<password> \
  --from-literal=REDSHIFT_PASSWORD=<password>
```

#### Step 3: Install Helm Chart

```bash
# Add Helm repository
helm repo add acryl https://executor-helm.acryl.io
helm repo update

# Install the chart
helm install acryl-executor-worker acryl/datahub-executor-worker \
  --set global.datahub.gms.url="https://<your-company>.acryl.io/gms" \
  --set global.datahub.executor.pool_id="<your-pool-id>"
```

**Required Helm Values:**

| Value                             | Description                                              | Example                     |
| --------------------------------- | -------------------------------------------------------- | --------------------------- |
| `global.datahub.gms.url`          | Your DataHub Cloud URL (must include `/gms`)             | `https://acme.acryl.io/gms` |
| `global.datahub.executor.pool_id` | Pool Identifier from "Creating an Executor Pool" section | `remote`                    |

**Access Token (required - uses the secret created in Step 2):**

The Helm chart references the Kubernetes Secret you created. These defaults match the secret name from Step 2:

| Value                          | Default                           | Description            |
| ------------------------------ | --------------------------------- | ---------------------- |
| `global.datahub.gms.secretRef` | `datahub-access-token-secret`     | Kubernetes Secret name |
| `global.datahub.gms.secretKey` | `datahub-access-token-secret-key` | Key within the Secret  |

**Optional Helm Values:**

| Value                                            | Description                      |
| ------------------------------------------------ | -------------------------------- |
| `image.tag`                                      | Executor version                 |
| `replicaCount`                                   | Number of executor replicas      |
| `resources.requests.cpu`                         | CPU allocation                   |
| `resources.requests.memory`                      | Memory allocation                |
| `global.datahub.executor.ingestions.max_workers` | Max concurrent ingestion tasks   |
| `extraVolumes`                                   | Additional volumes for secrets   |
| `extraVolumeMounts`                              | Volume mount paths               |
| `extraEnvs`                                      | Additional environment variables |

#### Step 4: Configure Secret Mounting (Optional)

Starting from DataHub Cloud v0.3.8.2, you can mount secrets from Kubernetes Secrets:

```yaml
# values.yaml
extraVolumes:
  - name: datahub-secret-store
    secret:
      secretName: datahub-secret-store
extraVolumeMounts:
  - mountPath: /mnt/secrets
    name: datahub-secret-store
```

Reference in your ingestion recipe:

```yaml
source:
  type: snowflake
  config:
    password: "${SNOWFLAKE_PASSWORD}"
```

:::note
Default mount path: `/mnt/secrets` (override with `DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR`). Reference secrets using `${SECRET_NAME}` syntax.
:::

For additional configuration options, refer to the [values.yaml](https://github.com/acryldata/datahub-executor-helm/blob/main/charts/datahub-executor-worker/values.yaml) file.

</TabItem>
</Tabs>

## Checking Remote Executor status

## Verifying Your Deployment

After deployment, verify the executor is running:

**Check logs for success message:**

<Tabs>
<TabItem value="ecs" label="Amazon ECS">

1. Navigate to **ECS > Clusters > [your-stack-name] > Tasks**
2. Click on the running task
3. Go to **Logs** tab
4. Look for: `celery worker initialization finished`

</TabItem>
<TabItem value="k8s" label="Kubernetes">

```bash
kubectl logs -l app.kubernetes.io/name=datahub-executor-worker | grep "celery worker initialization finished"
```

</TabItem>
</Tabs>

**Check Pool status in DataHub UI:**

Once deployed, DataHub will show the executor status:

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/pool-list-after.png?raw=true"/>
</p>

## Assigning Ingestion Sources to a Pool

After verifying your deployment:

1. Navigate to **Data Sources** in DataHub Cloud
2. Edit an existing source or click **Create new source**
3. In the **Finish Up** step, expand **Advanced**
4. Select your **Executor Pool** from the dropdown

<p align="center">
  <img width="80%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/ingestion-select-executor-pool.png?raw=true"/>
</p>

5. Click **Save & Run**

:::note
New Ingestion Sources will automatically use your designated Default Pool if you have assigned one.
:::

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/view-ingestion-running.png?raw=true"/>
</p>

## Advanced: Performance Settings

Executors use a weight-based queuing system to manage resource allocation:

- **Default Behavior**: With 4 ingestion threads (default), each task gets a weight of 0.25, allowing up to 4 parallel tasks
- **Resource-Intensive Tasks**: Tasks can be assigned a higher weight (up to 1.0) to limit parallelism
- **Queue Management**: If the total weight of running tasks exceeds 1.0, new tasks are queued
- **Priority Tasks**: Setting a weight of 1.0 ensures exclusive resource access

The following can be configured per Ingestion Source under **Extra Environment Variables**:

- `EXECUTOR_TASK_MEMORY_LIMIT` - Memory limit per task in kilobytes
  ```json
  { "EXECUTOR_TASK_MEMORY_LIMIT": "128000000" }
  ```
- `EXECUTOR_TASK_WEIGHT` - Task weight for resource allocation (default: 0.25 with 4 threads)
  ```json
  { "EXECUTOR_TASK_WEIGHT": "1.0" }
  ```

## Troubleshooting

### Common Issues

1. **Connection Failed**

   - Verify network connectivity (outbound HTTPS to `*.acryl.io`)
   - Check DataHub URL includes `/gms` suffix
   - Validate Executor Pool ID matches exactly
   - Validate access token is Remote Executor type (not Personal Access Token)

2. **Secret Access Failed**

   - Confirm secret ARNs are correct
   - Check IAM permissions for Secrets Manager
   - Verify secret format matches expected structure

3. **Container Failed to Start**
   - Verify ECR registry access was granted by DataHub
   - Check resource limits (memory/CPU)
   - Review container logs for specific errors

### FAQ

**Do AWS Secrets Manager secrets automatically update in the executor?**

No. Secrets are injected at container startup. Restart the ECS task to pick up secret changes.

**How do I upgrade the executor version?**

Update the `ImageTag` parameter (for ECS) or `image.tag` value (for Kubernetes), then redeploy. See [Update ECS Deployment](#update-ecs-deployment) for detailed steps.

**Can I run multiple executors in the same Pool?**

Yes. Increase `DesiredCount` in CloudFormation or `replicaCount` in Helm to run multiple executor instances for high availability.
