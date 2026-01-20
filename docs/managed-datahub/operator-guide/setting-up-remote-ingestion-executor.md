---
title: Configuring Remote Executor
description: Learn how to set up, deploy, and configure Remote Executors in your environment
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Configuring Remote Executor

<FeatureAvailability saasOnly />

## Overview

Remote Executors allow you to run metadata ingestion within your private network without exposing your data sources to the internet. The executor runs in **your** environment, connects to **your** databases, and sends only metadata back to DataHub Cloud.

**What you'll do:**

1. Prepare prerequisites (credentials, permissions, networking)
2. Create an Executor Pool in DataHub Cloud UI
3. Deploy the Remote Executor in your environment
4. Verify the deployment is working
5. Assign ingestion sources to your Pool

:::info
For an overview of how Remote Executors work, including the architecture diagram and security model, see [About Remote Executor](../remote-executor/about.md).
:::

## Prerequisites

Before deploying a Remote Executor, ensure you have the following:

### 1. DataHub Cloud Access

| Requirement                                       | How to Get It                                                                   |
| ------------------------------------------------- | ------------------------------------------------------------------------------- |
| User with **Manage Metadata Ingestion** privilege | Admin grants in Settings → Users & Groups                                       |
| Remote Executor Access Token                      | Settings → Access Tokens → Generate new token → Select **Remote Executor** type |
| Your DataHub Cloud URL                            | Format: `https://<your-company>.acryl.io/gms` (**must** include `/gms`)         |

:::warning
You must generate a **Remote Executor** type token, not a regular Personal Access Token. Using the wrong token type will cause authentication failures. Tokens do not expire by default, but your admin may configure expiration policies.
:::

### 2. Deployment Environment

- Access to your deployment platform (AWS ECS or Kubernetes)
- Necessary permissions to create resources (ECS tasks, K8s deployments, secrets)

### 3. Registry Access

- **For AWS ECS**: Provide your AWS account ID to DataHub Cloud to grant ECR access
- **For Kubernetes**: Work with DataHub team to set up access to the Remote Executor Docker image registry

### 4. AWS Permissions (Required for All Deployments)

Regardless of where you deploy the executor (ECS, Kubernetes, etc.), it needs AWS permissions to interact with SQS (task queue) and optionally S3 (log storage). The SQS queue is hosted in DataHub Cloud's AWS infrastructure.

<details>
<summary>IAM Policy Example (click to expand)</summary>

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SQSAccess",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl"
      ],
      "Resource": "<SQS queue ARN provided by DataHub>"
    },
    {
      "Sid": "S3LogUpload",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::<your-log-bucket>/*"
    }
  ]
}
```

:::note
Contact your DataHub Cloud representative for the exact SQS queue ARN pattern for your tenant.
:::

</details>

### 5. Network Requirements

The executor makes **outbound-only** connections. Ensure your firewall/security groups allow:

| Direction | Destination               | Port   | Purpose                    |
| --------- | ------------------------- | ------ | -------------------------- |
| Outbound  | `<your-company>.acryl.io` | 443    | DataHub Cloud API          |
| Outbound  | `sqs.*.amazonaws.com`     | 443    | Task queue                 |
| Outbound  | `s3.*.amazonaws.com`      | 443    | Log upload (if configured) |
| Outbound  | Your data sources         | Varies | Metadata extraction        |

### 6. Version Compatibility

The executor version should be compatible with your DataHub Cloud version. Using mismatched versions may cause recipe parsing errors or unexpected behavior. Check your DataHub Cloud version in **Settings → Platform → Version**.

## Creating and Managing Executor Pools

An **Executor Pool** is a logical grouping of one or more Remote Executors. You create a Pool in DataHub Cloud, then deploy executors that connect to that Pool.

### Create a New Pool

1. Navigate to **Ingestion** in the left sidebar
2. Click the **Executors** tab
3. Click **Create**

<p align="center">
  <img width="90%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/pool-list-before.png?raw=true"/>
</p>

4. Configure Pool settings:
   - **Pool Identifier**: A unique identifier (e.g., `production`, `us-west-2`, `data-team`)
   - **Description**: Purpose or details about the Pool
   - **Default Pool**: Optionally set as the default for new ingestion sources

<p align="center">
  <img width="85%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/configure-pool-modal.png?raw=true"/>
</p>

5. Click **Create** to provision the Pool

:::warning Critical: Remember Your Pool ID
The **Pool Identifier** you enter here must **exactly match** the `pool_id` configuration when you deploy the executor.

For example, if you create a Pool with ID `production`, your executor must be configured with `pool_id: "production"`.

If they don't match, tasks will be sent to a queue that your executor isn't listening to, and ingestion will stay stuck in "Pending".

**Forgot your Pool ID?** Navigate to **Ingestion → Executors** and click on your Pool to view its identifier.
:::

## Deploying Remote Executors

Once you've created an Executor Pool, deploy an executor in your environment that connects to it.

<Tabs>
<TabItem value="ecs" label="Amazon ECS">

### Deploy on Amazon ECS

#### Step 1: AWS Account Configuration

To access the private DataHub Cloud ECR registry, provide your AWS account ID to DataHub Cloud:

- Contact your DataHub Cloud representative, or
- Use a secure secret-sharing service like [One Time Secret](https://onetimesecret.com/)

This grants your AWS account permission to pull the Remote Executor container image.

#### Step 2: Configure CloudFormation Template

The DataHub Team will provide a [CloudFormation Template](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/remote-executor/datahub-executor.ecs.template.yaml) that provisions:

- An ECS cluster with a Remote Executor task
- IAM roles with necessary permissions
- Security groups for outbound access

**Required parameters:**

| Parameter            | Description                          | Example                     |
| -------------------- | ------------------------------------ | --------------------------- |
| `ExecutorPoolId`     | Must match your Pool ID from the UI  | `production`                |
| `VPCID`              | VPC where executor will run          | `vpc-12345678`              |
| `SubnetID`           | Subnet with outbound internet access | `subnet-12345678`           |
| `DataHubBaseUrl`     | Your DataHub Cloud URL (with `/gms`) | `https://acme.acryl.io/gms` |
| `DataHubAccessToken` | Remote Executor access token         | `eyJ...`                    |

**Optional parameters:**

| Parameter              | Description                           | Example                              |
| ---------------------- | ------------------------------------- | ------------------------------------ |
| `ImageTag`             | Executor version (defaults to latest) | `v0.3.15.4`                          |
| `SourceSecrets`        | AWS Secrets Manager secrets           | `DB_PASS=arn:aws:secretsmanager:...` |
| `EnvironmentVariables` | Additional env vars                   | `LOG_LEVEL=DEBUG`                    |

#### Step 3: Deploy Stack

```bash
aws --region <your-region> cloudformation create-stack \
  --stack-name datahub-remote-executor \
  --template-body file://datahub-executor.ecs.template.yaml \
  --capabilities CAPABILITY_AUTO_EXPAND CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=ExecutorPoolId,ParameterValue="production" \
    ParameterKey=VPCID,ParameterValue="<your-vpc>" \
    ParameterKey=SubnetID,ParameterValue="<your-subnet>" \
    ParameterKey=DataHubBaseUrl,ParameterValue="https://<your-company>.acryl.io/gms" \
    ParameterKey=DataHubAccessToken,ParameterValue="<your-token>"
```

Or use the [CloudFormation Console](https://console.aws.amazon.com/cloudformation).

#### Step 4: Configure Secrets (Optional)

To use secrets in your ingestion recipes without storing them in DataHub:

```bash
# Create a secret in AWS Secrets Manager
aws secretsmanager create-secret \
  --name my-database-password \
  --secret-string 'your-secret-password'
```

Map the secret to an environment variable in CloudFormation using the `SourceSecrets` parameter:

```
DB_PASSWORD=arn:aws:secretsmanager:<region>:<account>:secret:my-database-password
```

Then reference in your ingestion recipe:

```yaml
source:
  type: postgres
  config:
    password: "${DB_PASSWORD}"
```

### Update ECS Deployment

To update your Remote Executor deployment (e.g., to deploy a new container version or modify configuration), you'll need to update your existing CloudFormation Stack. This process involves re-deploying the CloudFormation template with your updated parameters while preserving your existing resources.

1. **Access CloudFormation**

   - Navigate to the AWS CloudFormation Console
   - Locate and select your Remote Executor stack
   - Click the **Update** button

2. **Update Template**
   - Select **Replace current template**
   - Choose **Upload a template file**
   - Download the latest DataHub Cloud Remote Executor [CloudFormation Template](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/remote-executor/datahub-executor.ecs.template.yaml)
   - Upload the template file

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/Screen-Shot-2023-01-19-at-4.23.32-PM.png"/>
</p>

3. **Configure Parameters**

   - Review and update parameters as needed:
     - `ImageTag`: Specify a new version if upgrading
     - `DataHubBaseUrl`: Verify your DataHub URL is correct
     - Other parameters will retain their previous values unless changed
   - Click **Next** to proceed

4. **Review and Deploy**
   - Review the configuration changes
   - Acknowledge any capabilities if prompted
   - Click **Update stack** to begin the update process

:::note
The update process will maintain your existing resources (e.g., secrets, IAM roles) while deploying the new configuration. Monitor the stack events to track the update progress.
:::

### Deploy on Kubernetes

The [datahub-executor-worker](https://executor-helm.acryl.io/index.yaml) Helm chart deploys Remote Executors on any Kubernetes cluster (EKS, GKE, AKS, etc.).

#### Step 1: Registry Access

Work with your DataHub Cloud representative to set up access to the container registry. Provide the following based on your platform:

- **AWS EKS**: The IAM role ARN used by your Kubernetes nodes or service account (e.g., `arn:aws:iam::123456789012:role/eks-node-role`)
- **Google Cloud GKE**: The cluster's IAM service account email (e.g., `my-sa@my-project.iam.gserviceaccount.com`)
- **Other platforms**: Contact DataHub team for registry credentials

#### Step 2: Create Namespace

```bash
kubectl create namespace datahub
```

#### Step 3: Create Secrets

```bash
# Create DataHub access token secret (required)
kubectl create secret generic datahub-remote-executor-secrets \
  -n datahub \
  --from-literal=executor-token=<your-remote-executor-token>

# Create source credentials (optional)
kubectl create secret generic datahub-source-credentials \
  -n datahub \
  --from-literal=SNOWFLAKE_PASSWORD=<password> \
  --from-literal=REDSHIFT_PASSWORD=<password>
```

#### Step 4: Install Helm Chart

```bash
# Add Helm repository
helm repo add acryl https://executor-helm.acryl.io
helm repo update
```

**Option A: Quick install with inline parameters** (for testing)

```bash
helm install acryl-executor-worker acryl/datahub-executor-worker \
  -n datahub \
  --set global.datahub.gms.url="https://<your-company>.acryl.io/gms" \
  --set global.datahub.gms.secretRef="datahub-remote-executor-secrets" \
  --set global.datahub.gms.secretKey="executor-token" \
  --set global.datahub.executor.pool_id="production"
```

**Option B: Install with values file** (recommended for production)

Create a `values.yaml` file:

```yaml
global:
  datahub:
    gms:
      url: "https://<your-company>.acryl.io/gms"
      secretRef: datahub-remote-executor-secrets
      secretKey: executor-token
    executor:
      pool_id: "production"

datahub-executor-worker:
  image:
    tag: "v0.3.15.4" # Check https://executor-helm.acryl.io for latest version
  resources:
    requests:
      memory: "2Gi"
      cpu: "1"
    limits:
      memory: "4Gi"
      cpu: "2"
```

Then install with:

```bash
helm install acryl-executor-worker acryl/datahub-executor-worker \
  -n datahub \
  -f values.yaml
```

#### Resource Sizing Guidelines

| Workload                | Memory | CPU     | Replicas | Notes                                |
| ----------------------- | ------ | ------- | -------- | ------------------------------------ |
| Light (< 50 sources)    | 2 GB   | 1 core  | 1        | Small schemas                        |
| Medium (50-200 sources) | 4 GB   | 2 cores | 2        | Typical usage                        |
| Heavy (200+ sources)    | 8 GB   | 4 cores | 2-4      | Large schemas, many concurrent tasks |

#### Step 5: Configure Secret Mounting (Optional)

For file-based secrets (available in DataHub Cloud v0.3.8.2+):

```yaml
# In values.yaml
extraVolumes:
  - name: source-credentials
    secret:
      secretName: datahub-source-credentials

extraVolumeMounts:
  - mountPath: /mnt/secrets
    name: source-credentials
```

Then reference in ingestion recipes:

```yaml
source:
  type: snowflake
  config:
    password: "${SNOWFLAKE_PASSWORD}"
```

<details>
<summary>Advanced: Raw Kubernetes Manifests</summary>

If you prefer raw manifests instead of Helm:

**ServiceAccount (for IRSA on EKS):**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: datahub-remote-executor-sa
  namespace: datahub
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<account-id>:role/DataHubRemoteExecutorRole
```

**Secret:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: datahub-remote-executor-secrets
  namespace: datahub
type: Opaque
stringData:
  executor-token: "<your-remote-executor-token>"
```

**ConfigMap:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: datahub-executor-config
  namespace: datahub
data:
  DATAHUB_GMS_URL: "https://<your-company>.acryl.io/gms"
  DATAHUB_EXECUTOR_POOL_ID: "production"
```

**Deployment:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datahub-remote-executor
  namespace: datahub
spec:
  replicas: 2
  selector:
    matchLabels:
      app: datahub-remote-executor
  template:
    metadata:
      labels:
        app: datahub-remote-executor
    spec:
      serviceAccountName: datahub-remote-executor-sa
      containers:
        - name: executor
          image: acryldata/datahub-executor:v0.3.15.4 # Check https://executor-helm.acryl.io for latest
          env:
            - name: DATAHUB_GMS_URL
              valueFrom:
                configMapKeyRef:
                  name: datahub-executor-config
                  key: DATAHUB_GMS_URL
            - name: DATAHUB_GMS_TOKEN
              valueFrom:
                secretKeyRef:
                  name: datahub-remote-executor-secrets
                  key: executor-token
            - name: DATAHUB_EXECUTOR_POOL_ID
              valueFrom:
                configMapKeyRef:
                  name: datahub-executor-config
                  key: DATAHUB_EXECUTOR_POOL_ID
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "4Gi"
              cpu: "2"
          livenessProbe:
            exec:
              command: ["find", "/tmp/worker_liveness_heartbeat", "-mmin", "-1"]
            initialDelaySeconds: 60
            periodSeconds: 30
          readinessProbe:
            exec:
              command:
                ["find", "/tmp/worker_readiness_heartbeat", "-mmin", "-1"]
            initialDelaySeconds: 10
            periodSeconds: 10
```

Apply with:

```bash
kubectl apply -f serviceaccount.yaml
kubectl apply -f secret.yaml
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
```

</details>

## Checking Remote Executor status

## Verifying Your Deployment

After deploying, verify the executor is running and connected.

### Step 1: Check Executor Logs

<Tabs>
<TabItem value="ecs" label="Amazon ECS">

```bash
# View logs in CloudWatch
aws logs tail /ecs/datahub-remote-executor --follow

# Or via ECS Console:
# ECS → Clusters → datahub-remote-executor → Tasks → Logs
```

</TabItem>
<TabItem value="k8s" label="Kubernetes">

```bash
kubectl logs -n datahub -l app.kubernetes.io/name=datahub-executor-worker -f
```

</TabItem>
<TabItem value="docker" label="Docker (Local Testing)">

```bash
docker logs -f datahub-executor
```

</TabItem>
</Tabs>

**Expected output (success):**

```
[INFO] Starting datahub executor worker
[INFO] Connecting to DataHub at https://acme.acryl.io/gms
[INFO] Executor pool: production
[INFO] celery worker initialization finished   ← This means success!
```

**If you see errors**, check the [Troubleshooting](#troubleshooting) section.

### Step 2: Check Pool Status in UI

1. Navigate to **Ingestion → Executors**
2. Find your Pool
3. Status should show **Healthy** with executor count ≥ 1

<p align="center">
  <img width="90%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/pool-list-after.png?raw=true"/>
</p>

### Step 3: Run a Test Ingestion

1. Go to **Ingestion → Sources**
2. Create or edit an ingestion source
3. In **Advanced** settings, select your **Executor Pool**
4. Click **Save & Run**
5. Watch the executor logs—you should see:

```
[INFO] Received task: RUN_INGEST
[INFO] Starting ingestion for source: test-source
[INFO] Ingestion completed successfully
```

### Verification Checklist

| Check                                        | Expected Result | If Failed                      |
| -------------------------------------------- | --------------- | ------------------------------ |
| Executor logs show "initialization finished" | ✅              | Check GMS URL, token, network  |
| Pool shows "Healthy" in UI                   | ✅              | Check Pool ID matches          |
| Test ingestion starts running                | ✅              | Check executor logs for errors |
| Ingestion completes successfully             | ✅              | Check data source connectivity |

## Assigning Ingestion Sources to an Executor Pool

After verifying your deployment, assign ingestion sources to run on your executor:

1. Navigate to **Ingestion → Sources**
2. Edit an existing source or click **Create new source**
3. In the **Finish Up** step, expand **Advanced**
4. Select your **Executor Pool** from the dropdown

<p align="center">
  <img width="80%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/ingestion-select-executor-pool.png?raw=true"/>
</p>

5. Click **Save & Run**

:::tip
If you set a **Default Pool**, new ingestion sources will automatically use it. You can override this per-source.
:::

<p align="center">
  <img width="90%" src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/view-ingestion-running.png?raw=true"/>
</p>

## Configuration Reference

### Essential Configuration

| Variable                   | Description                             | Required |
| -------------------------- | --------------------------------------- | -------- |
| `DATAHUB_GMS_URL`          | DataHub Cloud URL (must include `/gms`) | ✅ Yes   |
| `DATAHUB_GMS_TOKEN`        | Remote Executor access token            | ✅ Yes   |
| `DATAHUB_EXECUTOR_POOL_ID` | Must match Pool ID in UI                | ✅ Yes   |

### AWS Configuration

| Variable                | Description                             | Default |
| ----------------------- | --------------------------------------- | ------- |
| `AWS_REGION`            | AWS region for SQS                      | -       |
| `AWS_ACCESS_KEY_ID`     | AWS access key (if not using IAM roles) | -       |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key (if not using IAM roles) | -       |

### Performance Tuning

| Variable                                  | Description                      | Default       |
| ----------------------------------------- | -------------------------------- | ------------- |
| `DATAHUB_EXECUTOR_INGESTION_MAX_WORKERS`  | Max concurrent ingestion tasks   | `4`           |
| `DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS`   | Max concurrent monitoring tasks  | `10`          |
| `DATAHUB_EXECUTOR_SQS_VISIBILITY_TIMEOUT` | SQS visibility timeout (seconds) | `43200` (12h) |

### Logging (S3)

| Variable                   | Description              | Default |
| -------------------------- | ------------------------ | ------- |
| `DATAHUB_CLOUD_LOG_BUCKET` | S3 bucket for log upload | -       |
| `DATAHUB_CLOUD_LOG_PATH`   | S3 path prefix           | -       |

### Secrets Resolution

Configuring secrets enables you to manage ingestion sources from the DataHub UI without storing credentials inside DataHub. Once defined, secrets can be referenced by name inside of your ingestion source configurations using the `${SECRET_NAME}` syntax.

Secrets are resolved in this order:

| Priority | Source               | How It Works                          |
| -------- | -------------------- | ------------------------------------- |
| 1        | Environment variable | Looks for env var `MY_PASSWORD`       |
| 2        | DataHub Secrets      | Fetches from DataHub (managed in UI)  |
| 3        | File secrets         | Reads from `/mnt/secrets/MY_PASSWORD` |

**File secrets configuration:**

| Variable                               | Description                | Default         |
| -------------------------------------- | -------------------------- | --------------- |
| `DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR` | Directory for file secrets | `/mnt/secrets`  |
| `DATAHUB_EXECUTOR_FILE_SECRET_MAXLEN`  | Max secret file size       | `1048576` (1MB) |

## Observability & Monitoring

### Prometheus Metrics

The executor exposes Prometheus metrics for monitoring:

| Setting | Value        |
| ------- | ------------ |
| Port    | `9087`       |
| Path    | `/telemetry` |

**Scrape configuration:**

```yaml
scrape_configs:
  - job_name: "datahub-executor"
    static_configs:
      - targets: ["datahub-remote-executor:9087"]
    metrics_path: /telemetry
```

**Key metrics:**

| Metric                                                       | Type    | Description                        |
| ------------------------------------------------------------ | ------- | ---------------------------------- |
| `datahub_executor_worker_ingestion_requests_total`           | Counter | Total ingestion tasks received     |
| `datahub_executor_worker_ingestion_errors_total`             | Counter | Failed ingestion tasks             |
| `datahub_executor_worker_active_threads`                     | Gauge   | Currently running tasks            |
| `datahub_executor_worker_ingestion_visibility_timeout_total` | Counter | Tasks dropped due to queue timeout |

### Kubernetes Health Probes

Add these probes to your deployment for automatic restart on failure:

```yaml
livenessProbe:
  exec:
    command: ["find", "/tmp/worker_liveness_heartbeat", "-mmin", "-1"]
  initialDelaySeconds: 60
  periodSeconds: 30
  failureThreshold: 3

readinessProbe:
  exec:
    command: ["find", "/tmp/worker_readiness_heartbeat", "-mmin", "-1"]
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3
```

### Log Locations

| Log Type            | Location                            | Purpose                           |
| ------------------- | ----------------------------------- | --------------------------------- |
| Executor logs       | stdout (kubectl logs / docker logs) | Worker process, connection status |
| Task logs (live)    | DataHub UI → Ingestion → View Logs  | Real-time ingestion output        |
| Task logs (archive) | S3 bucket (if configured)           | Historical logs                   |

## Advanced: Performance Settings

### Task Weight-Based Queuing

Executors use a weight-based system to manage concurrent tasks:

- **Default**: With 4 workers, each task gets weight `0.25`, allowing 4 parallel tasks
- **Heavy tasks**: Assign weight `1.0` to run exclusively (no other tasks during execution)
- **Queue behavior**: If total weight exceeds `1.0`, new tasks queue until capacity frees

### Per-Source Configuration

Configure these in ingestion source **Extra Environment Variables**:

```json
{
  "EXECUTOR_TASK_WEIGHT": "1.0",
  "EXECUTOR_TASK_MEMORY_LIMIT": "4000000"
}
```

| Variable                     | Description                                              | Default         |
| ---------------------------- | -------------------------------------------------------- | --------------- |
| `EXECUTOR_TASK_WEIGHT`       | Task weight (0.0-1.0). Higher = more resources reserved. | `1/MAX_WORKERS` |
| `EXECUTOR_TASK_MEMORY_LIMIT` | Memory limit per task in KB. Kills task if exceeded.     | Unlimited       |

## Troubleshooting

### Task Stuck in "Pending"

**Symptoms:** Ingestion task stays "Pending" in UI, never starts.

**Diagnosis (Kubernetes):**

```bash
# Check if executor is running
kubectl get pods -n datahub -l app.kubernetes.io/name=datahub-executor-worker

# Check executor logs for connection status
kubectl logs -n datahub -l app.kubernetes.io/name=datahub-executor-worker | grep -i "initialization\|error\|failed"
```

**Diagnosis (ECS):**

```bash
# Check if task is running
aws ecs list-tasks --cluster datahub-remote-executor --desired-status RUNNING

# Check logs in CloudWatch
aws logs tail /ecs/datahub-remote-executor --since 1h | grep -i "initialization\|error\|failed"
```

**Causes and solutions:**

| Cause                 | How to Identify                  | Solution                            |
| --------------------- | -------------------------------- | ----------------------------------- |
| Executor not running  | No pods / container not started  | Check deployment, image pull access |
| Pool ID mismatch      | Logs show different pool than UI | Fix `pool_id` to match exactly      |
| SQS permission denied | "Access Denied" in logs          | Update IAM policy                   |
| Network blocked       | Connection timeout errors        | Allow outbound HTTPS to SQS         |

---

### Out of Memory (Exit Code 137)

**Symptoms:** Task fails with "Killed", "OOMKilled", or exit code `137`.

**Cause:** Ingestion subprocess exceeded memory limit. Common with large schemas (100k+ tables/columns).

**Solutions:**

1. **Increase container memory:**

   ```yaml
   # Kubernetes
   resources:
     limits:
       memory: "8Gi"
   ```

   ```bash
   # Docker
   docker run -m 8g ...
   ```

2. **Set per-task memory limit** (prevents one task from killing others):

   In ingestion source **Extra Environment Variables**:

   ```json
   { "EXECUTOR_TASK_MEMORY_LIMIT": "4000000" }
   ```

3. **Reduce parallelism:**

   ```yaml
   env:
     - name: DATAHUB_EXECUTOR_INGESTION_MAX_WORKERS
       value: "2"
   ```

---

### Logs Not Appearing in UI

**Symptoms:** Task completes but "View Logs" shows nothing or an error.

**Cause:** S3 log upload failed or not configured.

**Diagnosis:**

```bash
kubectl logs -n datahub -l app.kubernetes.io/name=datahub-executor-worker | grep -i "s3\|upload\|log"
```

**Solutions:**

| Issue                    | Solution                                |
| ------------------------ | --------------------------------------- |
| S3 bucket not configured | Set `DATAHUB_CLOUD_LOG_BUCKET`          |
| S3 permission denied     | Add `s3:PutObject` to IAM policy        |
| Bucket doesn't exist     | Create bucket or fix name               |
| Wrong region             | Ensure AWS_REGION matches bucket region |

---

### Visibility Timeout Errors

**Symptoms:** Logs show `ExecutionRequest dropped due to exceeded SQS visibility timeout`.

**Cause:** Task waited too long in the executor's internal queue before a worker became available. To prevent duplicate execution, it was dropped.

**Solutions:**

1. **Increase workers:**

   ```yaml
   env:
     - name: DATAHUB_EXECUTOR_INGESTION_MAX_WORKERS
       value: "8"
   ```

2. **Add more executor replicas:**

   ```yaml
   replicas: 4
   ```

3. **Reduce task weight** for fast-running sources to allow more parallelism

---

### Authentication Errors (401/403)

**Symptoms:** `ConfigError: Unable to connect to GMS` or HTTP 401/403 errors.

**Diagnosis:**

```bash
# Test token validity from inside the container
kubectl exec -it -n datahub <pod-name> -- \
  curl -H "Authorization: Bearer $DATAHUB_GMS_TOKEN" \
  "$DATAHUB_GMS_URL/health"
```

**Solutions:**

| Issue                | Solution                                                    |
| -------------------- | ----------------------------------------------------------- |
| Token expired        | Generate new token in DataHub UI                            |
| Wrong token type     | Must be **Remote Executor** type, not Personal Access Token |
| URL missing `/gms`   | Add `/gms` suffix: `https://company.acryl.io/gms`           |
| Extra trailing slash | Remove trailing slash from URL                              |

---

### Connectivity Issues (Proxy/Firewall)

**Symptoms:** Timeout errors, "Connection refused", can't reach GMS or SQS.

**Diagnosis:**

```bash
# Test connectivity from inside the container
kubectl exec -it -n datahub <pod-name> -- curl -v https://<company>.acryl.io/gms/health
kubectl exec -it -n datahub <pod-name> -- curl -v https://sqs.us-west-2.amazonaws.com
```

**Solutions:**

1. **Corporate proxy:** Add proxy environment variables:

   ```yaml
   env:
     - name: HTTP_PROXY
       value: "http://proxy.corp.com:8080"
     - name: HTTPS_PROXY
       value: "http://proxy.corp.com:8080"
     - name: NO_PROXY
       value: "localhost,127.0.0.1,.internal"
   ```

2. **Firewall rules:** Allow outbound HTTPS (443) to:
   - `*.acryl.io`
   - `sqs.*.amazonaws.com`
   - `s3.*.amazonaws.com` (if using S3 logs)

---

### Container Failed to Start

**Symptoms:** Pod in CrashLoopBackOff, container exits immediately.

**Diagnosis:**

```bash
kubectl describe pod -n datahub <pod-name>
kubectl logs -n datahub <pod-name> --previous
```

**Common causes:**

| Issue                     | Solution                                  |
| ------------------------- | ----------------------------------------- |
| Image pull failed         | Check registry access, image tag          |
| Missing required env vars | Ensure GMS_URL, TOKEN, POOL_ID are set    |
| Resource limits too low   | Increase memory/CPU limits                |
| Secret not found          | Verify secret exists in correct namespace |

---

### Frequently Asked Questions

**Do AWS Secrets Manager secrets automatically update in the executor?**

No. For ECS, secrets are injected at task startup. Restart the ECS task to pick up changes. For Kubernetes with mounted secrets, updates propagate automatically (with a delay).

**How can I verify successful deployment?**

1. Check logs for `celery worker initialization finished`
2. Verify Pool shows "Healthy" in DataHub UI
3. Run a test ingestion and confirm it starts

**Can I run multiple executors in one Pool?**

Yes. Multiple executors in the same Pool provide high availability and increased throughput. Tasks are distributed across available executors.

**What happens if an executor goes down during ingestion?**

The task will eventually time out (based on SQS visibility timeout) and can be retried. For critical workloads, run multiple executor replicas.
