---
title: Configuring Remote Executor
description: Learn how to set up, deploy, and configure Remote Executors in your environment
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Configuring Remote Executor
<FeatureAvailability saasOnly />

## Overview

This guide will walk you through the process of setting up Remote Executors in your environment, including:
1. Understanding Remote Executors and Pools
2. Configuration prerequisites
3. Creating and managing Executor Pools
4. Deploying Remote Executors in your environment
5. Assigning Ingestion Sources to Pools

## Understanding Remote Executors and Pools

A Remote Executor Pool provides a way to organize and manage your Remote Executors in DataHub. Here's how they work:

- A **Remote Executor Pool** (or **Pool**) is a logical grouping of one or more Remote Executors
- Each **Remote Executor** in a Pool automatically reports its status and health
- Each **Ingestion Source** can be assigned to a specific Pool
- One Pool can be designated as the **Default Pool** for new Ingestion Sources
- Multiple Pools can be created for different purposes (e.g., by region, department, or workload type)

## Configuration Prerequisites

Before deploying a Remote Executor, ensure you have the following:

1. **DataHub Cloud**
   - A DataHub User with the **Manage Metadata Ingestion** Platform privilege
   - A DataHub Remote Executor Access Token (generate from **Settings > Access Tokens > Generate new token > Remote Executor**)
   - Your DataHub Cloud URL (e.g., `<your-company>.acryl.io/gms`). **NOTE:** you MUST include the trailing `/gms` when configuring the executor.

2. **Deployment Environment**
   - Access to your deployment platform (AWS ECS or Kubernetes)
   - Necessary permissions to create resources

3. **Registry Access**
   - For AWS: Provide your AWS account ID to Acryl
   - For Kubernetes: Work with Acryl to set up access to the Remote Executor Docker Image Registry

## Creating and managing Executor Pools

Complete the following steps to create a new Executor Pool from the DataHub Cloud UI:

1. Navigate to the **Data Sources** section on the left sidebar
2. Click the **Executors** tab and click **Create**

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/pool-list-before.png?raw=true"/>
</p>

3. Configure Pool settings:
   - **Pool Identifier**: A unique identifier for this Pool
   - **Description**: Purpose or details about the Pool
   - **Default Pool**: Optionally set as the Default Pool for new Ingestion Sources

<p align="center">
  <img width="85%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/configure-pool-modal.png?raw=true"/>
</p>

4. Click **Create** to provision the Pool

## Deploying Remote Executors in Your Environment

Once you have created an Executor Pool in DataHub Cloud, you are now ready to deploy an Executor within your environment.

:::note
Work with Acryl to receive deployment templates specific to your environment (Helm charts, CloudFormation, or Terraform) for deploying Remote Executors in this Pool.
:::

<Tabs>
<TabItem value="ecs" label="Amazon ECS">

### Deploy on Amazon ECS

1. **Configure CloudFormation Template**
The Acryl Team will provide a [Cloudformation Template](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/remote-executor/datahub-executor.ecs.template.yaml) that you can run to provision an ECS cluster with a single remote ingestion task. It will also provision an AWS role for the task which grants the permissions necessary to read and delete from the private queue created for you, along with reading the secrets you've specified. At minimum, the template requires the following parameters:

   - Deployment Location (VPC and subnet)
   - DataHub Personal Access Token
   - DataHub Cloud URL (e.g., `<your-company>.acryl.io/gms`)
   - Optional: Acryl Remote Executor Version; defaults to latest

   Optional parameters:
   - Source Secrets: `SECRET_NAME=SECRET_ARN` (up to 10); separate multiple secrets by comma, e.g. `SECRET_NAME_1=SECRET_ARN_1,SECRET_NAME_2,SECRET_ARN_2`.
   - Environment Variables: `ENV_VAR_NAME=ENV_VAR_VALUE` (up to 10); separate multiple variable by comma, e.g. `ENV_VAR_NAME_1=ENV_VAR_VALUE_1,ENV_VAR_NAME_2,ENV_VAR_VALUE_2`.

:::note
Configuring Secrets enables you to manage ingestion sources from the DataHub UI without storing credentials inside DataHub. Once defined, secrets can be referenced by name inside of your DataHub Ingestion Source configurations using the usual convention: `${SECRET_NAME}`.
:::

2. **Deploy Stack**
   ```bash
   # Using AWS CLI
   aws cloudformation create-stack \
     --stack-name datahub-remote-executor \
     --template-body file://datahub-executor.ecs.template.yaml \
     --parameters ParameterKey=... ParameterValue=...
   ```

   Or use the [CloudFormation Console](https://console.aws.amazon.com/cloudformation)

3. **Configure Secrets (Optional)**
   ```bash
   # Create a secret in AWS Secrets Manager
   aws secretsmanager create-secret \
     --name my-source-secret \
     --secret-string '{"username":"user","password":"pass"}'
   ```

</TabItem>
<TabItem value="k8s" label="Kubernetes">

### Deploy on Kubernetes

1. **Add Helm Repository**
   ```bash
   helm repo add acryl https://executor-helm.acryl.io
   helm repo update
   ```

2. **Create Secrets**
   ```bash
   # Create DataHub PAT secret
   kubectl create secret generic datahub-access-token-secret \
     --from-literal=datahub-access-token-secret-key=<DATAHUB-ACCESS-TOKEN>

   # Create source credentials (optional)
   kubectl create secret generic datahub-secret-store \
     --from-literal=REDSHIFT_PASSWORD=password \
     --from-literal=SNOWFLAKE_PASSWORD=password
   ```

3. **Install Helm Chart**
   ```bash
   helm install \
     --set global.datahub.executor.pool_id="remote" \
     --set global.datahub.gms.url="https://<your-company>.acryl.io/gms" \
     --set image.tag=v0.3.1 \
     acryl datahub-executor-worker
   ```

4. **Mount Secrets (Optional)**
   ```yaml
   # In your values.yaml
   extraVolumes:
     - name: datahub-secret-store
       secret:
         secretName: datahub-secret-store
   extraVolumeMounts:
     - mountPath: /mnt/secrets
       name: datahub-secret-store
   ```

</TabItem>
</Tabs>

Once you have successfully deployed the Executor in your environment, DataHub will automatically begin reporting Executor Status in the UI:

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/pool-list-after.png?raw=true"/>
</p>

## Assigning Ingestion Sources to an Executor Pool

After you have created an Executor Pool and deployed the Executor within your environment, you are now ready to configure an Ingestion Source to run in that Pool.

1. Navigate to **Manage Data Sources** in DataHub Cloud 
2. Edit an existing Source or click **Create new source**
3. In the **Finish Up** step, expand the **Advanced** to select your desired **Executor Pool**

<p align="center">
  <img width="80%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/ingestion-select-executor-pool.png?raw=true"/>
</p>

:::note
New Ingestion Sources will automatically use your designated Default Pool if you have assigned one. You can override this assignment when creating or editing an Ingestion Source at any time.
:::

5. Click **Save & Run**. The task should show as 'Running' if properly configured.

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/view-ingestion-running.png?raw=true"/>
</p>

## Advanced: Performance Settings and Task Weight-Based Queuing

Executors use a weight-based queuing system to manage resource allocation efficiently:
- **Default Behavior**: With 4 ingestion threads (default), each task gets a weight of 0.25, allowing up to 4 parallel tasks
- **Resource-Intensive Tasks**: Tasks can be assigned a higher weight (up to 1.0) to limit parallelism
- **Queue Management**: If the total weight of running tasks exceeds 1.0, new tasks are queued until capacity becomes available
- **Priority Tasks**: Setting a weight of 1.0 ensures exclusive resource access - the task will run alone until completion

The following environment variables can be configured to manage memory-intensive ingestion tasks, prevent resource contention, and ensure stable execution of resource-demanding processes:

- `DATAHUB_EXECUTOR_INGESTION_MAX_WORKERS` (default: 4) - Maximum concurrent Ingestion tasks
- `DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS` (default: 10) - Maximum concurrent Observe monitoring tasks
- `EXECUTOR_TASK_MEMORY_LIMIT` - Memory limit per task in kilobytes, configured per Ingestion Source under **Extra Environment Variables**. This setting helps prevent the executor's master process from being OOM-killed and protects against memory-leaking ingestion tasks. Example configuration:
  ```json
  {"EXECUTOR_TASK_MEMORY_LIMIT": "128000000"}
  ```
- `EXECUTOR_TASK_WEIGHT` - Task weight for resource allocation, configured per Ingestion Source under **Extra Environment Variables**. By default, each task is assigned a weight of 1/MAX_THREADS (e.g., 0.25 with 4 threads). The total weight of concurrent tasks cannot exceed 1.0. Example configuration for a resource-intensive task:
  ```json
  {"EXECUTOR_TASK_WEIGHT": "1.0"}
  ```

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify network connectivity
   - Check DataHub URL configuration
   - Validate access token

2. **Secret Access Failed**
   - Confirm secret ARNs/names
   - Check permissions
   - Verify secret format

3. **Container Failed to Start**
   - Check resource limits
   - Verify registry access
   - Review container logs

### Frequently Asked Questions

**Do AWS Secrets Manager secrets automatically update in the executor?**
No. Secrets are wired into the executor container at deployment time. The ECS Task needs to be restarted when secrets change.

**How can I verify successful deployment?**
For ECS deployments, check AWS Console:
1. Navigate to **ECS > Cluster > Stack Name > Services > Logs**
2. Look for the log line: `Starting datahub executor worker`

This indicates successful connection to DataHub Cloud. 