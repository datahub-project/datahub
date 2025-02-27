---
description: >-
  This page describes the steps required to configure a remote executor,
  which allows you to ingest metadata from private metadata sources
  using private credentials via the DataHub UI.
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Setting up Remote Ingestion Executor
<FeatureAvailability saasOnly />

## Overview

:::note

The Remote Executor can now also be used for monitoring of ingestion sources.

:::

DataHub Cloud comes packaged with an Acryl-managed executor, which is hosted inside of Acryl's environment on your behalf. However, there are certain scenarios in which an Acryl-hosted executor is not ideal or sufficient to cover all of an organization's ingestion sources. For example, if an ingestion source is hosted behind a firewall or in an environment with strict access policies, then the Acryl executor might be unable to connect to it to extract metadata.

To accommodate these cases, Acryl supports configuring a Remote Ingestion Executor which can be deployed inside of your environment – whether that is on-prem or in cloud. This setup allows you to continue leveraging the DataHub Cloud console to create, schedule, and run both ingestion and assertion monitors, all while retaining network and credential isolation.

## Deploying a Remote Executor

:::note

The Remote Ingestion Executor is only available for DataHub Cloud. Setting up a new executor requires coordination with an Acryl representative.

:::

The Remote Ingestion Executor can be deployed on several different platforms, including [Amazon ECS](#deploying-on-amazon-ecs), [Kubernetes](#deploying-on-kubernetes) (GKE, EKS or self-hosted) and others. It can also be deployed using several different methods, including [CloudFormation](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/remote-executor/datahub-executor.ecs.template.yaml) or [Terraform](https://github.com/acryldata/datahub-terraform-modules/tree/main/remote-ingestion-executor) templates for ECS, or [Helm chart](https://github.com/acryldata/datahub-executor-helm) for Kubernetes. Please reach out to your Acryl representative for other alternatives.


### Deploying on Amazon ECS

:::note

Customers migrating from the legacy DataHub Executor: migration to the new executor requires a configuration change on Acryl side. Please contact your Acryl representative for detailed guidance.

Steps you will need to perform on your end when instructed by your Acryl representative:
1. Temporarily stop your legacy DataHub Remote Executor instance (e.g. `aws ecs update-service --desired-count 0 --cluster "cluster-name" --service "service-name"`)
2. Deploy new DataHub Executor using steps below.
3. Trigger an ingestion to make sure the new executor is working as expected.
4. Tear down legacy executor ECS deployment.

:::

1. **Provide AWS account ID**: Provide Acryl with the ID of the AWS account in which the remote executor will be hosted. This will be used to grant access to the private Acryl ECR registry. The account ID can be provided to your Acryl representative via Email or [One Time Secret](https://onetimesecret.com/).

2. **Provision an Acryl Executor** (ECS)**:** Acryl team will provide a [Cloudformation Template](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/remote-executor/datahub-executor.ecs.template.yaml) that you can run to provision an ECS cluster with a single remote ingestion task. It will also provision an AWS role for the task which grants the permissions necessary to read and delete from the private queue created for you, along with reading the secrets you've specified. At minimum, the template requires the following parameters:
   1. **Deployment Location:** The AWS VPC and subnet in which the Acryl Executor task is to be provisioned.
   2. **DataHub Personal Access Token**: A valid DataHub PAT. This can be generated inside of **Settings > Access Tokens** of DataHub web application. You can alternatively create a secret in AWS Secrets Manager and refer to that by ARN.
   3. **DataHub Cloud URL**: The URL for your DataHub instance, e.g. `<your-company>.acryl.io/gms`. Note that you MUST enter the trailing /gms when configuring the executor.
   4. **Acryl Remote Executor Version:** The version of the remote executor to deploy. This is converted into a container image tag. It will be set to the latest version of the executor by default.
   5. **Source Secrets:** The template accepts up to 10 named secrets which live inside your environment. Secrets are specified using the **OptionalSecrets** parameter in the following form: `SECRET_NAME=SECRET_ARN` with multiple separated by comma, e.g. `SECRET_NAME_1=SECRET_ARN_1,SECRET_NAME_2,SECRET_ARN_2.`
   6.  **Environment Variables:** The template accepts up to 10 arbitrary environment variables. These can be used to inject properties into your ingestion recipe from within your environment. Environment variables are specified using the **OptionalEnvVars** parameter in the following form: `ENV_VAR_NAME=ENV_VAR_VALUE` with multiple separated by comma, e.g. `ENV_VAR_NAME_1=ENV_VAR_VALUE_1,ENV_VAR_NAME_2,ENV_VAR_VALUE_2.`
       ``
       ``Providing secrets enables you to manage ingestion sources from the DataHub UI without storing credentials inside DataHub. Once defined, secrets can be referenced by name inside of your DataHub Ingestion Source configurations using the usual convention: `${SECRET_NAME}`.

       Note that the only external secret provider that is currently supported is AWS Secrets Manager.

3.  **Test the Executor:** To test your remote executor:

    1. Create a new Ingestion Source by clicking **Create new Source** in the **Ingestion** tab of the DataHub console. Configure your Ingestion Recipe as though you were running it from inside of your environment.
    2.  When working with "secret" fields (passwords, keys, etc), you can refer to any "self-managed" secrets by name: `${SECRET_NAME}:`


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/Screen-Shot-2023-01-19-at-4.16.52-PM.png"/>
</p>

    3. In the 'Finish Up' step, click '**Advanced'**.
    4. Update the '**Executor Id**' form field to be  '**remote**'. This indicates that you'd like to use the remote executor.
    5. Click '**Done**'.

    Now, simple click '**Execute**' to test out the remote executor. If your remote executor is configured properly, you should promptly see the ingestion task state change to 'Running'.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/Screen-Shot-2022-03-07-at-10.23.31-AM.png"/>
</p>

#### Updating a deployment
In order to update the executor, ie. to deploy a new container version, you'll need to update the CloudFormation Stack to re-deploy the CloudFormation template with a new set of parameters.
##### Steps - AWS Console
1. Navigate to CloudFormation in AWS Console
2. Select the stack dedicated to the remote executor
3. Click **Update**
4. Select **Replace Current Template**
5. Select **Upload a template file**
6. Upload a copy of the Acryl Remote Executor [CloudFormation Template](https://raw.githubusercontent.com/acryldata/datahub-cloudformation/master/Ingestion/templates/python.ecs.template.yaml)

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/Screen-Shot-2023-01-19-at-4.23.32-PM.png"/>
</p>

7. Click **Next**
8. Change parameters based on your modifications (e.g. ImageTag, etc)
9. Click **Next**
10. Confirm your parameter changes, and update. This should perform the necessary upgrades.

### Deploying on Kubernetes

The Helm chart [datahub-executor-worker](https://executor-helm.acryl.io/index.yaml) can be used to deploy on a Kubernetes cluster. These instructions also apply for deploying to Amazon Elastic Kubernetes Service (EKS) or Google Kubernetes Engine (GKE).

1. **Download Chart**: Download the [latest release](https://executor-helm.acryl.io/index.yaml) of the chart
2. **Unpack the release archive**:
  ```
  tar zxvf v0.0.4.tar.gz --strip-components=2
  ```
3. **Contact Acryl representative**: To provision the required access and infrastructure. The remote executor image is hosted on a private registry. For access within AWS, you will need to provide the IAM principal which will be allowed to pull from the ECR repository. For Google Cloud, you will need to provide the cluster's IAM service account.
4. **Create a secret with DataHub PAT**: This can be generated under **Settings > Access Tokens** on DataHub. You can create the secret object as follows:
  ```
  kubectl create secret generic datahub-access-token-secret --from-literal=datahub-access-token-secret-key=<DATAHUB-ACCESS-TOKEN>
  ```
5. **DataHub Cloud URL**: The URL for your DataHub instance, e.g. `https://<your-company>.acryl.io/gms`. Note that you MUST enter the trailing /gms when configuring the executor.
6. **Acryl Remote Executor Version:** The version of the remote executor to deploy. This is converted into a container image tag.
7. **Optionally** pass source secrets/environment variables as necessary. See [values.yaml](https://github.com/acryldata/datahub-executor-helm/blob/main/charts/datahub-executor-worker/values.yaml) file for all the available configurable options.
8. **Install DataHub Executor chart as follows:**
  ```
    helm install \
    --set global.datahub.executor.worker_id="remote" \
    --set global.datahub.gms.url="https://company.acryl.io/gms" \
    --set image.tag=v0.3.1 \
    acryl datahub-executor-worker
  ```
9. As of DataHub Cloud `v0.3.8.2` It is possible to pass secrets to ingestion recipes using Kubernetes Secret CRDs as shown below. This allows to update secrets at runtime without restarting Remote Executor process.
  ```
    # 1. Create K8s Secret object in remote executor namespace, e.g.
    apiVersion: v1
    kind: Secret
    metadata:
      name: datahub-secret-store
    data:
      REDSHIFT_PASSWORD: cmVkc2hpZnQtc2VjcmV0Cg==
      SNOWFLAKE_PASSWORD: c25vd2ZsYWtlLXNlY3JldAo=
    # 2. Add secret into your Remote Executor deployment:
    extraVolumes:
      - name: datahub-secret-store
        secret:
          secretName: datahub-secret-store
    # 3. Mount it under /mnt/secrets directory
    extraVolumeMounts:
    - mountPath: /mnt/secrets
      name: datahub-secret-store
  ```
You can then reference the mounted secrets directly in the ingestion recipe:
```yaml
source:
    type: redshift
    config:
        host_port: '<redshift host:port>'
        username: connector_test
        table_lineage_mode: mixed
        include_table_lineage: true
        include_tables: true
        include_views: true
        profiling:
            enabled: true
            profile_table_level_only: false
        stateful_ingestion:
            enabled: true
        password: '${REDSHIFT_PASSWORD}'
``` 

By default the executor will look for files mounted in `/mnt/secrets`, this is override-able by setting the env var: 
`DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR` to a different location (default: `/mnt/secrets`)

These files are expected to be under 1MB in data by default. To increase this limit set a higher value using:
`DATAHUB_EXECUTOR_FILE_SECRET_MAXLEN` (default: `1024768`, size in bytes)

## FAQ

### If I need to change (or add) a secret that is stored in AWS Secrets Manager, e.g. for rotation, will the new secret automatically get picked up by Acryl's executor?

Unfortunately, no. Secrets are wired into the executor container at deployment time, via environment variables. Therefore, the ECS Task will need to be restarted (either manually or via a stack parameter update) whenever your secrets change.

### I want to deploy multiple Acryl Executors. Is this currently possible?

Yes. Please contact your Acryl representative for details.

### I've run the CloudFormation Template, how can I tell that the container was successfully deployed?

We recommend verifying in AWS Console by navigating to **ECS > Cluster > Stack Name > Services > Logs.**
When you first deploy the executor, you should a single log line to indicate success:
```
Starting datahub executor worker
```
This indicates that the remote executor has established a successful connection to your DataHub instance and is ready to execute ingestion & monitors.
If you DO NOT see this log line, but instead see something else, please contact your Acryl representative for support.
