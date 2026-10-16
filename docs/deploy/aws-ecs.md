---
title: "Deploying DataHub on AWS ECS"
---

# Deploying DataHub on AWS ECS

> **Community-supported deployment path.** This guide describes a community-supported deployment of DataHub OSS on Amazon Elastic Container Service (ECS). The recommended production path is Kubernetes with Helm charts — see [Deploying to AWS (EKS)](https://docs.datahub.com/docs/deploy/aws). 

> **Scope.** This guide describes a functional reference deployment. It does not cover full production hardening, observability, high availability, or cost optimization. Treat every configuration value in this guide as a starting point, not a sizing recommendation.

> **Tested version.** This guide was tested with DataHub **v0.14.1**. On DataHub **1.5 and later**, GMS also requires a token service signing key and salt, and Frontend requires `DATAHUB_SECRET` — see [Configure the token service signing key and salt](#configure-the-token-service-signing-key-and-salt) and [Enable metadata service authentication](#enable-metadata-service-authentication).

> DataHub's environment variables and default configuration change between releases. If you are following this guide with a different DataHub version or need information on other configuration not listed here, cross-check the variables below against the current [Deployment Environment Variables](https://docs.datahub.com/docs/deploy/environment-vars) and [Docker Images](https://docs.datahub.com/docs/docker) reference before applying them.

This guide uses:

- Amazon ECS for running DataHub services
- Amazon RDS for MySQL DB
- Amazon Managed Streaming for Apache Kafka (MSK)
- Amazon OpenSearch Service
- AWS Secrets Manager
- Amazon ECR
- AWS Cloud Map
- Terraform for infrastructure provisioning

For environments that should not be publicly accessible, all ECS services can remain private and be accessed through a VPN or another private networking solution.

## Prerequisites

The following tools and resources are required:

- An AWS account with permissions to provision the required resources.
- [Terraform](https://developer.hashicorp.com/terraform/install).
- A VPC with subnets and security groups configured for ECS and the required AWS services.
- Access to the DataHub Docker images.
- Docker, if you need to build custom DataHub images.

## Architecture

This reference deployment runs as **AWS Fargate** ECS tasks with the following long-running services:

- **Frontend** — DataHub web application.
- **GMS** — DataHub Metadata Service. In this configuration, GMS also runs the MAE and MCE consumers.
- **Actions** — DataHub Actions and UI-based ingestion.

MAE and MCE can also be deployed as separate ECS services when they need to be scaled independently — see [Scale the deployment](#scale-the-deployment).

The deployment also uses a one-time ECS task for DataHub System Update. The DataHub Docker images include `acryldata/datahub-upgrade`, which runs System Update and performs SQL and search-index setup when configured to do so.

<p align="center">
  <img width="90%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/aws/aws-ecs-architecture.svg"/>
</p>

*If your environment should not be publicly accessible at all, remove the ALB from the public subnet, and reach Frontend through a VPN or other private connectivity.*

## Provision the infrastructure

The infrastructure can be provisioned using Terraform (see the [template files](https://github.com/acryldata/datahub-terraform-modules/tree/main/datahub-oss-ecs)), running the following commands:

```bash
terraform init
terraform validate
terraform plan -var environment=<environment>
terraform apply -var environment=<environment>
```

Resource names are built from the `environment` variable, not from the Terraform workspace, so the same template can be reused for several environments. Because workspaces are not used to separate environments, keep a separate Terraform state for each one (for example a different backend key or a separate working directory per environment).

Make sure that you are running from the correct Terraform directory, that `environment` is set to the environment you intend to change, and that your Terraform variables and AWS credentials are correctly set.

## Configure AWS Cloud Map

Create a private service-discovery namespace and a Cloud Map service for GMS ([AWS reference](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-service-discovery.html)).

Frontend and Actions can then resolve GMS through the Cloud Map DNS name.

Cloud Map is used instead of an Application Load Balancer in this reference deployment, since a load balancer isn't required for private service-to-service resolution. This can reduce the cost and infrastructure required for a reference deployment.

If your deployment requires load-balancer-based health checks, ingress, or additional traffic management, use an Application Load Balancer instead ([AWS reference](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/create-application-load-balancer.html)).

## Configure Amazon RDS for MySQL DB

Provision an Amazon RDS MySQL database that is accessible from the ECS tasks ([AWS reference](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html)).

> ⚠️ **Example configuration used in this implementation — not a sizing recommendation.**

| Setting | Value |
|---|---|
| Engine | MySQL |
| Storage | 20 GB |
| Instance class | db.t3.small |

Store the database password in AWS Secrets Manager ([AWS reference](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-secrets-manager.html)).

Use a dedicated database user with the permissions required by DataHub. Avoid using the master user for the application.

## Configure Amazon MSK

Provision an Amazon MSK cluster that is accessible from the ECS tasks ([AWS reference](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html)).

> ⚠️ **Example configuration used in this implementation — not a sizing recommendation.**

| Setting | Value |
|---|---|
| Number of nodes | 3 |
| Instance type | kafka.t3.small |
| EBS storage | 50 GB |
| Encryption | TLS |

Configure DataHub with the MSK broker endpoints.

When multiple brokers are available, include all required broker endpoints in `KAFKA_BOOTSTRAP_SERVER`. For example:

```
b-1.cluster-name.xxxxx.c2.kafka.us-east-2.amazonaws.com:9094,
b-2.cluster-name.xxxxx.c2.kafka.us-east-2.amazonaws.com:9094,
b-3.cluster-name.xxxxx.c2.kafka.us-east-2.amazonaws.com:9094
```

This deployment uses port 9094 for TLS connections. The Actions container must have access to the CA certificate required to establish the TLS connection to MSK.

Because the MSK cluster uses TLS, configure the Kafka security protocol per service:

- **GMS, MAE/MCE (Spring Boot–based services):** set `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SSL`.
- **Actions:** Actions is Python-based and does not read the `SPRING_KAFKA_PROPERTIES_*` variables. Configure `security.protocol` directly under `consumer_config` in the Actions YAML configuration (see [Customize the Actions image](#customize-the-actions-image)), optionally sourced from a `KAFKA_PROPERTIES_SECURITY_PROTOCOL` environment variable.

> **Note:** DataHub's Kafka-related environment variables differ between Spring Boot–based components (GMS, MAE/MCE) and Actions, and have changed across releases. Confirm the variable names that apply to each service against the [Kafka configuration reference](https://docs.datahub.com/docs/how/kafka-config) for the DataHub version you are deploying before copying these values as-is.

## Configure Amazon OpenSearch Service

Provision an Amazon OpenSearch Service domain that is accessible from the ECS tasks ([AWS reference](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/createupdatedomains.html)).

> ⚠️ **Example configuration used in this implementation — not a sizing recommendation.**

| Setting | Value |
|---|---|
| Number of instances | 3 |
| Instance type | t3.medium.search |
| EBS storage | 50 GB gp3 |
| Encryption | Enabled |
| HTTPS | Enforced |

When IAM authentication is enabled for OpenSearch, set the following variables in the GMS service:

```
OPENSEARCH_USE_AWS_IAM_AUTH=true
AWS_REGION=<AWS region>
```

## Configure Amazon ECR

Create an ECR repository for each DataHub service or custom one-time task image that you deploy ([AWS reference](https://docs.aws.amazon.com/AmazonECR/latest/userguide/repository-create.html)).

## Configure AWS Secrets Manager

Store DataHub application and database secrets in AWS Secrets Manager ([AWS reference](https://docs.aws.amazon.com/secretsmanager/latest/userguide/create_secret.html)).

The ECS execution roles require permission to retrieve the secrets used by each service:

```
secretsmanager:GetSecretValue
```

Grant only the secrets required by the corresponding task.

## Deploy the Frontend service

Create an ECS service for the DataHub Frontend ([AWS reference](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/getting-started.html)).

> ⚠️ **Example configuration used in this implementation — not a sizing recommendation.**

| Setting | Value |
|---|---|
| Port | 9002 |
| CPU | 1024 |
| Memory | 2048 MiB |

### Expose Frontend to users

Unlike GMS, the Frontend service is the component users interact with directly, so it needs a public (or VPN-reachable) entry point.

This reference deployment exposes Frontend through:

- An **Application Load Balancer (ALB)** in a public subnet, with an HTTPS listener terminating TLS using a certificate from AWS Certificate Manager ([AWS reference](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/create-application-load-balancer.html)).
- An **Amazon Route 53** record pointing your DataHub hostname at the ALB ([AWS reference](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-to-elb-load-balancer.html)).

If your environment should not be publicly accessible, create the ALB in a private subnet, and reach Frontend through a VPN or other private connectivity into the VPC instead.

### Configure GMS connectivity

Set:

```
DATAHUB_GMS_HOST=<GMS Cloud Map DNS name or ALB endpoint>
DATAHUB_GMS_PORT=8080
```

`DATAHUB_GMS_HOST` must point to the GMS endpoint reachable from the ECS task. Depending on your networking configuration, this can be the Cloud Map DNS name or an Application Load Balancer endpoint.

### Enable metadata service authentication

Enable authentication between DataHub services:

```
METADATA_SERVICE_AUTH_ENABLED=true
```

Provide the following values to the service through ECS secrets:

```
DATAHUB_SYSTEM_CLIENT_SECRET
DATAHUB_SYSTEM_CLIENT_ID
```

Frontend also needs the following secret:

```
DATAHUB_SECRET
```

For the complete description of these variables, see [Deployment Environment Variables](https://docs.datahub.com/docs/deploy/environment-vars).

## Deploy the GMS service

Create an ECS service for GMS ([AWS reference](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/getting-started.html)).

> ⚠️ **Example configuration used in this implementation — not a sizing recommendation.**

| Setting | Value |
|---|---|
| Port | 8080 |
| CPU | 1024 |
| Memory | 2048 MiB |

Configure GMS to connect to RDS MySQL DB, MSK, and OpenSearch.

Configure the GMS ECS service to register with the service registry ([AWS reference](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-service-discovery.html)).

### Schema registry

This deployment uses DataHub's internal schema registry.

Set:

```
SCHEMA_REGISTRY_TYPE=INTERNAL
KAFKA_SCHEMAREGISTRY_URL=http://localhost:8080/schema-registry/api/
```

If you use a separate schema registry, configure the corresponding values instead.

### MAE and MCE consumers

In this reference deployment, MAE and MCE run within GMS:

```
MAE_CONSUMER_ENABLED=true
MCE_CONSUMER_ENABLED=true
```

If you deploy the consumers as separate ECS services, set both variables to `false` in GMS and configure the corresponding consumer services separately.

### Metadata change topics

If MAE or MCE logs contain an error similar to:

```
UNKNOWN_TOPIC_OR_PARTITION
```

check the configured metadata-change topic names.

The following variables can be used when the default topic names do not match the topics in your Kafka cluster:

```
METADATA_CHANGE_PROPOSAL_TOPIC_NAME
FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME
METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME
METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME
```

If the consumers start successfully without overriding these variables, keep the DataHub defaults.

### Enable the search cache

To enable the GMS search cache:

```
SEARCH_SERVICE_ENABLE_CACHE=true
```

This can improve repeated search performance.

### Configure the token service signing key and salt

Set the following variables in GMS:

```
DATAHUB_TOKEN_SERVICE_SIGNING_KEY
DATAHUB_TOKEN_SERVICE_SALT
```

These are **required on DataHub 1.5 and later**.  Treat both values as secrets. Store them in AWS Secrets Manager and provide them to the GMS task through ECS secrets, as with the other credentials in this guide.

## Deploy the Actions service

Create an ECS service for DataHub Actions ([AWS reference](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/getting-started.html)).

> ⚠️ **Example configuration used in this implementation — not a sizing recommendation.**

| Setting | Value |
|---|---|
| CPU | 1024 |
| Memory | 2048 MiB |

### Configure Kafka

If a TLS connection is configured, **the corresponding CA certificate must be available inside the Actions container.**

## Build custom DataHub images

The default DataHub images can be used when their configuration is sufficient for your deployment. If additional configuration is required, build custom images using the official DataHub images as the base.

Pin the base image to a DataHub release tag or an immutable commit tag. Avoid `latest` or `debug` tags for any image used in this deployment.

### Customize the Actions image

This reference deployment requires custom configuration for the Actions service.

1. Remove the default configuration that is not required by your deployment in the custom Dockerfile:
    ```
    RUN rm -f /etc/datahub/actions/system/conf/*
    ```
2. Update `executor.yaml`.
3. Update any other yaml files for Actions configuration for your setup ([Datahub Github](https://github.com/datahub-project/datahub/tree/master/docker/datahub-actions/config))
4. Copy all files to:
   ```
   /etc/datahub/actions/system/conf/
   ```
5. Add the certificate for MSK to:
   ```
   /etc/datahub/kafka/certs/
   ```

All configuration files for Actions service use the certificate to establish the Kafka TLS connection:

```yaml
consumer_config:
  security.protocol: SSL
  ssl.ca.location: /etc/datahub/kafka/certs/<certificate_name>.pem
```

Configure the GMS endpoint and authentication headers:

```yaml
datahub:
  server: "${DATAHUB_GMS_PROTOCOL:-http}://${DATAHUB_GMS_HOST}:${DATAHUB_GMS_PORT}"
  extra_headers:
    Authorization: "Basic ${DATAHUB_SYSTEM_CLIENT_ID}:${DATAHUB_SYSTEM_CLIENT_SECRET}"
```

Apply the same GMS and Kafka configuration changes to `executor.yaml`, excluding the action-specific configuration.

### Customize the Frontend image

This reference deployment customizes the default Frontend user password using `user.props`.

Copy the customized file into the image at:

```
/datahub-frontend/conf
```

If you do not need to customize the default user configuration, use the standard DataHub Frontend image.

## Run System Update

DataHub uses the `datahub-upgrade` image to run System Update. This image performs SQL and search-index setup when `DATAHUB_SQL_SETUP_ENABLED=true` — confirm this variable name and behavior against the [Docker Images](https://docs.datahub.com/docs/docker) reference for your DataHub version.

Run System Update as a one-time ECS task rather than as a long-running ECS service.

Configure the task with the same database, Kafka, and OpenSearch connectivity required by GMS.

### Run the System Update task

Start the task with `aws ecs run-task`, using the System Update task definition. Because it is a one-time task and not part of an ECS service, you must pass the Fargate launch type and network configuration explicitly:

```bash
aws ecs run-task \
  --cluster <cluster-name> \
  --launch-type FARGATE \
  --task-definition <system-update-task-definition>[:<revision>] \
  --network-configuration "awsvpcConfiguration={subnets=[<subnet-id>],securityGroups=[<security-group-id>],assignPublicIp=DISABLED}" \
  --count 1 \
  --region <aws-region>
```

Use the same private subnets and security groups as GMS so the task can reach RDS, MSK, and OpenSearch. Omit the revision to use the latest ACTIVE revision of the task definition.

Run System Update again whenever you upgrade the DataHub version.

### OpenSearch

GMS and System Update require the OpenSearch permissions necessary to manage DataHub indexes:

```
es:ESHttpGet
es:ESHttpPost
es:ESHttpPut
es:ESHttpDelete
es:ESHttpHead
```

Scope these permissions to the OpenSearch domain.

## Update and restart services

Frontend, GMS, Actions, and any separately deployed MAE or MCE consumers run as long-running ECS services. To roll out changes to one of them, use `aws ecs update-service`.

Force a new deployment with the service's current task definition:

```bash
aws ecs update-service \
  --cluster <cluster-name> \
  --service <service-name> \
  --force-new-deployment \
  --region <aws-region>
```

Use `--force-new-deployment` when the task definition itself has not changed but the tasks need to restart, for example:

- You rotated a value in AWS Secrets Manager. ECS reads secrets only when a task starts, so running tasks keep the old value until they are replaced.
- You need to restart tasks after changing networking or other infrastructure the tasks depend on.

If you changed the task definition (new image tag, environment variables, CPU or memory), register the new revision and point the service at it. This starts a deployment on its own, so `--force-new-deployment` is not needed:

```bash
aws ecs update-service \
  --cluster <cluster-name> \
  --service <service-name> \
  --task-definition <task-definition-family>:<new-revision> \
  --region <aws-region>
```

## Verify the deployment

After provisioning the infrastructure:

1. Run the System Update task and verify that it completes successfully. This task should run before starting the services.
2. Verify that Frontend, GMS, and Actions are running and healthy.
3. Verify that GMS can be resolved through Cloud Map or the configured load balancer.
4. Verify that GMS can connect to RDS.
5. Verify that GMS, Actions, and the System Update task can connect to MSK.
6. Verify that GMS can authenticate to OpenSearch using the ECS execution role.
7. Verify that the ECS tasks can retrieve the required secrets from Secrets Manager.
8. Verify that Frontend can communicate with GMS.
9. Verify that Actions can authenticate with GMS and consume Kafka messages.

## Scale the deployment

The resource allocations in this guide are example values used for this reference deployment — they are not DataHub sizing requirements.

Increase ECS CPU and memory according to your ingestion volume, search workload, number of Actions, and Kafka traffic.

For larger deployments, MAE and MCE can be separated from GMS and deployed as independent ECS services. Set:

```
MAE_CONSUMER_ENABLED=false
MCE_CONSUMER_ENABLED=false
```

in GMS, and configure the dedicated consumer services separately.

Similarly, an Application Load Balancer can be introduced when your deployment requires additional ingress, health checks, or traffic-management capabilities beyond what Cloud Map provides.

## Troubleshooting

### GMS cannot connect to OpenSearch

Verify:

- OpenSearch endpoint
- Port 443
- HTTPS configuration
- `OPENSEARCH_USE_AWS_IAM_AUTH`
- `AWS_REGION`
- ECS execution role permissions
- VPC and security-group connectivity

### Actions cannot connect to GMS

Verify:

- `DATAHUB_GMS_HOST`
- `DATAHUB_GMS_PORT`
- Cloud Map or ALB connectivity
- `DATAHUB_SYSTEM_CLIENT_ID`
- `DATAHUB_SYSTEM_CLIENT_SECRET`
- GMS metadata-service authentication configuration

If authentication is enabled, verify that the custom Actions configuration passes the system credentials to GMS.

### Actions cannot connect to MSK

Verify:

- MSK broker endpoints
- TLS port 9094
- Kafka security protocol (see [Configure Amazon MSK](#configure-amazon-msk))
- CA certificate
- Security-group rules
- ECS task networking

### ECS tasks cannot retrieve secrets

Verify that the ECS execution role has:

```
secretsmanager:GetSecretValue
```

and that the role can access the specific secret ARN referenced by the task.

## Related documentation

- [Deploying to AWS (EKS)](https://docs.datahub.com/docs/deploy/aws)
- [Docker Images](https://docs.datahub.com/docs/docker)
- [Deployment Environment Variables](https://docs.datahub.com/docs/deploy/environment-vars)
- [DataHub Actions](https://docs.datahub.com/docs/actions)
- [Kafka Configuration](https://docs.datahub.com/docs/how/kafka-config)