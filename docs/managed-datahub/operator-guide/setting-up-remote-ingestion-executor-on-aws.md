---
description: >-
  This page describes the steps required to configure a remote ingestion
  executor, which allows you to ingest metadata from private metadata sources
  via the DataHub UI.
---
<FeatureAvailability saasOnly />

# Setting up Remote Ingestion Executor on AWS

## Overview

UI-based Metadata Ingestion reduces the overhead associated with operating DataHub. It allows you to create, schedule, and run batch metadata ingestion on demand in just a few clicks, without requiring custom orchestration. Behind the scenes, a simple ingestion "executor" abstraction makes this possible.&#x20;

Acryl DataHub comes packaged with an Acryl-managed ingestion executor, which is hosted inside of Acryl's environment on your behalf. However, there are certain scenarios in which an Acryl-hosted executor is not sufficient to cover all of an organization's ingestion sources.&#x20;

For example, if an ingestion source is not publicly accessible via the internet, e.g. hosted privately within a specific AWS account, then the Acryl executor will be unable to extract metadata from it.

![Option 1: Acryl-hosted ingestion runner](../imgs/saas/image-(12).png)

To accommodate these cases, Acryl supports configuring a remote ingestion executor which can be deployed inside of your AWS account. This setup allows you to continue leveraging the Acryl DataHub console to create, schedule, and run metadata ingestion, all while retaining network isolation.&#x20;

![Option 2: Customer-hosted ingestion runner](../imgs/saas/image-(6).png)

## Setting up a Remote Ingestion Executor

1. **Provide AWS Account Id**: Provide Acryl Team with the id of the AWS in which the remote executor will be hosted. This will be used to grant access to private Acryl containers and create a unique SQS queue which your remote agent will subscribe to. The account id can be provided to your Acryl representative via Email or [One Time Secret](https://onetimesecret.com/). \

2.  **Provision an Acryl Executor** (ECS)**:** Acryl team will provide a [Cloudformation Template](https://github.com/acryldata/datahub-cloudformation/blob/master/Ingestion/templates/python.ecs.template.yaml) that you can run to provision an ECS cluster with a single remote ingestion task. It will also provision an AWS role for the task which grants the permissions necessary to read and delete from the private SQS queue created for you. At minimum, the template requires the following parameters:

    1. **Deployment Location:** The AWS VPC + subnet in which the Acryl Executor task is to be provisioned.&#x20;
    2. **SQS Queue ARN**: Reference to your private SQS command queue. (Provided by Acryl)&#x20;
    3. **DataHub Access Token**: A valid DataHub access token.&#x20;
    4. **Acryl DataHub URL**: The URL for your DataHub instance, e.g. your-company.acryl.io.


3.  **Test the Executor:** To test your remote executor:

    1. Create a new Ingestion Source by clicking '**Create new Source**' the '**Ingestion**' tab of the DataHub console. Configure your Ingestion Recipe as though you were running it from inside of your environment.&#x20;
    2. In the 'Finish Up' step, click '**Advanced'**.&#x20;
    3. Update the '**Executor Id**' form field to be  '**remote**'. This indicates that you'd like to use the remote executor.&#x20;
    4. Click '**Done**'.&#x20;

    Now, simple click '**Execute**' to test out the remote executor. If your remote executor is configured properly, you should promptly see the ingestion task state change to 'Running'.&#x20;

![](../imgs/saas/Screen-Shot-2022-03-07-at-10.23.31-AM.png)

## &#x20;
