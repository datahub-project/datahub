---
description: >-
  This guide will walk through the configuration required to start receiving
  DataHub Cloud events via AWS EventBridge.
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';


# Setting up Events API on AWS EventBridge
<FeatureAvailability saasOnly />

## Entity Events API

* See the Entity Events API Docs [here](docs/managed-datahub/datahub-api/entity-events-api.md)

## Event Structure

As are all AWS EventBridge events, the payload itself will be wrapped by a set of standard fields, outlined [here](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events.html). The most notable include

* **source:** A unique identifier for the source of the event. We tend to use \`acryl.events\` by default.
* **account**: The account in which the event originated. This will be the Acryl AWS Account ID provided by your Acryl customer success rep.
* **detail**: The place where the Entity Event payload will appear.

#### Sample Event

```
{
  "version": "0",
  "id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718",
  "detail-type": "entityChangeEvent",
  "source": "acryl.events",
  "account": "111122223333",
  "time": "2017-12-22T18:43:48Z",
  "region": "us-west-1",
  "detail": {
      "entityUrn": "urn:li:dataset:abc",
      "entityType": "dataset",
      "category": "TAG",
        "operation": "ADD",
      "modifier": "urn:li:tag:pii",
      "parameters": {
         "tagUrn": "urn:li:tag:pii"
      }
  }
}
```

#### Sample Pattern

```
{ 
  "source": ["acryl.events"], 
  "detail": {
    "category": ["TAG"],
    "parameters": {
      "tagUrn": ["urn:li:tag:pii"]
    }
  }
}
```

_Sample Event Pattern Filtering any Add Tag Events on a PII Tag_

## Step 1: Create an Event Bus

We recommend creating a dedicated event bus for Acryl. To do so, follow the steps below:

1\. Navigate to the AWS console inside the account where you will deploy Event Bridge.

2\. Search and navigate to the **EventBridge** page.

3\. Navigate to the **Event Buses** tab.

3\. Click **Create Event Bus.**

4\. Give the new bus a name, e.g. **acryl-events.**

5\. Define a **Resource Policy**

When creating your new event bus, you need to create a Policy that allows the Acryl AWS account to publish messages to the bus. This involves granting the **PutEvents** privilege to the Acryl account via an account id.

**Sample Policy**

```
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "allow_account_to_put_events",
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::795586375822:root"
    },
    "Action": "events:PutEvents",
    "Resource": "<event-bus-arn>"
  }]
}
```

Notice that you'll need to populate the following fields on your own

* **event-bus-arn**: This is the AWS ARN of your new event bus.

## Step 2: Create a Routing Rule

Once you've defined an event bus, you need to create a rule for routing incoming events to your destinations, for example an SQS topic, a Lambda function, a Log Group, etc.

To do so, follow the below steps

1\. Navigate to the **Rules** tab.

2\. Click **Create Rule**.

3\. Give the rule a name. This will usually depend on the target where you intend to route requests matching the rule.

4\. In the **Event Bus** field, select the event bus created in **Step 1**.

5\. Select the 'Rule with Event Pattern' option

6\. Click **Next.**

7\. For **Event Source**, choose **Other**

8\. **** Optional: Define a Sample Event. You can use the Sample Event defined in the **Event Structure** section above.

9\. Define a matching Rule. This determines which Acryl events will be routed based on the current rule. You can use the Sample Rule defined in the **Event Structure** section above as a reference.

10\. Define a Target: This defines where the events that match the rule should be routed.



## Step 3: Configure Acryl to Send Events

Once you've completed these steps, communicate the following information to your Acryl Customer Success rep:

* The ARN of the new Event Bus.
* The AWS region in which the Event Bus is located.

This will enable Acryl to begin sending events to your EventBridge bus.



__
