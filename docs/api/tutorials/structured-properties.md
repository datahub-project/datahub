import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Structured Properties

## Why Would You Use Structured Properties?

 Structured properties are a structured, named set of properties that can be attached to logical entities like Datasets, DataJobs, etc.
Structured properties have values that are types. Conceptually, they are like “field definitions”.

Learn more about structured properties in the [Structured Properties Feature Guide](../../../docs/features/feature-guides/structured-properties.md).


### Goal Of This Guide

This guide will show you how to execute the following actions with structured properties.
- Create structured properties
- Read structured properties
- Delete structured properties (soft delete / hard delete)
- Add structured properties to a dataset
- Patch structured properties (add / remove / update a single property)

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

Additionally, you need to have the following tools installed according to the method you choose to interact with DataHub:

<Tabs>
<TabItem value="CLI" label="CLI" default>

Install the relevant CLI version. Forms are available as of CLI version `0.13.1`. The corresponding SaaS release version is `v0.2.16.5`
Connect to your instance via [init](https://datahubproject.io/docs/cli/#init):

- Run `datahub init` to update the instance you want to load into.
- Set the server to your sandbox instance, `https://{your-instance-address}/gms`.
- Set the token to your access token.


</TabItem>
<TabItem value="OpenAPI" label="OpenAPI">

Requirements for OpenAPI are:
* curl
* jq

</TabItem>
</Tabs>


## Create Structured Properties

The following code will create a structured property `io.acryl.privacy.retentionTime`. 

<Tabs>
<TabItem value="CLI" label="CLI" default>

Create a yaml file representing the properties you’d like to load. 
For example, below file represents a property `io.acryl.privacy.retentionTime`. You can see the full example [here](https://github.com/datahub-project/datahub/blob/example-yaml-sp/metadata-ingestion/examples/structured_properties/struct_props.yaml).
        
```yaml
- id: io.acryl.privacy.retentionTime
  # - urn: urn:li:structuredProperty:io.acryl.privacy.retentionTime # optional if id is provided
  qualified_name: io.acryl.privacy.retentionTime # required if urn is provided
  type: number
  cardinality: MULTIPLE
  display_name: Retention Time
  entity_types:
    - dataset # or urn:li:entityType:datahub.dataset
    - dataFlow
  description: "Retention Time is used to figure out how long to retain records in a dataset"
  allowed_values:
    - value: 30
      description: 30 days, usually reserved for datasets that are ephemeral and contain pii
    - value: 90
      description: Use this for datasets that drive monthly reporting but contain pii
    - value: 365
      description: Use this for non-sensitive data that can be retained for longer
```

Use the CLI to create your properties:
```commandline
datahub properties upsert -f {properties_yaml}
```

If successful, you should see `Created structured property urn:li:structuredProperty:...`

</TabItem>
<TabItem value="OpenAPI" label="OpenAPI">

```commandline
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "qualifiedName": "io.acryl.privacy.retentionTime",
    "displayName": "io.acryl.privacy.retentionTime",
    "valueType": "urn:li:dataType:datahub.string",
    "allowedValues": [
        {
            "value": {"string": "foo"},
            "description": "test foo value"
        },
        {
            "value": {"string": "bar"},
            "description": "test bar value"
        }
    ],
    "cardinality": "SINGLE",
    "entityTypes": [
        "urn:li:entityType:datahub.dataset"
    ],
    "description": "test description"
}' | jq
```
</TabItem>
</Tabs>

## Read Structured Properties

You can see the properties you created by running the following command:

<Tabs>
<TabItem value="CLI" label="CLI" default>


```commandline
datahub properties get --urn {urn}
```
For Example, you can run `datahub properties get --urn urn:li:structuredProperty:io.acryl.privacy.retentionTime`.
If successful, you should see metadata about your properties returned.

</TabItem>
<TabItem value="OpenAPI" label="OpenAPI">

Example Request:
```
curl -X 'GET' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json' | jq
```

Example Response: 

```commandline
{
  "value": {
    "allowedValues": [
      {
        "value": {
          "string": "foo"
        },
        "description": "test foo value"
      },
      {
        "value": {
          "string": "bar"
        },
        "description": "test bar value"
      }
    ],
    "qualifiedName": "io.acryl.privacy.retentionTime",
    "displayName": "MyProperty01",
    "valueType": "urn:li:dataType:datahub.string",
    "description": "test description",
    "entityTypes": [
      "urn:li:entityType:datahub.dataset"
    ],
    "cardinality": "SINGLE"
  }
}


```

</TabItem>
</Tabs>


## Delete Structured Properties

There are two types of deletion present in DataHub: hard and soft delete. As of the current release only the soft delete is supported for Structured Properties. See the subsections below for more details.

### Soft Delete
A soft deleted Structured Property does not remove any underlying data on the Structured Property entity or the Structured Property's values written to other entities. The soft delete is 100% reversible with zero data loss. When a Structured Property is soft deleted, a few operations are not available.

Structured Property Soft Delete Effects:

- Entities with a soft deleted Structured Property value will not return the soft deleted properties
- Updates to a soft deleted Structured Property's definition are denied
- Adding a soft deleted Structured Property's value to an entity is denied
- Search filters using a soft deleted Structured Property will be denied

### Hard Delete
(TBD)

<Tabs>
<TabItem value="CLI-soft" label="CLI (Soft/Hard)" default>

```commandline
datahub delete --urn {urn}
```

You can hard delete a Structured Property by adding the `--hard` flag to the command. 

```commandline
datahub delete --urn {urn} --hard
```

</TabItem>
<TabItem value="OpenAPI" label="OpenAPI (Soft Delete)">

The following command will soft delete the test property by writing to the status aspect.

```
curl -X 'POST' \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
"removed": true
}' | jq
```

If you want to **remove the soft delete**, you can do so by either hard deleting the status aspect or changing the removed boolean to `false` like below.

```
curl -X 'POST' \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
"removed": false
}' | jq
```

</TabItem>
</Tabs>


## Set Structured Property To a Dataset

This action will set/replace all structured properties on the entity. See PATCH operations to add/remove a single property.

<Tabs>
<TabItem value="CLI" label="CLI" default>

You can set structured properties to a dataset by creating a dataset yaml file with structured properties.
Please refer to the [full example here.](https://github.com/datahub-project/datahub/blob/example-yaml-sp/metadata-ingestion/examples/structured_properties/datasets.yaml)

You can define structured properties at the dataset level in the dataset yaml like below. 

```yaml
- id: user.clicks
  platform: hive
  ...
  properties:
    io.acryl.privacy.retentionTime: 365
  structured_properties: # dataset level structured properties go here
    clusterType: primary
    clusterName: gold
    io.acryl.privacy.retentionTime: 123456
    projectNames:
      - Tracking
      - DataHub
```

Use the CLI to upsert your dataset yaml file:
```commandline
datahub dataset upsert -f {dataset_yaml}
```
If successful, you should see `Created dataset urn:li:dataset:...`

Once your datasets are uploaded, you can view them in the UI and view the properties associated with them under the Properties tab.
Or you can run the following command to view the properties associated with the dataset:

```commandline
datahub dataset get --urn {urn}
```
</TabItem>
<TabItem value="OpenAPI" label="OpenAPI">

```commandline

curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
      "values": [
        {"string": "foo"}
      ]
    }
  ]
}' | jq
```

</TabItem>
</Tabs>

## Patch Structured Property Value

This section will show you how to patch a structured property value - either by removing, adding, or upserting a single property.

### Add Structured Property Value

For this example, we'll extend create a second structured property and apply both properties to the same dataset used previously. 
After this your system should include both io.acryl.privacy.retentionTime and my.test.MyProperty02.

<Tabs>
<TabItem value="OpenAPI" label="OpenAPI">

Let's start by creating the second structured property.

```
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Amy.test.MyProperty02/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "qualifiedName": "my.test.MyProperty02",
    "displayName": "MyProperty02",
    "valueType": "urn:li:dataType:datahub.string",
    "allowedValues": [
        {
            "value": {"string": "foo2"},
            "description": "test foo2 value"
        },
        {
            "value": {"string": "bar2"},
            "description": "test bar2 value"
        }
    ],
    "cardinality": "SINGLE",
    "entityTypes": [
        "urn:li:entityType:datahub.dataset"
    ]
}' | jq

```

This command will attach one of each of the two properties to our test dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`.

```
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
      "values": [
        {"string": "foo"}
      ]
    },
    {
      "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty02",
      "values": [
        {"string": "bar2"}
      ]
    }
  ]
}' | jq
```

</TabItem>
</Tabs>

### Remove Structured Property Value

The expected state of our test dataset include 2 structured properties. 
We'd like to remove the first one and preserve the second property.

<Tabs>
<TabItem value="OpenAPI" label="OpenAPI">

```
curl -X 'PATCH' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json-patch+json' \
  -d '{
        "patch": [
            {
                "op": "remove",
                "path": "/properties/urn:li:structuredProperty:io.acryl.privacy.retentionTime"
            }
        ],
        "arrayPrimaryKeys": {
            "properties": [
                "propertyUrn"
            ]
        }
      }' | jq


The response will show that the expected property has been removed.

{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
  "aspects": {
    "structuredProperties": {
      "value": {
        "properties": [
          {
            "values": [
              {
                "string": "bar2"
              }
            ],
            "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty02"
          }
        ]
      }
    }
  }
}

```
</TabItem>
</Tabs>


### Update Structured Property Value

In this example, we'll add the property back with a different value, preserving the existing property.

<Tabs>
<TabItem value="OpenAPI" label="OpenAPI">

```
curl -X 'PATCH' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json-patch+json' \
  -d '{
        "patch": [
            {
                "op": "add",
                "path": "/properties/urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "value": {
                    "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                    "values": [
                        {
                            "string": "bar"
                        }
                    ]
                }
            }
        ],
        "arrayPrimaryKeys": {
            "properties": [
                "propertyUrn"
            ]
        }
    }' | jq

```

Below is the expected response: 
```
{
    "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
    "aspects": {
        "structuredProperties": {
            "value": {
                "properties": [
                    {
                        "values": [
                            {
                                "string": "bar2"
                            }
                        ],
                        "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty02"
                    },
                    {
                        "values": [
                            {
                                "string": "bar"
                            }
                        ],
                        "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime"
                    }
                ]
            }
        }
    }
}
```

The response shows that the property was re-added with the new value bar instead of the previous value foo.

</TabItem>
</Tabs>

