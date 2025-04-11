import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Structured Properties

## Why Would You Use Structured Properties?

 Structured properties are a structured, named set of properties that can be attached to logical entities like Datasets, DataJobs, etc.
Structured properties have values that are typed and support constraints.

Learn more about structured properties in the [Structured Properties Feature Guide](../../../docs/features/feature-guides/properties/overview.md).


### Goal Of This Guide

This guide will show you how to execute the following actions with structured properties.
- Create structured properties
- List structured properties
- Read structured properties
- Delete structured properties
- Add structured properties to a dataset
- Patch structured properties (add / remove / update a single property)
- Update structured property with breaking schema changes
- Search & aggregations using structured properties

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

Additionally, you need to have the following tools installed according to the method you choose to interact with DataHub:

<Tabs>
<TabItem value="CLI" label="CLI" default>

Install the relevant CLI version. 
Structured Properties were introduced in version `0.13.1`, but we continuously improve and add new functionality, so you should always [upgrade](https://datahubproject.io/docs/cli/#installation) to the latest cli for best results.
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
```shell
datahub properties upsert -f {properties_yaml}
```

If successful, you should see `Created structured property urn:li:structuredProperty:...`

</TabItem>

<TabItem value="Graphql" label="GraphQL" default>

```graphql
mutation createStructuredProperty {
  createStructuredProperty(
    input: {
      id: "retentionTime",
      qualifiedName:"retentionTime",
      displayName: "Retention Time",
      description: "Retention Time is used to figure out how long to retain records in a dataset",
      valueType: "urn:li:dataType:datahub.number",
      allowedValues: [
        {numberValue: 30, description: "30 days, usually reserved for datasets that are ephemeral and contain pii"},
        {numberValue: 90, description:"description: Use this for datasets that drive monthly reporting but contain pii"},
        {numberValue: 365, description:"Use this for non-sensitive data that can be retained for longer"}
      ],
      cardinality: SINGLE,
      entityTypes: ["urn:li:entityType:datahub.dataset", "urn:li:entityType:datahub.dataFlow"],
    }
  ) {
    urn
  }
}
```

</TabItem>

<TabItem value="OpenAPI v2" label="OpenAPI v2">

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
		"qualifiedName": "io.acryl.privacy.retentionTime",
	  "valueType": "urn:li:dataType:datahub.number",
	  "description": "Retention Time is used to figure out how long to retain records in a dataset",
	  "displayName": "Retention Time",
	  "cardinality": "MULTIPLE",
	  "entityTypes": [
        "urn:li:entityType:datahub.dataset",
        "urn:li:entityType:datahub.dataFlow"
		  ],
	  "allowedValues": [
	    {
	      "value": {"double": 30},
	      "description": "30 days, usually reserved for datasets that are ephemeral and contain pii"
	    },
	    {
	      "value": {"double": 60},
	      "description": "Use this for datasets that drive monthly reporting but contain pii"
	    },
	    {
	      "value": {"double": 365},
	      "description": "Use this for non-sensitive data that can be retained for longer"
	    }
	  ]
}' | jq
```
</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"qualifiedName": "io.acryl.privacy.retentionTime",
		"valueType": "urn:li:dataType:datahub.number",
		"description": "Retention Time is used to figure out how long to retain records in a dataset",
		"displayName": "Retention Time",
		"cardinality": "MULTIPLE",
		"entityTypes": [
			"urn:li:entityType:datahub.dataset",
			"urn:li:entityType:datahub.dataFlow"
		],
		"allowedValues": [
			{
				"value": {
					"double": 30
				},
				"description": "30 days, usually reserved for datasets that are ephemeral and contain pii"
			},
			{
				"value": {
					"double": 60
				},
				"description": "Use this for datasets that drive monthly reporting but contain pii"
			},
			{
				"value": {
					"double": 365
				},
				"description": "Use this for non-sensitive data that can be retained for longer"
			}
		]
	}
}' | jq
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "propertyDefinition": {
    "value": {
      "allowedValues": [
        {
          "description": "30 days, usually reserved for datasets that are ephemeral and contain pii",
          "value": {
            "double": 30
          }
        },
        {
          "description": "Use this for datasets that drive monthly reporting but contain pii",
          "value": {
            "double": 60
          }
        },
        {
          "description": "Use this for non-sensitive data that can be retained for longer",
          "value": {
            "double": 365
          }
        }
      ],
      "displayName": "Retention Time",
      "qualifiedName": "io.acryl.privacy.retentionTime",
      "valueType": "urn:li:dataType:datahub.number",
      "description": "Retention Time is used to figure out how long to retain records in a dataset",
      "entityTypes": [
        "urn:li:entityType:datahub.dataset",
        "urn:li:entityType:datahub.dataFlow"
      ],
      "cardinality": "MULTIPLE"
    }
  }
}
```

</TabItem>
</Tabs>

## List Structured Properties

You can list all structured properties in your DataHub instance using the following methods:

<Tabs>
<TabItem value="CLI" label="CLI" default>

```shell
datahub properties list
```

This will show all properties with their full details. 

Example Response:
```json
{
  "urn": "urn:li:structuredProperty:clusterName",
  "qualified_name": "clusterName",
  "type": "urn:li:dataType:datahub.string",
  "description": "Test Cluster Name Property",
  "display_name": "Cluster's name",
  "entity_types": [
    "urn:li:entityType:datahub.dataset"
  ],
  "cardinality": "SINGLE"
}
{
  "urn": "urn:li:structuredProperty:projectNames",
  "qualified_name": "projectNames",
  "type": "urn:li:dataType:datahub.string",
  "description": "Test property for project name",
  "display_name": "Project Name",
  "entity_types": [
    "urn:li:entityType:datahub.dataset",
    "urn:li:entityType:datahub.dataFlow"
  ],
  "cardinality": "MULTIPLE",
  "allowed_values": [
    {
      "value": "Tracking",
      "description": "test value 1 for project"
    },
    {
      "value": "DataHub",
      "description": "test value 2 for project"
    }
  ]
}
```


If you only want to see the URNs, you can use:

```shell
datahub properties list --no-details
```

Example Response:
```
[2025-01-08 22:23:00,625] INFO     {datahub.cli.specific.structuredproperties_cli:134} - Listing structured property urns only, use --details for more information
urn:li:structuredProperty:clusterName
urn:li:structuredProperty:clusterType
urn:li:structuredProperty:io.acryl.dataManagement.deprecationDate
urn:li:structuredProperty:projectNames
```

To download all the structured property definitions into a single file that you can use with the `upsert` command as described in the [create section](#create-structured-properties), you can run the list command with the `--to-file` option.

```shell
datahub properties list --to-file structured_properties.yaml
```

Example Response:
```yaml
  - urn: urn:li:structuredProperty:clusterName
    qualified_name: clusterName
    type: urn:li:dataType:datahub.string
    description: Test Cluster Name Property
    display_name: Cluster's name
    entity_types:
      - urn:li:entityType:datahub.dataset
    cardinality: SINGLE
  - urn: urn:li:structuredProperty:clusterType
    qualified_name: clusterType
    type: urn:li:dataType:datahub.string
    description: Test Cluster Type Property
    display_name: Cluster's type
    entity_types:
      - urn:li:entityType:datahub.dataset
    cardinality: SINGLE
  - urn: urn:li:structuredProperty:io.acryl.dataManagement.deprecationDate
    qualified_name: io.acryl.dataManagement.deprecationDate
    type: urn:li:dataType:datahub.date
    display_name: Deprecation Date
    entity_types:
      - urn:li:entityType:datahub.dataset
      - urn:li:entityType:datahub.dataFlow
      - urn:li:entityType:datahub.dataJob
      - urn:li:entityType:datahub.schemaField
    cardinality: SINGLE
  - urn: urn:li:structuredProperty:io.acryl.privacy.enumProperty5712
    qualified_name: io.acryl.privacy.enumProperty5712
    type: urn:li:dataType:datahub.string
    description: The retention policy for the dataset
    entity_types:
      - urn:li:entityType:datahub.dataset
    cardinality: MULTIPLE
    allowed_values:
      - value: foo
      - value: bar
... etc.
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

Example Request:
```bash
curl -X 'GET' \
  'http://localhost:9002/openapi/v3/entity/structuredproperty?systemMetadata=false&includeSoftDelete=false&skipCache=false&aspects=structuredPropertySettings&aspects=propertyDefinition&aspects=institutionalMemory&aspects=structuredPropertyKey&aspects=status&count=10&sortCriteria=urn&sortOrder=ASCENDING&query=*' \
  -H 'accept: application/json'
```

Example Response:
```json
{
  "scrollId": "...",
  "entities": [
    {
      "urn": "urn:li:structuredProperty:clusterName",
      "propertyDefinition": {
        "value": {
          "immutable": false,
          "qualifiedName": "clusterName",
          "displayName": "Cluster's name",
          "valueType": "urn:li:dataType:datahub.string",
          "description": "Test Cluster Name Property",
          "entityTypes": [
            "urn:li:entityType:datahub.dataset"
          ],
          "cardinality": "SINGLE"
        }
      },
      "structuredPropertyKey": {
        "value": {
          "id": "clusterName"
        }
      }
    }
  ]
}
```

Key Query Parameters:
- `count`: Number of results to return per page (default: 10)
- `sortCriteria`: Field to sort by (default: urn)
- `sortOrder`: Sort order (ASCENDING or DESCENDING)
- `query`: Search query to filter properties (* for all)

</TabItem>
</Tabs>

The list endpoint returns all structured properties in your DataHub instance. Each property includes:
- URN: Unique identifier for the property
- Qualified Name: The property's qualified name
- Type: The data type of the property (string, number, date, etc.)
- Description: A description of the property's purpose
- Display Name: Human-readable name for the property
- Entity Types: The types of entities this property can be applied to
- Cardinality: Whether the property accepts single (SINGLE) or multiple (MULTIPLE) values
- Allowed Values: If specified, the list of allowed values for this property

## Read a single Structured Property

You can read an individual property you created by running the following command:

<Tabs>
<TabItem value="CLI" label="CLI" default>


```commandline
datahub properties get --urn {urn}
```
For example, you can run `datahub properties get --urn urn:li:structuredProperty:io.acryl.privacy.retentionTime`.
If successful, you should see metadata about your properties returned.

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "qualified_name": "io.acryl.privacy.retentionTime",
  "type": "urn:li:dataType:datahub.number",
  "description": "Retention Time is used to figure out how long to retain records in a dataset",
  "display_name": "Retention Time",
  "entity_types": [
    "urn:li:entityType:datahub.dataset",
    "urn:li:entityType:datahub.dataFlow"
  ],
  "cardinality": "MULTIPLE",
  "allowed_values": [
    {
      "value": "30",
      "description": "30 days, usually reserved for datasets that are ephemeral and contain pii"
    },
    {
      "value": "90",
      "description": "Use this for datasets that drive monthly reporting but contain pii"
    },
    {
      "value": "365",
      "description": "Use this for non-sensitive data that can be retained for longer"
    }
  ]
}
```

</TabItem>
<TabItem value="GraphQL" label="GraphQL">

Example Request:
```graphql
query {
  structuredProperty(urn: "urn:li:structuredProperty:projectNames") {
    urn
    type
    definition {
      qualifiedName
      displayName
      description
      cardinality
      allowedValues {
        value {
          ... on StringValue {
            stringValue
          }
          ... on NumberValue {
            numberValue
          }
        }
        description
      }
      entityTypes {
        urn
        info {
          type
          qualifiedName
        }
      }
    }
  }
}
```

Example Response:
```json
{
  "data": {
    "structuredProperty": {
      "urn": "urn:li:structuredProperty:projectNames",
      "type": "STRUCTURED_PROPERTY",
      "definition": {
        "qualifiedName": "projectNames",
        "displayName": "Project Name",
        "description": "Test property for project name",
        "cardinality": "MULTIPLE",
        "allowedValues": [
          {
            "value": {
              "stringValue": "Tracking"
            },
            "description": "test value 1 for project"
          },
          {
            "value": {
              "stringValue": "DataHub"
            },
            "description": "test value 2 for project"
          }
        ],
        "entityTypes": [
          {
            "urn": "urn:li:entityType:datahub.dataset",
            "info": {
              "type": "DATASET",
              "qualifiedName": "datahub.dataset"
            }
          },
          {
            "urn": "urn:li:entityType:datahub.dataFlow",
            "info": {
              "type": "DATA_FLOW",
              "qualifiedName": "datahub.dataFlow"
            }
          }
        ]
      }
    }
  },
  "extensions": {}
}
```
</TabItem>

<TabItem value="OpenAPI v2" label="OpenAPI v2">

Example Request:
```
curl -X 'GET' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json' | jq
```

Example Response: 

```json
{
  "value": {
    "allowedValues": [
      {
        "value": {
          "double": 30.0
        },
        "description": "30 days, usually reserved for datasets that are ephemeral and contain pii"
      },
      {
        "value": {
          "double": 60.0
        },
        "description": "Use this for datasets that drive monthly reporting but contain pii"
      },
      {
        "value": {
          "double": 365.0
        },
        "description": "Use this for non-sensitive data that can be retained for longer"
      }
    ],
    "qualifiedName": "io.acryl.privacy.retentionTime",
    "displayName": "Retention Time",
    "valueType": "urn:li:dataType:datahub.number",
    "description": "Retention Time is used to figure out how long to retain records in a dataset",
    "entityTypes": [
      "urn:li:entityType:datahub.dataset",
      "urn:li:entityType:datahub.dataFlow"
    ],
    "cardinality": "MULTIPLE"
  }
}
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

Example Request:
```
curl -X 'GET' -v \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition' \
  -H 'accept: application/json' | jq
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "propertyDefinition": {
    "value": {
      "allowedValues": [
        {
          "description": "30 days, usually reserved for datasets that are ephemeral and contain pii",
          "value": {
            "double": 30
          }
        },
        {
          "description": "Use this for datasets that drive monthly reporting but contain pii",
          "value": {
            "double": 60
          }
        },
        {
          "description": "Use this for non-sensitive data that can be retained for longer",
          "value": {
            "double": 365
          }
        }
      ],
      "displayName": "Retention Time",
      "qualifiedName": "io.acryl.privacy.retentionTime",
      "valueType": "urn:li:dataType:datahub.number",
      "description": "Retention Time is used to figure out how long to retain records in a dataset",
      "entityTypes": [
        "urn:li:entityType:datahub.dataset",
        "urn:li:entityType:datahub.dataFlow"
      ],
      "cardinality": "MULTIPLE"
    }
  }
}
```

</TabItem>
</Tabs>


## Set Structured Property To a Dataset

This action will set/replace all structured properties on the entity. See PATCH operations to add/remove a single property.

<Tabs>
<TabItem value="GraphQL" label="GraphQL" default>

```graphql
mutation upsertStructuredProperties {
  upsertStructuredProperties(
    input: {
      assetUrn: "urn:li:mydataset1",
      structuredPropertyInputParams: [
        {
          structuredPropertyUrn: "urn:li:structuredProperty:mystructuredproperty",
          values: [
            {
              stringValue: "123"
            }
          ]
        }
      ]
    }
  ) {
    properties {
      structuredProperty {
        urn
      }
    }
  }
}

```

</TabItem>
<TabItem value="CLI" label="CLI">

You can set structured properties to a dataset by creating a dataset yaml file with structured properties. For example, below is a dataset yaml file with structured properties in both the field and dataset level. 

Please refer to the [full example here.](https://github.com/datahub-project/datahub/blob/example-yaml-sp/metadata-ingestion/examples/structured_properties/datasets.yaml)

```yaml
- id: user_clicks_snowflake
  platform: snowflake
  schema:
    fields:
      - id: user_id
        structured_properties:
          io.acryl.dataManagement.deprecationDate: "2023-01-01"
  structured_properties:
    io.acryl.dataManagement.replicationSLA: 90
```

Use the CLI to upsert your dataset yaml file:
```commandline
datahub dataset upsert -f {dataset_yaml}
```
If successful, you should see `Update succeeded for urn:li:dataset:...`



</TabItem>

<TabItem value="OpenAPI v2" label="OpenAPI v2">

Following command will set structured properties `retentionTime` as `60.0` to a dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`.
Please note that the structured property and the dataset must exist before executing this command. (You can create sample datasets using the `datahub docker ingest-sample-data`)

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
      "values": [
        {"double": 60.0}
      ]
    }
  ]
}' | jq
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

Following command will set structured properties `retentionTime` as `60.0` to a dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`.
Please note that the structured property and the dataset must exist before executing this command. (You can create sample datasets using the `datahub docker ingest-sample-data`)

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"properties": [
			{
			  "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
			  "values": [
				{"double": 60.0}
			  ]
			}
		  ]
	}
}' | jq
```
Example Response:

```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
  "structuredProperties": {
    "value": {
      "properties": [
        {
          "values": [
            {
              "double": 60
            }
          ],
          "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime"
        }
      ]
    }
  }
}
```
</TabItem>
</Tabs>

#### Expected Outcomes

Once your datasets are uploaded, you can view them in the UI and view the properties associated with them under the Properties tab.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/sp-set.png"/>
</p>

Or you can run the following command to view the properties associated with the dataset:

```commandline
datahub dataset get --urn {urn}
```

## Read Structured Properties From a Dataset

For reading all structured properties from a dataset:

<Tabs>
<TabItem value="Graphql" label="GraphQL" default>

```graphql
query getDataset {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.ecommerce.customer,PROD)") {
    structuredProperties {
      properties {
        structuredProperty {
          urn
          type
          definition {
            displayName
            description
            allowedValues {
              description
            }
          }
        }
        values {
          ... on StringValue {
            stringValue
          }
          ... on NumberValue {
            numberValue
          }
        }
        valueEntities {
          urn
          type
        }
      }
    }
  }
}
```

</TabItem>  
</Tabs>

## Remove Structured Properties From a Dataset

For removing a structured property or list of structured properties from a dataset:

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```graphql
mutation removeStructuredProperties {
  removeStructuredProperties(
    input: {
      assetUrn: "urn:li:mydataset1",
      structuredPropertyUrns: ["urn:li:structuredProperty:mystructuredproperty"]
    }
  ) {
    properties {
			structuredProperty {urn}
		}
  }
}
```

</TabItem>  
</Tabs>

## Patch Structured Property Value

This section will show you how to patch a structured property value - either by removing, adding, or upserting a single property.

### Add Structured Property Value

For this example, we'll extend create a second structured property and apply both properties to the same dataset used previously. 
After this your system should include both `io.acryl.privacy.retentionTime` and `io.acryl.privacy.retentionTime02`.

<Tabs>
<TabItem value="OpenAPI v2" label="OpenAPI v2">

Let's start by creating the second structured property.

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime02/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "qualifiedName": "io.acryl.privacy.retentionTime02",
    "displayName": "Retention Time 02",
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

This command will attach one of each of the two properties to our test dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`
Specifically, this will set `io.acryl.privacy.retentionTime` as `60.0` and `io.acryl.privacy.retentionTime02` as `bar2`.


```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
      "values": [
        {"double": 60.0}
      ]
    },
    {
      "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02",
      "values": [
        {"string": "bar2"}
      ]
    }
  ]
}' | jq
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

Let's start by creating the second structured property.

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime02/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"qualifiedName": "io.acryl.privacy.retentionTime02",
		"displayName": "Retention Time 02",
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
	}
}' | jq
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02",
  "propertyDefinition": {
    "value": {
      "allowedValues": [
        {
          "value": {
            "string": "foo2"
          },
          "description": "test foo2 value"
        },
        {
          "value": {
            "string": "bar2"
          },
          "description": "test bar2 value"
        }
      ],
      "entityTypes": [
        "urn:li:entityType:datahub.dataset"
      ],
      "qualifiedName": "io.acryl.privacy.retentionTime02",
      "displayName": "Retention Time 02",
      "cardinality": "SINGLE",
      "valueType": "urn:li:dataType:datahub.string"
    }
  }
}
```

This command will attach one of each of the two properties to our test dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`
Specically, this will set `io.acryl.privacy.retentionTime` as `60.0` and `io.acryl.privacy.retentionTime02` as `bar2`.


```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties?createIfNotExists=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"properties": [
			{
			  "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
			  "values": [
				{"double": 60.0}
			  ]
			},
			{
			  "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02",
			  "values": [
				{"string": "bar2"}
			  ]
			}
		  ]
	}
}' | jq
```

Example Response:

```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
  "structuredProperties": {
    "value": {
      "properties": [
        {
          "values": [
            {
              "double": 60
            }
          ],
          "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime"
        },
        {
          "values": [
            {
              "string": "bar2"
            }
          ],
          "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02"
        }
      ]
    }
  }
}
```

</TabItem>
</Tabs>

#### Expected Outcomes
You can see that the dataset now has two structured properties attached to it.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/sp-add.png"/>
</p>



### Remove Structured Property Value

The expected state of our test dataset include 2 structured properties. 
We'd like to remove the first one (`io.acryl.privacy.retentionTime`) and preserve the second property. (`io.acryl.privacy.retentionTime02`).

<Tabs>
<TabItem value="OpenAPI v2" label="OpenAPI v2">

```shell
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
```
The response will show that the expected property has been removed.

```json
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
            "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02"
          }
        ]
      }
    }
  }
}
```
</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

```shell
curl -X 'PATCH' -v \
  'http://localhost:8080/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
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
```
The response will show that the expected property has been removed.

```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
  "structuredProperties": {
    "value": {
      "properties": [
        {
          "values": [
            {
              "string": "bar2"
            }
          ],
          "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02"
        }
      ]
    }
  }
}
```
</TabItem>

</Tabs>

#### Expected Outcomes
You can see that the first property has been removed and the second property is still present.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/sp-remove.png"/>
</p>



### Upsert Structured Property Value

In this example, we'll add the property back with a different value, preserving the existing property.

<Tabs>
<TabItem value="graphql" label="GraphQL">

```graphql
mutation updateStructuredProperty {
  updateStructuredProperty(
    input: {
      urn: "urn:li:structuredProperty:retentionTime",
      displayName: "Retention Time",
      description: "Retention Time is used to figure out how long to retain records in a dataset",
      newAllowedValues: [
        {
          numberValue: 30,
          description: "30 days, usually reserved for datasets that are ephemeral and contain pii"
        },
        {
          numberValue: 90,
          description: "Use this for datasets that drive monthly reporting but contain pii"
        },
        {
          numberValue: 365,
          description: "Use this for non-sensitive data that can be retained for longer"
        }
      ]
    }
  ) {
    urn
  }
}

```

</TabItem>
<TabItem value="OpenAPI v2" label="OpenAPI v2">

```shell
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
                            "double": 365.0
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

Example Response:

```json
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
            "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02"
          },
          {
            "values": [
              {
                "double": 365.0
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

The response shows that the property was re-added with the new value 365.0 instead of the previous value 60.0.

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

```shell
curl -X 'PATCH' -v \
  'http://localhost:8080/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
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
                            "double": 365.0
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

Example Response:

```json
{
  "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
  "structuredProperties": {
    "value": {
      "properties": [
        {
          "values": [
            {
              "string": "bar2"
            }
          ],
          "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02"
        },
        {
          "values": [
            {
              "double": 365
            }
          ],
          "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime"
        }
      ]
    }
  }
}
```

The response shows that the property was re-added with the new value 365 instead of the previous value 60.

</TabItem>

</Tabs>

#### Expected Outcomes
You can see that the first property has been added back with a new value and the second property is still present.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/apis/tutorials/sp-upsert.png"/>
</p>


## Delete Structured Properties

There are two types of deletion present in DataHub: hard and soft delete.

:::note SOFT DELETE
A soft deleted Structured Property does not remove any underlying data on the Structured Property entity or the Structured Property's values written to other entities. 
The soft delete is 100% reversible with zero data loss. When a Structured Property is soft deleted, a few operations are not available.

Structured Property Soft Delete Effects:

- Entities with a soft deleted Structured Property value will not return the soft deleted properties
- Updates to a soft deleted Structured Property's definition are denied
- Adding a soft deleted Structured Property's value to an entity is denied
- Search filters using a soft deleted Structured Property will be denied
:::

:::note HARD DELETE
A hard deleted Structured Property REMOVES all underlying data for the Structured Property entity and the Structured Property's values written to other entities.
The hard delete is NOT reversible.

Structured Property Hard Delete Effects:

- Structured Property entity is removed
- Structured Property values are removed via PATCH MCPs on their respective entities
- Rollback is not possible
- Elasticsearch index mappings will continue to contain references to the hard deleted property until reindex
:::

### Soft Delete

<Tabs>
<TabItem value="CLI" label="CLI (Soft Delete)" default>

The following command will soft delete the test property.

```commandline
datahub delete --urn {urn}
```

</TabItem>
<TabItem value="OpenAPI v2" label="OpenAPI v2 (Soft Delete)">

The following command will soft delete the test property by writing to the status aspect.

```shell
curl -X 'POST' \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
"removed": true
}' | jq
```

If you want to **remove the soft delete**, you can do so by either hard deleting the status aspect or changing the removed boolean to `false` like below.

```shell
curl -X 'POST' \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
"removed": false
}' | jq
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3 (Soft Delete)">

The following command will soft delete the test property by writing to the status aspect.

```shell
curl -X 'POST' \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"removed": true
	}
}' | jq
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "status": {
    "value": {
      "removed": true
    }
  }
}
```

If you want to **remove the soft delete**, you can do so by either hard deleting the status aspect or changing the removed boolean to `false` like below.

```shell
curl -X 'POST' \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/status?systemMetadata=false&createIfNotExists=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"removed": true
	}
}' | jq
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "status": {
    "value": {
      "removed": false
    }
  }
}
```

</TabItem>

</Tabs>

### Hard Delete

<Tabs>
<TabItem value="CLI" label="CLI (Hard Delete)">

The following command will hard delete the test property.

```commandline
datahub delete --urn {urn} --hard
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3 (Hard Delete)">

The following command will hard delete the test property.

```shell
curl -v -X 'DELETE' \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime'
```

Example Response:

```text
> DELETE /openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime HTTP/1.1
> Host: localhost:8080
> User-Agent: curl/8.4.0
> Accept: */*
> 
< HTTP/1.1 200 OK
< Date: Fri, 14 Jun 2024 17:30:27 GMT
< Content-Length: 0
< Server: Jetty(11.0.19)
```
</TabItem>

</Tabs>

#### Index Mappings Cleanup

After the asynchronous delete of all Structured Property values have been processed, triggered by the above
hard delete, it is possible to remove the remaining index mappings. Note that if even 1 Structured Property value remains
the mapping will not be removed for a given entity index.

Run the DataHub system-update job (automatically run with every helm upgrade or install and quickstart) with
the following environment variables enabled.

This will trigger an ES index which will take time to complete. During the process the entire index is recreated.

```shell
ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX=true
ENABLE_STRUCTURED_PROPERTIES_SYSTEM_UPDATE=true
```

## Update Structured Property With Breaking Schema Changes

This section will demonstrate how to make backwards incompatible schema changes. Making backwards incompatible
schema changes will remove previously written data.

Breaking schema changes are implemented by setting a version string within the Structured Property definition. This
version must be in the following format: `yyyyMMddhhmmss`, i.e. `20240614080000`

:::note IMPORTANT NOTES
Old values will not be retrieve-able after the new Structured Property definition is applied. 

The old values will be subject to deletion asynchronously (future work).
:::

In the following example, we'll revisit the `retentionTime` structured property and apply a breaking change
by changing the cardinality from `MULTIPLE` to `SINGLE`. Normally this change would be rejected as a
backwards incompatible change since values that were previously written may have multiple values written
which would no longer be valid.

<Tabs>
<TabItem value="CLI" label="CLI" default>

Edit the previously created definition yaml: Change the cardinality to `SINGLE` and add a `version`.

```yaml
- id: io.acryl.privacy.retentionTime
  # - urn: urn:li:structuredProperty:io.acryl.privacy.retentionTime # optional if id is provided
  qualified_name: io.acryl.privacy.retentionTime # required if urn is provided
  type: number
  cardinality: SINGLE
  version: '20240614080000'
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
<TabItem value="OpenAPI v3" label="OpenAPI v3">

Change the cardinality to `SINGLE` and add a `version`.

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v3/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Aio.acryl.privacy.retentionTime/propertyDefinition?createIfNotExists=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
	"value": {
		"qualifiedName": "io.acryl.privacy.retentionTime",
		"valueType": "urn:li:dataType:datahub.number",
		"description": "Retention Time is used to figure out how long to retain records in a dataset",
		"displayName": "Retention Time",
		"cardinality": "SINGLE",
		"version": "20240614080000",
		"entityTypes": [
			"urn:li:entityType:datahub.dataset",
			"urn:li:entityType:datahub.dataFlow"
		],
		"allowedValues": [
			{
				"value": {
					"double": 30
				},
				"description": "30 days, usually reserved for datasets that are ephemeral and contain pii"
			},
			{
				"value": {
					"double": 60
				},
				"description": "Use this for datasets that drive monthly reporting but contain pii"
			},
			{
				"value": {
					"double": 365
				},
				"description": "Use this for non-sensitive data that can be retained for longer"
			}
		]
	}
}' | jq
```

Example Response:

```json
{
  "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
  "propertyDefinition": {
    "value": {
      "allowedValues": [
        {
          "description": "30 days, usually reserved for datasets that are ephemeral and contain pii",
          "value": {
            "double": 30
          }
        },
        {
          "description": "Use this for datasets that drive monthly reporting but contain pii",
          "value": {
            "double": 60
          }
        },
        {
          "description": "Use this for non-sensitive data that can be retained for longer",
          "value": {
            "double": 365
          }
        }
      ],
      "displayName": "Retention Time",
      "qualifiedName": "io.acryl.privacy.retentionTime",
      "valueType": "urn:li:dataType:datahub.number",
      "description": "Retention Time is used to figure out how long to retain records in a dataset",
      "entityTypes": [
        "urn:li:entityType:datahub.dataset",
        "urn:li:entityType:datahub.dataFlow"
      ],
      "version": "20240614080000",
      "cardinality": "SINGLE"
    }
  }
}
```

</TabItem>
</Tabs>

## Structured Properties - Search & Aggregation

Currently Structured Properties can be used to filter search results. This currently excludes fulltext search.

The following examples re-use the two previously defined Structured Properties.

`io.acryl.privacy.retentionTime` - An example numeric property.

`io.acryl.privacy.retentionTime02` - An example string property.

<Tabs>
<TabItem value="GraphQL" label="GraphQL" default>

Range Query:

Document should be returned based on the previously assigned value of 60.

```graphql
query {
    scrollAcrossEntities(
        input: {
            types: DATASET,
            count: 10,
            query: "*",
            orFilters: {
                and: [
                    {
                        field: "structuredProperties.io.acryl.privacy.retentionTime",
                        condition: GREATER_THAN,
                        values: [
                            "45.0"
                        ]
                    }
                ]
            }
        }
    ) {
        searchResults {
            entity {
                urn,
                type
            }
        }
    }
}
```

Exists Query:

Document should be returned based on the previously assigned value.

```graphql
query {
  scrollAcrossEntities(
    input: {
      types: DATASET,
      count: 10,
      query: "*",
      orFilters: {
        and: [
          {
            field: "structuredProperties.io.acryl.privacy.retentionTime",
            condition: EXISTS
          }
        ]
      }
    }
  ) {
    searchResults {
      entity {
        urn,
        type
      }
    }
  }
}
```

Equality Query:

Document should be returned based on the previously assigned value of 'bar2'.

```graphql
query {
  scrollAcrossEntities(
    input: {
      types: DATASET,
      count: 10,
      query: "*",
      orFilters: {
        and: [
          {
            field: "structuredProperties.io.acryl.privacy.retentionTime02",
            condition: EQUAL
            values: [
              "bar2"
            ]
          }
        ]
      }
    }
  ) {
    searchResults {
      entity {
        urn,
        type
      }
    }
  }
}
```

</TabItem>

<TabItem value="OpenAPI v3" label="OpenAPI v3">

Unlike GraphQL which has a parsed input object for filtering, OpenAPI only includes a structured query which
relies on the `query_string` syntax. See the Elasticsearch [documentation](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl-query-string-query.html) for detailed syntax.

In order to use the `query_string` syntax we'll need to know a bit about the Structured Property's definition such
as whether it is versioned or un-unversioned and its type. This information will be added to the `query` url parameter.

Un-versioned Example:

Structured Property URN - `urn:li:structuredProperty:io.acryl.privacy.retentionTime`

Elasticsearch Field Name - `structuredProperties.io_acryl_privacy_retentionTime`

Versioned:

Structured Property Version - `20240614080000`

Structured Property Type - `string`

Structured Property URN - `urn:li:structuredProperty:io.acryl.privacy.retentionTime02`

Elasticsearch Field Name - `structuredProperties._versioned.io_acryl_privacy_retentionTime02.20240614080000.string`

Range Query:

query - `structuredProperties.io_acryl_privacy_retentionTime:>45`

```shell
curl -X 'GET' \
  'http://localhost:9002/openapi/v3/entity/dataset?systemMetadata=false&aspects=datasetKey&aspects=structuredProperties&count=10&sort=urn&sortOrder=ASCENDING&query=structuredProperties.io_acryl_privacy_retentionTime%3A%3E45' \
  -H 'accept: application/json'
```

Example Response:

```json
{
  "entities": [
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "datasetKey": {
        "value": {
          "name": "SampleHiveDataset",
          "platform": "urn:li:dataPlatform:hive",
          "origin": "PROD"
        }
      },
      "structuredProperties": {
        "value": {
          "properties": [
            {
              "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
              "values": [
                {
                  "double": 60
                }
              ]
            },
            {
              "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02",
              "values": [
                {
                  "string": "bar2"
                }
              ]
            }
          ]
        }
      }
    }
  ]
}
```

Exists Query:

query - `_exists_:structuredProperties.io_acryl_privacy_retentionTime`

```shell
curl -X 'GET' \
  'http://localhost:9002/openapi/v3/entity/dataset?systemMetadata=false&aspects=datasetKey&aspects=structuredProperties&count=10&sort=urn&sortOrder=ASCENDING&query=_exists_%3AstructuredProperties.io_acryl_privacy_retentionTime' \
  -H 'accept: application/json'
```

Example Response:

```json
{
  "entities": [
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "datasetKey": {
        "value": {
          "name": "SampleHiveDataset",
          "platform": "urn:li:dataPlatform:hive",
          "origin": "PROD"
        }
      },
      "structuredProperties": {
        "value": {
          "properties": [
            {
              "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
              "values": [
                {
                  "double": 60
                }
              ]
            },
            {
              "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02",
              "values": [
                {
                  "string": "bar2"
                }
              ]
            }
          ]
        }
      }
    }
  ]
}
```

Equality Query:

query - `structuredProperties._versioned.io_acryl_privacy_retentionTime02.20240614080000.string`

```shell
curl -X 'GET' \
  'http://localhost:9002/openapi/v3/entity/dataset?systemMetadata=false&aspects=datasetKey&aspects=structuredProperties&count=10&sort=urn&sortOrder=ASCENDING&query=structuredProperties._versioned.io_acryl_privacy_retentionTime02.20240614080000.string' \
  -H 'accept: application/json'
```

Example Response:

```json
{
  "entities": [
    {
      "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
      "datasetKey": {
        "value": {
          "name": "SampleHiveDataset",
          "platform": "urn:li:dataPlatform:hive",
          "origin": "PROD"
        }
      },
      "structuredProperties": {
        "value": {
          "properties": [
            {
              "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
              "values": [
                {
                  "double": 60
                }
              ]
            },
            {
              "propertyUrn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime02",
              "values": [
                {
                  "string": "bar2"
                }
              ]
            }
          ]
        }
      }
    }
  ]
}
```

</TabItem>
</Tabs>

### Structured Property Aggregations

Structured properties can also be used in GraphQL's aggregation queries using the same naming convention outlined above 
for search filter field names. There are currently no aggregation endpoints for OpenAPI.

<Tabs>
<TabItem value="GraphQL" label="GraphQL" default>

Aggregation Query:

```graphql
query {
  aggregateAcrossEntities(
    input: {
      types: [], 
      facets: [
        "structuredProperties.io.acryl.privacy.retentionTime02",
        "structuredProperties.io.acryl.privacy.retentionTime"], 
      query: "*", 
      orFilters: [], 
      searchFlags: {maxAggValues: 100}
    }) {
  facets {
    field
      aggregations {
        value
        count
      }
    }
  }
}
```

Example Response:

```json
{
  "data": {
    "aggregateAcrossEntities": {
      "facets": [
        {
          "field": "structuredProperties.io.acryl.privacy.retentionTime02",
          "aggregations": [
            {
              "value": "bar2",
              "count": 1
            }
          ]
        },
        {
          "field": "structuredProperties.io.acryl.privacy.retentionTime",
          "aggregations": [
            {
              "value": "60.0",
              "count": 1
            }
          ]
        }
      ]
    }
  },
  "extensions": {}
}
```

</TabItem>
</Tabs>
