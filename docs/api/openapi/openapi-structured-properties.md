# Structured Properties - DataHub OpenAPI v2 Guide

This guides walks through the process of creating and using a Structured Property using the `v2` version
of the DataHub OpenAPI implementation. Note that this refers to DataHub's OpenAPI version and not the version of OpenAPI itself.

Requirements:
* curl
* jq

## Structured Property Definition

Before a structured property can be added to an entity it must first be defined. Here is an example
structured property being created against a local quickstart instance.

### Create Property Definition

Example Request:

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Amy.test.MyProperty01/propertyDefinition' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "qualifiedName": "my.test.MyProperty01",
    "displayName": "MyProperty01",
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

### Read Property Definition

Example Request:

```shell
curl -X 'GET' -v \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Amy.test.MyProperty01/propertyDefinition' \
  -H 'accept: application/json' | jq
```

Example Response:

```json
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
    "qualifiedName": "my.test.MyProperty01",
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

### Delete Property Definition

There are two types of deletion present in DataHub: `hard` and `soft` delete. As of the current release only the `soft` delete
is supported for Structured Properties. See the subsections below for more details.

#### Soft Delete

A `soft` deleted Structured Property does not remove any underlying data on the Structured Property entity
or the Structured Property's values written to other entities. The `soft` delete is 100% reversible with zero data loss.
When a Structured Property is `soft` deleted, a few operations are not available.

Structured Property Soft Delete Effects:

* Entities with a `soft` deleted Structured Property value will not return the `soft` deleted properties
* Updates to a `soft` deleted Structured Property's definition are denied
* Adding a `soft` deleted Structured Property's value to an entity is denied
* Search filters using a `soft` deleted Structured Property will be denied

The following command will `soft` delete the test property `MyProperty01` created in this guide by writing
to the `status` aspect.

```shell
curl -X 'POST' \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Amy.test.MyProperty01/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
"removed": true
}' | jq
```

Removing the `soft` delete from the Structured Property can be done by either `hard` deleting the `status` aspect or
changing the `removed` boolean to `false.

```shell
curl -X 'POST' \
  'http://localhost:8080/openapi/v2/entity/structuredProperty/urn%3Ali%3AstructuredProperty%3Amy.test.MyProperty01/status?systemMetadata=false' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
"removed": false
}' | jq
```

#### Hard Delete

⚠ **Not Implemented** ⚠

## Applying Structured Properties

Structured Properties can now be added to entities which have the `structuredProperties` as aspect. In the following
example we'll attach and remove properties to an example dataset entity with urn `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`.

### Set Structured Property Values

This will set/replace all structured properties on the entity. See `PATCH` operations to add/remove a single property.

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty01",
      "values": [
        {"string": "foo"}
      ]
    }
  ]
}' | jq
```

### Patch Structured Property Value

For this example, we'll extend create a second structured property and apply both properties to the same
dataset used previously. After this your system should include both `my.test.MyProperty01` and `my.test.MyProperty02`.

```shell
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

```shell
curl -X 'POST' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty01",
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

#### Remove Structured Property Value

The expected state of our test dataset include 2 structured properties. We'd like to remove the first one and preserve
the second property.

```shell
curl -X 'PATCH' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json-patch+json' \
  -d '{
        "patch": [
            {
                "op": "remove",
                "path": "/properties/urn:li:structuredProperty:my.test.MyProperty01"
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
            "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty02"
          }
        ]
      }
    }
  }
}
```

#### Add Structured Property Value

In this example, we'll add the property back with a different value, preserving the existing property.

```shell
curl -X 'PATCH' -v \
  'http://localhost:8080/openapi/v2/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29/structuredProperties' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json-patch+json' \
  -d '{
        "patch": [
            {
                "op": "add",
                "path": "/properties/urn:li:structuredProperty:my.test.MyProperty01",
                "value": {
                    "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty01",
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

The response shows that the property was re-added with the new value `bar` instead of the previous value `foo`.

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
                        "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty02"
                    },
                    {
                        "values": [
                            {
                                "string": "bar"
                            }
                        ],
                        "propertyUrn": "urn:li:structuredProperty:my.test.MyProperty01"
                    }
                ]
            }
        }
    }
}
```
