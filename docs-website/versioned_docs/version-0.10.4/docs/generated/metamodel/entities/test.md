---
sidebar_position: 24
title: Test
slug: /generated/metamodel/entities/test
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/test.md
---

# Test

A DataHub test

## Aspects

### testInfo

Information about a DataHub Test

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testInfo"
  },
  "name": "TestInfo",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Searchable": {
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "name",
      "doc": "The name of the test"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "category",
      "doc": "Category of the test"
    },
    {
      "Searchable": {
        "fieldType": "TEXT"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Description of the test"
    },
    {
      "type": {
        "type": "record",
        "name": "TestDefinition",
        "namespace": "com.linkedin.test",
        "fields": [
          {
            "type": {
              "type": "enum",
              "symbolDocs": {
                "JSON": "JSON / YAML test def"
              },
              "name": "TestDefinitionType",
              "namespace": "com.linkedin.test",
              "symbols": [
                "JSON"
              ]
            },
            "name": "type",
            "doc": "The Test Definition Type"
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "json",
            "default": null,
            "doc": "JSON format configuration for the test"
          }
        ]
      },
      "name": "definition",
      "doc": "Configuration for the Test"
    }
  ],
  "doc": "Information about a DataHub Test"
}
```

</details>

## Relationships

### Incoming

These are the relationships stored in other entity's aspects

- IsFailing

  - Dataset via `testResults.failing`

- IsPassing

  - Dataset via `testResults.passing`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
