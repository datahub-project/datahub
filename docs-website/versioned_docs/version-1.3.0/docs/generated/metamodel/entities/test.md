---
sidebar_position: 23
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
   - DataJob via `testResults.failing`
   - DataFlow via `testResults.failing`
   - DataProcess via `testResults.failing`
   - DataProcessInstance via `testResults.failing`
   - Chart via `testResults.failing`
   - Dashboard via `testResults.failing`
   - Notebook via `testResults.failing`
   - Corpuser via `testResults.failing`
   - CorpGroup via `testResults.failing`
   - Domain via `testResults.failing`
   - Container via `testResults.failing`
   - Tag via `testResults.failing`
   - GlossaryTerm via `testResults.failing`
   - GlossaryNode via `testResults.failing`
   - MlModel via `testResults.failing`
   - MlModelGroup via `testResults.failing`
   - MlModelDeployment via `testResults.failing`
   - MlFeatureTable via `testResults.failing`
   - MlFeature via `testResults.failing`
   - MlPrimaryKey via `testResults.failing`
- IsPassing

   - Dataset via `testResults.passing`
   - DataJob via `testResults.passing`
   - DataFlow via `testResults.passing`
   - DataProcess via `testResults.passing`
   - DataProcessInstance via `testResults.passing`
   - Chart via `testResults.passing`
   - Dashboard via `testResults.passing`
   - Notebook via `testResults.passing`
   - Corpuser via `testResults.passing`
   - CorpGroup via `testResults.passing`
   - Domain via `testResults.passing`
   - Container via `testResults.passing`
   - Tag via `testResults.passing`
   - GlossaryTerm via `testResults.passing`
   - GlossaryNode via `testResults.passing`
   - MlModel via `testResults.passing`
   - MlModelGroup via `testResults.passing`
   - MlModelDeployment via `testResults.passing`
   - MlFeatureTable via `testResults.passing`
   - MlFeature via `testResults.passing`
   - MlPrimaryKey via `testResults.passing`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
