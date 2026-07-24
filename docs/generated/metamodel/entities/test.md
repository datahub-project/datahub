# Test
A DataHub test


## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### testInfo
Information about a DataHub Test



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| name | string | ✓ | The name of the test | Searchable |
| category | string | ✓ | Category of the test | Searchable |
| description | string |  | Description of the test | Searchable |
| definition | TestDefinition | ✓ | Configuration for the Test |  |



#### Raw Schema


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
        "fieldType": "TEXT",
        "searchTier": 2
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





### Relationships

#### Incoming
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
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
