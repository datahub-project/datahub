---
sidebar_position: 26
title: SchemaField
slug: /generated/metamodel/entities/schemafield
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/schemaField.md
---

# SchemaField

## Aspects

### schemaFieldKey

Key for a SchemaField

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "schemaFieldKey"
  },
  "name": "SchemaFieldKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "fieldType": "URN"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "parent",
      "doc": "Parent associated with the schema field"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "type": "string",
      "name": "fieldPath",
      "doc": "fieldPath identifying the schema field"
    }
  ],
  "doc": "Key for a SchemaField"
}
```

</details>

## Relationships

### Incoming

These are the relationships stored in other entity's aspects

- DownstreamOf

  - Dataset via `upstreamLineage.fineGrainedLineages`

- ForeignKeyTo

  - Dataset via `schemaMetadata.foreignKeys.foreignFields`
  - GlossaryTerm via `schemaMetadata.foreignKeys.foreignFields`

- Consumes

  - DataJob via `dataJobInputOutput.inputDatasetFields`

- Produces

  - DataJob via `dataJobInputOutput.outputDatasetFields`

- consumesField

  - Chart via `inputFields.fields.schemaFieldUrn`
  - Dashboard via `inputFields.fields.schemaFieldUrn`

- Asserts

  - Assertion via `assertionInfo.datasetAssertion.fields`

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
