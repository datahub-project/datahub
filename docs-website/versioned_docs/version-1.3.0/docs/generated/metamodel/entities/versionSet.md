---
sidebar_position: 25
title: VersionSet
slug: /generated/metamodel/entities/versionset
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/versionSet.md
---
# VersionSet
## Aspects

### versionSetProperties
None
<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "versionSetProperties"
  },
  "name": "VersionSetProperties",
  "namespace": "com.linkedin.versionset",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "queryByDefault": "false"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "latest",
      "doc": "The latest versioned entity linked to in this version set"
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "ALPHANUMERIC_GENERATED_BY_DATAHUB": "String managed by DataHub. Currently, an 8 character alphabetical string.",
          "LEXICOGRAPHIC_STRING": "String sorted lexicographically."
        },
        "name": "VersioningScheme",
        "namespace": "com.linkedin.versionset",
        "symbols": [
          "LEXICOGRAPHIC_STRING",
          "ALPHANUMERIC_GENERATED_BY_DATAHUB"
        ]
      },
      "name": "versioningScheme",
      "doc": "What versioning scheme is being utilized for the versioned entities sort criterion. Static once set"
    }
  ]
}
```
</details>

## Relationships

### Incoming
These are the relationships stored in other entity's aspects
- VersionOf

   - Dataset via `versionProperties.versionSet`
   - MlModel via `versionProperties.versionSet`
## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
