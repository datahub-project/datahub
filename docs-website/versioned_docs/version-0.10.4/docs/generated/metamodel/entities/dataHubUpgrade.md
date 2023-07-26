---
sidebar_position: 42
title: DataHubUpgrade
slug: /generated/metamodel/entities/datahubupgrade
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubUpgrade.md
---

# DataHubUpgrade

## Aspects

### dataHubUpgradeRequest

Information collected when kicking off a DataHubUpgrade

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubUpgradeRequest"
  },
  "name": "DataHubUpgradeRequest",
  "namespace": "com.linkedin.upgrade",
  "fields": [
    {
      "type": "long",
      "name": "timestampMs",
      "doc": "Timestamp when we started this DataHubUpgrade"
    },
    {
      "type": "string",
      "name": "version",
      "doc": "Version of this upgrade"
    }
  ],
  "doc": "Information collected when kicking off a DataHubUpgrade"
}
```

</details>

### dataHubUpgradeResult

Information collected when a DataHubUpgrade successfully finishes

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubUpgradeResult"
  },
  "name": "DataHubUpgradeResult",
  "namespace": "com.linkedin.upgrade",
  "fields": [
    {
      "type": "long",
      "name": "timestampMs",
      "doc": "Timestamp when we started this DataHubUpgrade"
    },
    {
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "name": "result",
      "default": null,
      "doc": "Result map to place helpful information about this upgrade job"
    }
  ],
  "doc": "Information collected when a DataHubUpgrade successfully finishes"
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
