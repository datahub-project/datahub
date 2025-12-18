---
sidebar_position: 53
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
      "type": [
        {
          "type": "enum",
          "symbolDocs": {
            "ABORTED": "Upgrade with an error state and should not be re-run.",
            "FAILED": "Upgrade with an error state, however the upgrade should be re-run.",
            "IN_PROGRESS": "Upgrade in progress.",
            "SUCCEEDED": "Upgrade was successful."
          },
          "name": "DataHubUpgradeState",
          "namespace": "com.linkedin.upgrade",
          "symbols": [
            "IN_PROGRESS",
            "SUCCEEDED",
            "FAILED",
            "ABORTED"
          ]
        },
        "null"
      ],
      "name": "state",
      "default": "SUCCEEDED",
      "doc": "Upgrade state  UpgradeResult.Result"
    },
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

## [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
