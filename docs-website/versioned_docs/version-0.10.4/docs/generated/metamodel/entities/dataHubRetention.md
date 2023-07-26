---
sidebar_position: 38
title: DataHubRetention
slug: /generated/metamodel/entities/datahubretention
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataHubRetention.md
---

# DataHubRetention

## Aspects

### dataHubRetentionKey

Key for a DataHub Retention

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubRetentionKey"
  },
  "name": "DataHubRetentionKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "type": "string",
      "name": "entityName",
      "doc": "Entity name to apply retention to. * (or empty) for applying defaults."
    },
    {
      "type": "string",
      "name": "aspectName",
      "doc": "Aspect name to apply retention to. * (or empty) for applying defaults."
    }
  ],
  "doc": "Key for a DataHub Retention"
}
```

</details>

### dataHubRetentionConfig

None

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataHubRetentionConfig"
  },
  "name": "DataHubRetentionConfig",
  "namespace": "com.linkedin.retention",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "Retention",
        "namespace": "com.linkedin.retention",
        "fields": [
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "VersionBasedRetention",
                "namespace": "com.linkedin.retention",
                "fields": [
                  {
                    "type": "int",
                    "name": "maxVersions"
                  }
                ],
                "doc": "Keep max N latest records"
              }
            ],
            "name": "version",
            "default": null
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "TimeBasedRetention",
                "namespace": "com.linkedin.retention",
                "fields": [
                  {
                    "type": "int",
                    "name": "maxAgeInSeconds"
                  }
                ],
                "doc": "Keep records that are less than X seconds old"
              }
            ],
            "name": "time",
            "default": null
          }
        ],
        "doc": "Base class that encapsulates different retention policies.\nOnly one of the fields should be set"
      },
      "name": "retention"
    }
  ]
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
