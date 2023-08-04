---
sidebar_position: 0
title: Data Platform
slug: /generated/metamodel/entities/dataplatform
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/dataPlatform.md
---

# Data Platform

Data Platforms are systems or tools that contain Datasets, Dashboards, Charts, and all other kinds of data assets modeled in the metadata graph.

Examples of data platforms are `redshift`, `hive`, `bigquery`, `looker`, `tableau` etc.

## Identity

Data Platforms are identified by the name of the technology. A complete list of currently supported data platforms is available [here](https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-service/restli-servlet-impl/src/main/resources/DataPlatformInfo.json).

## Aspects

### dataPlatformKey

Key for a Data Platform

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformKey"
  },
  "name": "DataPlatformKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "type": "string",
      "name": "platformName",
      "doc": "Data platform name i.e. hdfs, oracle, espresso"
    }
  ],
  "doc": "Key for a Data Platform"
}
```

</details>

### dataPlatformInfo

Information about a data platform

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInfo"
  },
  "name": "DataPlatformInfo",
  "namespace": "com.linkedin.dataplatform",
  "fields": [
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": false,
        "fieldType": "TEXT_PARTIAL"
      },
      "validate": {
        "strlen": {
          "max": 15
        }
      },
      "type": "string",
      "name": "name",
      "doc": "Name of the data platform"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldType": "TEXT_PARTIAL"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "displayName",
      "default": null,
      "doc": "The name that will be used for displaying a platform type."
    },
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "FILE_SYSTEM": "Value for a file system, e.g. hdfs",
          "KEY_VALUE_STORE": "Value for a key value store, e.g. espresso, voldemort",
          "MESSAGE_BROKER": "Value for a message broker, e.g. kafka",
          "OBJECT_STORE": "Value for an object store, e.g. ambry",
          "OLAP_DATASTORE": "Value for an OLAP datastore, e.g. pinot",
          "OTHERS": "Value for other platforms, e.g salesforce, dovetail",
          "QUERY_ENGINE": "Value for a query engine, e.g. presto",
          "RELATIONAL_DB": "Value for a relational database, e.g. oracle, mysql",
          "SEARCH_ENGINE": "Value for a search engine, e.g seas"
        },
        "name": "PlatformType",
        "namespace": "com.linkedin.dataplatform",
        "symbols": [
          "FILE_SYSTEM",
          "KEY_VALUE_STORE",
          "MESSAGE_BROKER",
          "OBJECT_STORE",
          "OLAP_DATASTORE",
          "OTHERS",
          "QUERY_ENGINE",
          "RELATIONAL_DB",
          "SEARCH_ENGINE"
        ],
        "doc": "Platform types available at LinkedIn"
      },
      "name": "type",
      "doc": "Platform type this data platform describes"
    },
    {
      "type": "string",
      "name": "datasetNameDelimiter",
      "doc": "The delimiter in the dataset names on the data platform, e.g. '/' for HDFS and '.' for Oracle"
    },
    {
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "logoUrl",
      "default": null,
      "doc": "The URL for a logo associated with the platform"
    }
  ],
  "doc": "Information about a data platform"
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/datahub-project/static-assets/main//imgs/datahub-metadata-model.png)
