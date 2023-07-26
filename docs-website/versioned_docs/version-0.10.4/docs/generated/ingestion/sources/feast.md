---
sidebar_position: 13
title: Feast
slug: /generated/ingestion/sources/feast
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/feast.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Feast

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability          | Status | Notes              |
| ------------------- | ------ | ------------------ |
| Table-Level Lineage | ✅     | Enabled by default |

This plugin extracts:

- Entities as [`MLPrimaryKey`](/docs/graphql/objects#mlprimarykey)
- Fields as [`MLFeature`](/docs/graphql/objects#mlfeature)
- Feature views and on-demand feature views as [`MLFeatureTable`](/docs/graphql/objects#mlfeaturetable)
- Batch and stream source details as [`Dataset`](/docs/graphql/objects#dataset)
- Column types associated with each entity and feature

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[feast]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: feast
  config:
    # Coordinates
    path: "/path/to/repository/"
    # Options
    environment: "PROD"

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                          | Description                                                                                                                                               |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">path</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to Feast repository                                                                                                                                  |
| <div className="path-line"><span className="path-main">environment</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                | Environment to use when constructing URNs <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">fs_yaml_file</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                               | Path to the `feature_store.yaml` file used to configure the feature store                                                                                 |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "FeastRepositorySourceConfig",
  "type": "object",
  "properties": {
    "path": {
      "title": "Path",
      "description": "Path to Feast repository",
      "type": "string"
    },
    "fs_yaml_file": {
      "title": "Fs Yaml File",
      "description": "Path to the `feature_store.yaml` file used to configure the feature store",
      "type": "string"
    },
    "environment": {
      "title": "Environment",
      "description": "Environment to use when constructing URNs",
      "default": "PROD",
      "type": "string"
    }
  },
  "required": [
    "path"
  ],
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.feast.FeastRepositorySource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/feast.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Feast, feel free to ping us on [our Slack](https://slack.datahubproject.io).
