---
sidebar_position: 10
title: Demo Data
slug: /generated/ingestion/sources/demo-data
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/demo-data.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Demo Data

This source loads sample data into DataHub. It is intended for demo and testing purposes only.

### CLI based Ingestion

#### Install the Plugin

The `demo-data` source works out of the box with `acryl-datahub`.

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: demo-data
  config: {}
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field | Description |
| :---- | :---------- |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "DemoDataConfig",
  "type": "object",
  "properties": {},
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.demo_data.DemoDataSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/demo_data.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Demo Data, feel free to ping us on [our Slack](https://slack.datahubproject.io).
