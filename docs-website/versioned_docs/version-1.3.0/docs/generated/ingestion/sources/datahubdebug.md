---
sidebar_position: 13
title: DataHubDebug
slug: /generated/ingestion/sources/datahubdebug
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/datahubdebug.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DataHubDebug
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)


DataHubDebugSource is helper to debug things in executor where ingestion is running.

This source can perform the following tasks:
1. Network probe of a URL. Different from test connection in sources as that is after source starts.



### CLI based Ingestion

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">dns_probe_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "additionalProperties": false,
  "properties": {
    "dns_probe_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Dns Probe Url"
    }
  },
  "title": "DataHubDebugSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.debug.datahub_debug.DataHubDebugSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/debug/datahub_debug.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for DataHubDebug, feel free to ping us on [our Slack](https://datahub.com/slack).
