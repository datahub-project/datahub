---
sidebar_position: 9
title: CSV Enricher
slug: /generated/ingestion/sources/csv-enricher
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/csv-enricher.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# CSV Enricher
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Supported by default. |
| [Domains](../../../domains.md) | ✅ | Supported by default. |
| Extract Ownership | ✅ | Supported by default. |
| Extract Tags | ✅ | Supported by default. |


:::tip Looking to ingest a CSV data file into DataHub, as an asset?
Use the [Local File](./s3.md) ingestion source.
The CSV enricher is used for enriching entities already ingested into DataHub.
:::

This plugin is used to bulk upload metadata to Datahub.
It will apply glossary terms, tags, description, owners and domain at the entity level. It can also be used to apply tags,
glossary terms, and documentation at the column level. These values are read from a CSV file. You have the option to either overwrite
or append existing values.

The format of the CSV is demonstrated below. The header is required and URNs should be surrounded by quotes when they contains commas (most URNs contains commas).

```
resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain,ownership_type_urn
"urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub.growth.users,PROD)",,[urn:li:glossaryTerm:Users],[urn:li:tag:HighQuality],[urn:li:corpuser:lfoe|urn:li:corpuser:jdoe],CUSTOM,"description for users table",urn:li:domain:Engineering,urn:li:ownershipType:a0e9176c-d8cf-4b11-963b-f7a1bc2333c9
"urn:li:dataset:(urn:li:dataPlatform:hive,datahub.growth.users,PROD)",first_name,[urn:li:glossaryTerm:FirstName],,,,"first_name description",
"urn:li:dataset:(urn:li:dataPlatform:hive,datahub.growth.users,PROD)",last_name,[urn:li:glossaryTerm:LastName],,,,"last_name description",
```

Note that the first row does not have a subresource populated. That means any glossary terms, tags, and owners will
be applied at the entity field. If a subresource is populated (as it is for the second and third rows), glossary
terms and tags will be applied on the column. Every row MUST have a resource. Also note that owners can only
be applied at the resource level.

If ownership_type_urn is set then ownership_type must be set to CUSTOM.

Note that you have the option in your recipe config to write as a PATCH or as an OVERRIDE. This choice will apply to
all metadata for the entity, not just a single aspect. So OVERRIDE will override all metadata, including performing
deletes if a metadata field is empty. The default is PATCH.

:::note
This source will not work on very large csv files that do not fit in memory.
:::


### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: csv-enricher 
  config:
    # relative path to your csv file to ingest
    filename: ./path/to/your/file.csv

# Default sink is datahub-rest and doesn't need to be configured
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization options

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">filename</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | File path or URL of CSV file to ingest.  |
| <div className="path-line"><span className="path-main">array_delimiter</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Delimiter to use when parsing array fields (tags, terms and owners) <div className="default-line default-line-with-docs">Default: <span className="default-value">&#124;</span></div> |
| <div className="path-line"><span className="path-main">delimiter</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Delimiter to use when parsing CSV <div className="default-line default-line-with-docs">Default: <span className="default-value">,</span></div> |
| <div className="path-line"><span className="path-main">write_semantics</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE". NOTE: this will apply to all metadata for the entity, not just a single aspect. <div className="default-line default-line-with-docs">Default: <span className="default-value">PATCH</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "additionalProperties": false,
  "properties": {
    "filename": {
      "description": "File path or URL of CSV file to ingest.",
      "title": "Filename",
      "type": "string"
    },
    "write_semantics": {
      "default": "PATCH",
      "description": "Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be \"PATCH\" or \"OVERRIDE\". NOTE: this will apply to all metadata for the entity, not just a single aspect.",
      "title": "Write Semantics",
      "type": "string"
    },
    "delimiter": {
      "default": ",",
      "description": "Delimiter to use when parsing CSV",
      "title": "Delimiter",
      "type": "string"
    },
    "array_delimiter": {
      "default": "|",
      "description": "Delimiter to use when parsing array fields (tags, terms and owners)",
      "title": "Array Delimiter",
      "type": "string"
    }
  },
  "required": [
    "filename"
  ],
  "title": "CSVEnricherConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.csv_enricher.CSVEnricherSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/csv_enricher.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for CSV Enricher, feel free to ping us on [our Slack](https://datahub.com/slack).
