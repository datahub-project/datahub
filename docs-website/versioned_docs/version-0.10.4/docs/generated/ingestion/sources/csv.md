---
sidebar_position: 6
title: CSV
slug: /generated/ingestion/sources/csv
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/csv.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# CSV

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

This plugin is used to bulk upload metadata to Datahub.
It will apply glossary terms, tags, decription, owners and domain at the entity level. It can also be used to apply tags,
glossary terms, and documentation at the column level. These values are read from a CSV file. You have the option to either overwrite
or append existing values.

The format of the CSV is demonstrated below. The header is required and URNs should be surrounded by quotes when they contains commas (most URNs contains commas).

```
resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain
"urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub.growth.users,PROD",,[urn:li:glossaryTerm:Users],[urn:li:tag:HighQuality],[urn:li:corpuser:lfoe;urn:li:corpuser:jdoe],TECHNICAL_OWNER,"description for users table",urn:li:domain:Engineering
"urn:li:dataset:(urn:li:dataPlatform:hive,datahub.growth.users,PROD",first_name,[urn:li:glossaryTerm:FirstName],,,,"first_name description"
"urn:li:dataset:(urn:li:dataPlatform:hive,datahub.growth.users,PROD",last_name,[urn:li:glossaryTerm:LastName],,,,"last_name description"
```

Note that the first row does not have a subresource populated. That means any glossary terms, tags, and owners will
be applied at the entity field. If a subresource is populated (as it is for the second and third rows), glossary
terms and tags will be applied on the column. Every row MUST have a resource. Also note that owners can only
be applied at the resource level.

:::note
This source will not work on very large csv files that do not fit in memory.
:::

### CLI based Ingestion

#### Install the Plugin

The `csv-enricher` source works out of the box with `acryl-datahub`.

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
# See https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for customization options
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                              | Description                                                                                                                                                                                                                                                                              |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">filename</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | File path or URL of CSV file to ingest.                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">array_delimiter</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                | Delimiter to use when parsing array fields (tags, terms and owners) <div className="default-line default-line-with-docs">Default: <span className="default-value">&#124;</span></div>                                                                                                    |
| <div className="path-line"><span className="path-main">delimiter</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                      | Delimiter to use when parsing CSV <div className="default-line default-line-with-docs">Default: <span className="default-value">,</span></div>                                                                                                                                           |
| <div className="path-line"><span className="path-main">write_semantics</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                | Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE" <div className="default-line default-line-with-docs">Default: <span className="default-value">PATCH</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "CSVEnricherConfig",
  "type": "object",
  "properties": {
    "filename": {
      "title": "Filename",
      "description": "File path or URL of CSV file to ingest.",
      "type": "string"
    },
    "write_semantics": {
      "title": "Write Semantics",
      "description": "Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be \"PATCH\" or \"OVERRIDE\"",
      "default": "PATCH",
      "type": "string"
    },
    "delimiter": {
      "title": "Delimiter",
      "description": "Delimiter to use when parsing CSV",
      "default": ",",
      "type": "string"
    },
    "array_delimiter": {
      "title": "Array Delimiter",
      "description": "Delimiter to use when parsing array fields (tags, terms and owners)",
      "default": "|",
      "type": "string"
    }
  },
  "required": [
    "filename"
  ],
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.csv_enricher.CSVEnricherSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/csv_enricher.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for CSV, feel free to ping us on [our Slack](https://slack.datahubproject.io).
