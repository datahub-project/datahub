---
sidebar_position: 15
title: File Based Lineage
slug: /generated/ingestion/sources/file-based-lineage
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/file-based-lineage.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# File Based Lineage

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

This plugin pulls lineage metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/file_lineage.yml).

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[datahub-lineage-file]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: datahub-lineage-file
  config:
    # Coordinates
    file: /path/to/file_lineage.yml
    # Whether we want to query datahub-gms for upstream data
    preserve_upstream: False

sink:
# sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                          | Description                                                                                                                                                                                                                                                                                                                                |
| :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <div className="path-line"><span className="path-main">file</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | File path or URL to lineage file to ingest.                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">preserve_upstream</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                         | Whether we want to query datahub-gms for upstream data. False means it will hard replace upstream data for a given entity. True means it will query the backend for existing upstreams and include it in the ingestion run <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "LineageFileSourceConfig",
  "type": "object",
  "properties": {
    "file": {
      "title": "File",
      "description": "File path or URL to lineage file to ingest.",
      "type": "string"
    },
    "preserve_upstream": {
      "title": "Preserve Upstream",
      "description": "Whether we want to query datahub-gms for upstream data. False means it will hard replace upstream data for a given entity. True means it will query the backend for existing upstreams and include it in the ingestion run",
      "default": true,
      "type": "boolean"
    }
  },
  "required": [
    "file"
  ],
  "additionalProperties": false
}
```

</TabItem>
</Tabs>

### Lineage File Format

The lineage source file should be a `.yml` file with the following top-level keys:

**version**: the version of lineage file config the config conforms to. Currently, the only version released
is `1`.

**lineage**: the top level key of the lineage file containing a list of **EntityNodeConfig** objects

**EntityNodeConfig**:

- **entity**: **EntityConfig** object
- **upstream**: (optional) list of child **EntityNodeConfig** objects
- **fineGrainedLineages**: (optional) list of **FineGrainedLineageConfig** objects

**EntityConfig**:

- **name**: identifier of the entity. Typically name or guid, as used in constructing entity urn.
- **type**: type of the entity (only `dataset` is supported as of now)
- **env**: the environment of this entity. Should match the values in the
  table [here](/docs/graphql/enums/#fabrictype)
- **platform**: a valid platform like kafka, snowflake, etc..
- **platform_instance**: optional string specifying the platform instance of this entity

For example if dataset URN is `urn:li:dataset:(urn:li:dataPlatform:redshift,userdb.public.customer_table,DEV)` then **EntityConfig** will look like:

```yml
name: userdb.public.customer_table
type: dataset
env: DEV
platform: redshift
```

**FineGrainedLineageConfig**:

- **upstreamType**: type of upstream entity in a fine-grained lineage; default = "FIELD_SET"
- **upstreams**: (optional) list of upstream schema field urns
- **downstreamType**: type of downstream entity in a fine-grained lineage; default = "FIELD_SET"
- **downstreams**: (optional) list of downstream schema field urns
- **transformOperation**: (optional) transform operation applied to the upstream entities to produce the downstream field(s)
- **confidenceScore**: (optional) the confidence in this lineage between 0 (low confidence) and 1 (high confidence); default = 1.0

**FineGrainedLineageConfig** can be used to display fine grained lineage, also referred to as column-level lineage,
for custom sources.

You can also view an example lineage file checked in [here](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/bootstrap_data/file_lineage.yml)

### Code Coordinates

- Class Name: `datahub.ingestion.source.metadata.lineage.LineageFileSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/metadata/lineage.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for File Based Lineage, feel free to ping us on [our Slack](https://slack.datahubproject.io).
