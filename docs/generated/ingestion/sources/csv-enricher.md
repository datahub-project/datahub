


# CSV Enricher

## Overview

Csv Enricher is a DataHub utility or metadata-focused integration. Learn more in the [official Csv Enricher documentation](https://datahub.com/docs/).

The DataHub integration for Csv Enricher covers metadata entities and operational objects relevant to this connector. It also captures ownership and tags.

:::info Looking to ingest a CSV data file into DataHub, as an asset?

Use the Local File ingestion source. The CSV enricher is used for enriching entities already ingested into DataHub.
:::

## Concept Mapping

While the specific concept mapping is still pending, this shows the generic concept mapping in DataHub.

| Source Concept                                           | DataHub Concept              | Notes                                                            |
| -------------------------------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| Platform/account/project scope                           | Platform Instance, Container | Organizes assets within the platform context.                    |
| Core technical asset (for example table/view/topic/file) | Dataset                      | Primary ingested technical asset.                                |
| Schema fields / columns                                  | SchemaField                  | Included when schema extraction is supported.                    |
| Ownership and collaboration principals                   | CorpUser, CorpGroup          | Emitted by modules that support ownership and identity metadata. |
| Dependencies and processing relationships                | Lineage edges                | Available when lineage extraction is supported and enabled.      |


## Module `csv-enricher`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Descriptions | ✅ | Supported by default. |
| [Domains](../../../domains.md) | ✅ | Supported by default. |
| Extract Ownership | ✅ | Supported by default. |
| Extract Tags | ✅ | Supported by default. |

### Overview

The `csv-enricher` module ingests metadata from Csv Enricher into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This plugin is used to bulk upload metadata to DataHub. It supports structured properties in addition to existing enrichment fields such as glossary terms, tags, descriptions, owners, and domain at the entity level. It can also be used to apply tags, glossary terms, and documentation at the column level. These values are read from a CSV file. You have the option to either overwrite or append existing values.

The format of the CSV is demonstrated below. The header is required and URNs should be surrounded by quotes when they contain commas (most URNs contain commas).

```txt
resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain,ownership_type_urn,classification,owner_team
"urn:li:dataset:(urn:li:dataPlatform:snowflake,datahub.growth.users,PROD)",,[urn:li:glossaryTerm:Users],[urn:li:tag:HighQuality],[urn:li:corpuser:lfoe|urn:li:corpuser:jdoe],CUSTOM,"description for users table",urn:li:domain:Engineering,urn:li:ownershipType:a0e9176c-d8cf-4b11-963b-f7a1bc2333c9,Sensitive,Finance
"urn:li:dataset:(urn:li:dataPlatform:hive,datahub.growth.users,PROD)",first_name,[urn:li:glossaryTerm:FirstName],,,,"first_name description",
"urn:li:dataset:(urn:li:dataPlatform:hive,datahub.growth.users,PROD)",last_name,[urn:li:glossaryTerm:LastName],,,,"last_name description",
```

Note that the first row does not have a subresource populated. That means any glossary terms, tags, owners, domains, descriptions, and structured properties will be applied at the entity level. If a subresource is populated (as it is for the second and third rows), glossary terms and tags will be applied on the column. Every row MUST have a resource. Also note that owners and structured properties can only be applied at the resource level.

Structured properties are configured using explicit column mappings in the recipe via `structured_properties`.
If the value in a mapped structured property column is empty, it is ignored.

Example recipe config:

```yaml
source:
  type: csv-enricher
  config:
    filename: ./path/to/your/file.csv
    structured_properties:
      owner_team: "io.acryl.metadata.ownerTeam"
      classification: "urn:li:structuredProperty:io.acryl.privacy.classification"
```

With that config, CSV columns `owner_team` and `classification` are interpreted as structured properties.

When `write_semantics` is set to `OVERRIDE`, structured properties are replaced as part of the full entity-level overwrite behavior. This means structured properties not present in mapped CSV columns will be removed from the entity.

If ownership_type_urn is set then ownership_type must be set to CUSTOM.

Note that you have the option in your recipe config to write as a PATCH or as an OVERRIDE. This choice will apply to all metadata for the entity, not just a single aspect. So OVERRIDE will override all metadata, including performing deletes if a metadata field is empty. The default is PATCH.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.


### Install the Plugin
```shell
pip install 'acryl-datahub[csv-enricher]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: csv-enricher
  config:
    # relative path to your csv file to ingest
    filename: ./path/to/your/file.csv

    # optional explicit mapping from CSV columns to structured properties
    structured_properties:
      tier: "io.acryl.metadata.tier"
      classification: "urn:li:structuredProperty:io.acryl.privacy.classification"

# Default sink is datahub-rest and doesn't need to be configured
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization options

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">filename</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | File path or URL of CSV file to ingest.  |
| <div className="path-line"><span className="path-main">array_delimiter</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Delimiter to use when parsing array fields (tags, terms and owners) <div className="default-line default-line-with-docs">Default: <span className="default-value">&#124;</span></div> |
| <div className="path-line"><span className="path-main">delimiter</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Delimiter to use when parsing CSV <div className="default-line default-line-with-docs">Default: <span className="default-value">,</span></div> |
| <div className="path-line"><span className="path-main">structured_properties</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Explicit mapping from CSV column names to structured property ids or URNs. Example: {'classification': 'urn:li:structuredProperty:io.acryl.privacy.classification'} <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">write_semantics</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE". NOTE: this will apply to all metadata for the entity, not just a single aspect. <div className="default-line default-line-with-docs">Default: <span className="default-value">PATCH</span></div> |

</div>




#### Schema


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
    },
    "structured_properties": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Explicit mapping from CSV column names to structured property ids or URNs. Example: {'classification': 'urn:li:structuredProperty:io.acryl.privacy.classification'}",
      "title": "Structured Properties"
    }
  },
  "required": [
    "filename"
  ],
  "title": "CSVEnricherConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

:::warning Performance Considerations

This source will not work on very large csv files that do not fit in memory.
:::

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.csv_enricher.CSVEnricherSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/csv_enricher.py)


:::tip Questions?

If you've got any questions on configuring ingestion for CSV Enricher, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
