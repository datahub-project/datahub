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
