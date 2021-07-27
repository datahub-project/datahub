# dbt

This plugin pulls metadata from dbt's artifact files:

- [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - This file contains model, source and lineage data.
- [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
  - This file contains schema data.
  - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
- [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
  - This file contains metadata for sources with freshness checks.
  - We transfer dbt's freshness checks to DataHub's last-modified fields.
  - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.
- target_platform:
  - The data platform you are enriching with dbt metadata.
  - [data platforms](https://github.com/linkedin/datahub/blob/master/gms/impl/src/main/resources/DataPlatformInfo.json)
- load_schemas:
  - Load schemas from dbt catalog file, not necessary when the underlying data platform already has this data.
- node_type_pattern:
  - Use this filter to exclude and include node types using allow or deny method

```yml
source:
  type: "dbt"
  config:
    manifest_path: "./path/dbt/manifest_file.json"
    catalog_path: "./path/dbt/catalog_file.json"
    sources_path: "./path/dbt/sources_file.json" # (optional, used for freshness checks)
    target_platform: "postgres" # optional, eg "postgres", "snowflake", etc.
    load_schemas: True or False
    node_type_pattern: # optional
      deny:
        - ^test.*
      allow:
        - ^.*
```

Note: when `load_schemas` is False, models that use [identifiers](https://docs.getdbt.com/reference/resource-properties/identifier) to reference their source tables are ingested using the model identifier as the model name to preserve the lineage.
