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
  table [here](https://datahubproject.io/docs/graphql/enums/#fabrictype)
- **platform**: a valid platform like kafka, snowflake, etc..
- **platform_instance**: optional string specifying the platform instance of this entity

For example if dataset URN is `urn:li:dataset:(urn:li:dataPlatform:redshift,userdb.public.customer_table,DEV)` then **EntityConfig** will look like:
  ```yml
  name : userdb.public.customer_table
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

You can also view an example lineage file checked in [here](../../../../metadata-ingestion/examples/bootstrap_data/file_lineage.yml)
