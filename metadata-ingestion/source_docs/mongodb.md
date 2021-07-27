# MongoDB `mongodb`

Extracts:

- List of databases
- List of collections in each database and infers schemas for each collection

By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

```yml
source:
  type: "mongodb"
  config:
    # For advanced configurations, see the MongoDB docs.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    connect_uri: "mongodb://localhost"
    username: admin
    password: password
    env: "PROD" # Optional, default is "PROD"
    authMechanism: "DEFAULT"
    options: {}
    database_pattern: {}
    collection_pattern: {}
    enableSchemaInference: True
    schemaSamplingSize: 1000
    useRandomSampling: True # whether to randomly sample docs for schema or just use the first ones, True by default
    # database_pattern/collection_pattern are similar to schema_pattern/table_pattern from above
```
