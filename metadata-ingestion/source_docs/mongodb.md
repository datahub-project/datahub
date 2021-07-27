# MongoDB

To install this plugin, run `pip install 'acryl-datahub[mongodb]'`.

This plugin extracts the following:

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
    # used for PyMongo
    authMechanism: "DEFAULT"

    options: {} # kwargs to pass to pymongo.MongoClient
    enableSchemaInference: True
    schemaSamplingSize: 1000 # number of samples for determining schema
    useRandomSampling: True # whether to randomly sample docs for schema or just use the first ones, True by default

    env: "PROD" # Optional, default is "PROD"

    # regex pattern to allow/deny databases
    database_pattern:
      deny:
        # ...
      allow:
        # ...
    # regex pattern to allow/deny collections
    collection_pattern:
      deny:
        # ...
      allow:
        # ...
```
