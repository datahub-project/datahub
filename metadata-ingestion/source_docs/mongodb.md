# MongoDB

## Setup

To install this plugin, run `pip install 'acryl-datahub[mongodb]'`.

## Capabilities

This plugin extracts the following:

- List of databases
- List of collections in each database and infers schemas for each collection

By default, schema inference samples 1,000 documents from each collection. Setting `schemaSamplingSize: null` will scan the entire collection.
Moreover, setting `useRandomSampling: False` will sample the first documents found without random selection, which may be faster for large collections.

Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

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

    # Regex pattern to allow/deny databases. If left blank, will ingest all.
    database_pattern:
      deny:
        # ...
      allow:
        # ...
    # Regex pattern to allow/deny collections. If left blank, will ingest all.
    collection_pattern:
      deny:
        # ...
      allow:
        # ...
```

## Config details

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
