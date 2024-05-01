# Which DataHub API is for me?

DataHub supplys several APIs to manipulate metadata on the platform. These are our most-to-least recommended approaches:

- Our most recommended tools for extending and customizing the behavior of your DataHub instance are our SDKs in [Python](metadata-ingestion/as-a-library.md) and [Java](metadata-integration/java/as-a-library.md).
- If you'd like to customize the DataHub client or roll your own; the [GraphQL API](docs/api/graphql/getting-started.md) is our what powers our frontend. We figure if it's good enough for us, it's good enough for everyone! If `graphql` doesn't cover everything in your usecase, drop into [our slack](docs/slack.md) and let us know how we can improve it!
- If you are less familiar with `graphql` and would rather use OpenAPI, we offer [OpenAPI](docs/api/openapi/openapi-usage-guide.md) endpoints that allow you to produce metadata events and query metadata.
- Finally, if you're a brave soul and know exactly what you are doing... are you sure you don't just want to use the SDK directly? If you insist, the [Rest.li API](docs/api/restli/restli-overview.md) is a much more powerful, low level API intended only for advanced users.

## Python and Java SDK

We offer an SDK for both Python and Java that provide full functionality when it comes to CRUD operations and any complex functionality you may want to build into DataHub.
<a
    className='button button--primary button--lg'
    href="/docs/metadata-ingestion/as-a-library">
Get started with the Python SDK
</a>

<a
    className='button button--primary button--lg'
    href="/docs/metadata-integration/java/as-a-library">
Get started with the Java SDK
</a>

## GraphQL API

The `graphql` API serves as the primary public API for the platform. It can be used to fetch and update metadata programatically in the language of your choice. Intended as a higher-level API that simplifies the most common operations.

<a
    className='button button--primary button--lg'
    href="/docs/api/graphql/getting-started">
Get started with the GraphQL API
</a>

## OpenAPI

For developers who prefer OpenAPI to GraphQL for programmatic operations. Provides lower-level API access to the entire DataHub metadata model for writes, reads and queries.
<a
    className='button button--primary button--lg'
    href="/docs/api/openapi/openapi-usage-guide">
Get started with OpenAPI
</a>

## Rest.li API

:::caution
The Rest.li API is intended only for advanced users. If you're just getting started with DataHub, we recommend the GraphQL API
:::

The Rest.li API represents the underlying persistence layer, and exposes the raw PDL models used in storage. Under the hood, it powers the GraphQL API. Aside from that, it is also used for system-specific ingestion of metadata, being used by the Metadata Ingestion Framework for pushing metadata into DataHub directly. For all intents and purposes, the Rest.li API is considered system-internal, meaning DataHub components are the only ones to consume this API directly.
<a
    className='button button--primary button--lg'
    href="/docs/api/restli/restli-overview">
Get started with our Rest.li API
</a>

## DataHub API Comparison

DataHub supports several APIs, each with its own unique usage and format.
Here's an overview of what each API can do.

> Last Updated : Feb 16 2024

| Feature                            | GraphQL                                                                      | Python SDK                                                                   | OpenAPI |
|------------------------------------|------------------------------------------------------------------------------|------------------------------------------------------------------------------|---------|
| Create a Dataset                   | 🚫                                                                           | ✅ [[Guide]](/docs/api/tutorials/datasets.md)                                 | ✅       |
| Delete a Dataset (Soft Delete)     | ✅ [[Guide]](/docs/api/tutorials/datasets.md#delete-dataset)                  | ✅ [[Guide]](/docs/api/tutorials/datasets.md#delete-dataset)                  | ✅       |
| Delete a Dataset (Hard Delete)     | 🚫                                                                           | ✅ [[Guide]](/docs/api/tutorials/datasets.md#delete-dataset)                  | ✅       |
| Search a Dataset                   | ✅ [[Guide]](/docs/how/search.md#graphql)                                     | ✅                                                                            | ✅       |
| Read a Dataset Deprecation         | ✅                                                                            | ✅                                                                            | ✅       |
| Read Dataset Entities (V2)         | ✅                                                                            | ✅                                                                            | ✅       |
| Create a Tag                       | ✅ [[Guide]](/docs/api/tutorials/tags.md#create-tags)                         | ✅ [[Guide]](/docs/api/tutorials/tags.md#create-tags)                         | ✅       |
| Read a Tag                         | ✅ [[Guide]](/docs/api/tutorials/tags.md#read-tags)                           | ✅ [[Guide]](/docs/api/tutorials/tags.md#read-tags)                           | ✅       |
| Add Tags to a Dataset              | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-dataset)               | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-dataset)               | ✅       |
| Add Tags to a Column of a Dataset  | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-column-of-a-dataset)   | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-column-of-a-dataset)   | ✅       |
| Remove Tags from a Dataset         | ✅ [[Guide]](/docs/api/tutorials/tags.md#remove-tags)                         | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags#remove-tags)                | ✅       |
| Create Glossary Terms              | ✅ [[Guide]](/docs/api/tutorials/terms.md#create-terms)                       | ✅ [[Guide]](/docs/api/tutorials/terms.md#create-terms)                       | ✅       |
| Read Terms from a Dataset          | ✅ [[Guide]](/docs/api/tutorials/terms.md#read-terms)                         | ✅ [[Guide]](/docs/api/tutorials/terms.md#read-terms)                         | ✅       |
| Add Terms to a Column of a Dataset | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-column-of-a-dataset) | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-column-of-a-dataset) | ✅       |
| Add Terms to a Dataset             | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-dataset)             | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-dataset)             | ✅       |
| Create Domains                     | ✅ [[Guide]](/docs/api/tutorials/domains.md#create-domain)                    | ✅ [[Guide]](/docs/api/tutorials/domains.md#create-domain)                    | ✅       |
| Read Domains                       | ✅ [[Guide]](/docs/api/tutorials/domains.md#read-domains)                     | ✅ [[Guide]](/docs/api/tutorials/domains.md#read-domains)                     | ✅       |
| Add Domains to a Dataset           | ✅ [[Guide]](/docs/api/tutorials/domains.md#add-domains)                      | ✅ [[Guide]](/docs/api/tutorials/domains.md#add-domains)                      | ✅       |
| Remove Domains from a Dataset      | ✅ [[Guide]](/docs/api/tutorials/domains.md#remove-domains)                   | ✅ [[Guide]](/docs/api/tutorials/domains.md#remove-domains)                   | ✅       |
| Create / Upsert Users              | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-users)                      | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-users)                      | ✅       |
| Create / Upsert Group              | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-group)                      | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-group)                      | ✅       |
| Read Owners of a Dataset           | ✅ [[Guide]](/docs/api/tutorials/owners.md#read-owners)                       | ✅ [[Guide]](/docs/api/tutorials/owners.md#read-owners)                       | ✅       |
| Add Owner to a Dataset             | ✅ [[Guide]](/docs/api/tutorials/owners.md#add-owners)                        | ✅ [[Guide]](/docs/api/tutorials/owners.md#add-owners#remove-owners)          | ✅       |
| Remove Owner from a Dataset        | ✅ [[Guide]](/docs/api/tutorials/owners.md#remove-owners)                     | ✅ [[Guide]](/docs/api/tutorials/owners.md)                                   | ✅       |
| Add Lineage                        | ✅ [[Guide]](/docs/api/tutorials/lineage.md)                     | ✅ [[Guide]](/docs/api/tutorials/lineage.md#add-lineage)                      | ✅ |
| Add Column Level (Fine Grained) Lineage                  | 🚫                                                            | ✅ [[Guide]](docs/api/tutorials/lineage.md#add-column-level-lineage)                                                           | ✅       |
| Add Documentation (Description) to a Column of a Dataset | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-column) | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-column) | ✅       |
| Add Documentation (Description) to a Dataset             | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-dataset) | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-dataset) | ✅       |
| Add / Remove / Replace Custom Properties on a Dataset    | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/custom-properties.md)        | ✅       |
| Add ML Feature to ML Feature Table                       | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#add-mlfeature-to-mlfeaturetable)        | ✅       |
| Add ML Feature to MLModel                                | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#add-mlfeature-to-mlmodel)        | ✅       |
| Add ML Group to MLFeatureTable                           | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#add-mlgroup-to-mlfeaturetable)        | ✅       |
| Create MLFeature                                         | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlfeature)      | ✅       |
| Create MLFeatureTable                                    | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlfeaturetable) | ✅       |
| Create MLModel                                           | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlmodel)        | ✅       |
| Create MLModelGroup                                      | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlmodelgroup)   | ✅       |
| Create MLPrimaryKey                                      | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlprimarykey)   | ✅       |
| Create MLFeatureTable                                    | 🚫                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlfeaturetable)| ✅       |
| Read MLFeature                                           | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeature)        | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeature)        | ✅       |
| Read MLFeatureTable                                      | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeaturetable)   | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeaturetable)   | ✅       |
| Read MLModel                                             | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodel)          | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodel)          | ✅       |
| Read MLModelGroup                                        | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodelgroup)     | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodelgroup)     | ✅       |
| Read MLPrimaryKey                                        | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlprimarykey)     | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlprimarykey)     | ✅       |
| Create Data Product                                      | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/create_dataproduct.py)        | ✅       |
| Create Lineage Between Chart and Dashboard               | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_chart_dashboard.py) | ✅       |
| Create Lineage Between Dataset and Chart                 | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_chart.py) | ✅       |
| Create Lineage Between Dataset and DataJob               | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_job_dataset.py) | ✅       |
| Create Finegrained Lineage as DataJob for Dataset        | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py) | ✅       |
| Create Finegrained Lineage for Dataset                  | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py)        | ✅       |
| Create Dataset Lineage with Kafka                       | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_kafka.py)        | ✅       |
| Create Dataset Lineage with MCPW & Rest Emitter         | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py)        | ✅       |
| Create Dataset Lineage with Rest Emitter                | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_rest.py)        | ✅       |
| Create DataJob with Dataflow                            | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_job_dataflow.py) [[Simple]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_job_dataflow_new_api_simple.py) [[Verbose]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_job_dataflow_new_api_verbose.py) | ✅       |
| Create Programmatic Pipeline                            | 🚫                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/programatic_pipeline.py) | ✅       |
