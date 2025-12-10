---
title: DataHub APIs and SDKs Overview
sidebar_label: APIs and SDKs Overview
slug: /api/datahub-apis
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/datahub-apis.md
---
# DataHub APIs and SDKs Overview

DataHub has several APIs to manipulate metadata on the platform. Here's the list of APIs and their pros and cons to help you choose the right one for your use case.

| API                                                        | Definition                         | Pros                                     | Cons                                                                                                                      |
| ---------------------------------------------------------- | ---------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **[Python SDK](/metadata-ingestion/as-a-library.md)**      | SDK                                | Highly flexible, Good for bulk execution | Requires an understanding of the metadata change event                                                                    |
| **[Java SDK](/metadata-integration/java/as-a-library.md)** | SDK                                | Highly flexible, Good for bulk execution | Requires an understanding of the metadata change event                                                                    |
| **[GraphQL API](docs/api/graphql/getting-started.md)**     | GraphQL interface                  | Intuitive; mirrors UI capabilities       | Less flexible than SDKs; requires knowledge of GraphQL syntax                                                             |
| **[OpenAPI](docs/api/openapi/openapi-usage-guide.md)**     | Lower-level API for advanced users | Most powerful and flexible               | Can be hard to use for straightforward use cases; no corresponding SDKs, but OpenAPI spec is generated within the product |

In general, **Python and Java SDKs** are our most recommended tools for extending and customizing the behavior of your DataHub instance, especially for programmatic use cases.

:::warning
About async usage of APIs - DataHub's asynchronous APIs perform only basic schema validation when receiving MCP requests, similar to direct production to MCP Kafka topics. While requests must conform to the MCP schema to be accepted, actual processing happens later in the pipeline. Any processing failures that occur after the initial acceptance are captured in the Failed MCP topic, but these failures are not immediately surfaced to the API caller since they happen asynchronously.
:::

## Python and Java SDK

We offer an SDK for both Python and Java that provide full functionality when it comes to CRUD operations and any complex functionality you may want to build into DataHub. We recommend using the SDKs for most use cases. Here are the examples of how to use the SDKs:

- Define a lineage between data entities
- Executing bulk operations - e.g. adding tags to multiple datasets
- Creating custom metadata entities

Learn more about the SDKs:

- **[Python SDK →](/metadata-ingestion/as-a-library.md)**
- **[Java SDK →](/metadata-integration/java/as-a-library.md)**

## GraphQL API

The `graphql` API serves as the primary API used by the DataHub frontend. It is generally assumed that accesses to the GraphQL API are coming in from the frontend so it often comes along with default caching, synchronous operations, and other UI targeted expectations. Care should be taken when used programmatically to fetch and update due to this since operations are intentionally limited in scope. Intended as a higher-level API that simplifies the most common operations.

The GraphQL API can be useful if you're getting started with DataHub since it's more user-friendly and straightfoward, especially when using GraphiQL. Here are some examples of how to use the GraphQL API:

- Search for datasets with conditions
- Query for relationships between entities

Learn more about the GraphQL API:

- **[GraphQL API →](docs/api/graphql/getting-started.md)**

## DataHub API Comparison

DataHub supports several APIs, each with its own unique usage and format.
Here's an overview of what each API can do.

> Last Updated : Feb 16 2024

| Feature                                                  | GraphQL                                                                       | Python SDK                                                                                                                                     | OpenAPI |
| -------------------------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ------- | --- | --- |
| Create a Dataset                                         | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/datasets.md)                                                                                                  | ✅      |
| Delete a Dataset (Soft Delete)                           | ✅ [[Guide]](/docs/api/tutorials/datasets.md#delete-dataset)                  | ✅ [[Guide]](/docs/api/tutorials/datasets.md#delete-dataset)                                                                                   | ✅      |
| Delete a Dataset (Hard Delete)                           | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/datasets.md#delete-dataset)                                                                                   | ✅      |
| Search a Dataset                                         | ✅ [[Guide]](/docs/how/search.md#graphql)                                     | ✅                                                                                                                                             | ✅      |
| Read a Dataset Deprecation                               | ✅                                                                            | ✅                                                                                                                                             | ✅      |
| Read Dataset Entities (V2)                               | ✅                                                                            | ✅                                                                                                                                             | ✅      |
| Create a Tag                                             | ✅ [[Guide]](/docs/api/tutorials/tags.md#create-tags)                         | ✅ [[Guide]](/docs/api/tutorials/tags.md#create-tags)                                                                                          | ✅      |
| Read a Tag                                               | ✅ [[Guide]](/docs/api/tutorials/tags.md#read-tags)                           | ✅ [[Guide]](/docs/api/tutorials/tags.md#read-tags)                                                                                            | ✅      |
| Add Tags to a Dataset                                    | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-dataset)               | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-dataset)                                                                                | ✅      |
| Add Tags to a Column of a Dataset                        | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-column-of-a-dataset)   | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags-to-a-column-of-a-dataset)                                                                    | ✅      |
| Remove Tags from a Dataset                               | ✅ [[Guide]](/docs/api/tutorials/tags.md#remove-tags)                         | ✅ [[Guide]](/docs/api/tutorials/tags.md#add-tags#remove-tags)                                                                                 | ✅      |
| Create Glossary Terms                                    | ✅ [[Guide]](/docs/api/tutorials/terms.md#create-terms)                       | ✅ [[Guide]](/docs/api/tutorials/terms.md#create-terms)                                                                                        | ✅      |
| Read Terms from a Dataset                                | ✅ [[Guide]](/docs/api/tutorials/terms.md#read-terms)                         | ✅ [[Guide]](/docs/api/tutorials/terms.md#read-terms)                                                                                          | ✅      |
| Add Terms to a Column of a Dataset                       | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-column-of-a-dataset) | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-column-of-a-dataset)                                                                  | ✅      |
| Add Terms to a Dataset                                   | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-dataset)             | ✅ [[Guide]](/docs/api/tutorials/terms.md#add-terms-to-a-dataset)                                                                              | ✅      |
| Create Domains                                           | ✅ [[Guide]](/docs/api/tutorials/domains.md#create-domain)                    | ✅ [[Guide]](/docs/api/tutorials/domains.md#create-domain)                                                                                     | ✅      |
| Read Domains                                             | ✅ [[Guide]](/docs/api/tutorials/domains.md#read-domains)                     | ✅ [[Guide]](/docs/api/tutorials/domains.md#read-domains)                                                                                      | ✅      |
| Add Domains to a Dataset                                 | ✅ [[Guide]](/docs/api/tutorials/domains.md#add-domains)                      | ✅ [[Guide]](/docs/api/tutorials/domains.md#add-domains)                                                                                       | ✅      |
| Remove Domains from a Dataset                            | ✅ [[Guide]](/docs/api/tutorials/domains.md#remove-domains)                   | ✅ [[Guide]](/docs/api/tutorials/domains.md#remove-domains)                                                                                    | ✅      |
| Create / Upsert Users                                    | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-users)                      | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-users)                                                                                       | ✅      |
| Create / Upsert Group                                    | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-group)                      | ✅ [[Guide]](/docs/api/tutorials/owners.md#upsert-group)                                                                                       | ✅      |
| Read Owners of a Dataset                                 | ✅ [[Guide]](/docs/api/tutorials/owners.md#read-owners)                       | ✅ [[Guide]](/docs/api/tutorials/owners.md#read-owners)                                                                                        | ✅      |
| Add Owner to a Dataset                                   | ✅ [[Guide]](/docs/api/tutorials/owners.md#add-owners)                        | ✅ [[Guide]](/docs/api/tutorials/owners.md#add-owners#remove-owners)                                                                           | ✅      |
| Remove Owner from a Dataset                              | ✅ [[Guide]](/docs/api/tutorials/owners.md#remove-owners)                     | ✅ [[Guide]](/docs/api/tutorials/owners.md)                                                                                                    | ✅      |
| Add Lineage                                              | ✅ [[Guide]](/docs/api/tutorials/lineage.md)                                  | ✅ [[Guide]](/docs/api/tutorials/lineage.md#add-lineage)                                                                                       | ✅      |
| Add Column Level (Fine Grained) Lineage                  | 🚫                                                                            | ✅ [[Guide]](docs/api/tutorials/lineage.md#add-column-level-lineage)                                                                           | ✅      |
| Add Documentation (Description) to a Column of a Dataset | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-column)   | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-column)                                                                    | ✅      |
| Add Documentation (Description) to a Dataset             | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-dataset)  | ✅ [[Guide]](/docs/api/tutorials/descriptions.md#add-description-on-dataset)                                                                   | ✅      |
| Add / Remove / Replace Custom Properties on a Dataset    | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/custom-properties.md)                                                                                         | ✅      |
| Add ML Feature to ML Feature Table                       | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#add-mlfeature-to-mlfeaturetable)                                                                        | ✅      |
| Add ML Feature to MLModel                                | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#add-mlfeature-to-mlmodel)                                                                               | ✅      |
| Add ML Group to MLFeatureTable                           | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#add-mlgroup-to-mlfeaturetable)                                                                          | ✅      |
| Create MLFeature                                         | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlfeature)                                                                                       | ✅      |
| Create MLFeatureTable                                    | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlfeaturetable)                                                                                  | ✅      |
| Create MLModel                                           | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlmodel)                                                                                         | ✅      |
| Create MLModelGroup                                      | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlmodelgroup)                                                                                    | ✅      |
| Create MLPrimaryKey                                      | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlprimarykey)                                                                                    | ✅      |
| Create MLFeatureTable                                    | 🚫                                                                            | ✅ [[Guide]](/docs/api/tutorials/ml.md#create-mlfeaturetable)                                                                                  | ✅      |
| Read MLFeature                                           | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeature)                        | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeature)                                                                                         | ✅      |
| Read MLFeatureTable                                      | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeaturetable)                   | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlfeaturetable)                                                                                    | ✅      |
| Read MLModel                                             | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodel)                          | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodel)                                                                                           | ✅      |
| Read MLModelGroup                                        | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodelgroup)                     | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlmodelgroup)                                                                                      | ✅      |
| Read MLPrimaryKey                                        | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlprimarykey)                     | ✅ [[Guide]](/docs/api/tutorials/ml.md#read-mlprimarykey)                                                                                      | ✅      |
| Create Data Product                                      | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/create_dataproduct.py)                  | ✅      |
| Create Lineage Between Chart and Dashboard               | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_chart_dashboard.py)             | ✅      |
| Create Lineage Between Dataset and Chart                 | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_chart.py)               | ✅      |
| Create Lineage Between Dataset and DataJob               | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_job_dataset.py)         | ✅      |
| Create Finegrained Lineage as DataJob for Dataset        | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py) | ✅      |
| Create Finegrained Lineage for Dataset                   | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py) | ✅      |     | ✅  |
| Create DataJob with Dataflow                             | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_job_dataflow.py)                | ✅      |
| Create Programmatic Pipeline                             | 🚫                                                                            | ✅ [[Code]](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/programatic_pipeline.py)                | ✅      |
