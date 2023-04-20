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

> Last Updated : Apr 8 2023

| Feature                                                 | GraphQL                                                                                      | Python SDK                                                                                    | OpenAPI |
|---------------------------------------------------------|----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------| ------- |
| Create a dataset                                        | 🚫                                                                                           | ✅ [[Guide]](/docs/api/tutorials/modifying-datasets.md)                                        | ✅      |
| Delete a dataset (Soft delete)                          | ✅ [[Guide]](/docs/api/tutorials/modifying-datasets.md#delete-dataset)                        | ✅ [[Guide]](/docs/api/tutorials/modifying-datasets.md#delete-dataset)                         | ✅      |
| Delete a dataset (Hard delele)                          | 🚫                                                                                           | ✅ [[Guide]](/docs/api/tutorials/modifying-datasets.md#delete-dataset)                         | ✅      |
| Search a dataset                                        | ✅                                                                                            | ✅                                                                                             | ✅      |
| Create a tag                                            | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md)                                   | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md)                                    | ✅      |
| Read a tag                                              | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md)                                   | ✅                                                                                             | ✅      |
| Add tags to a dataset                                   | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                          | ✅                                                                                             | ✅      |
| Add tags to a column of a dataset                       | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                          | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                           | ✅      |
| Remove tags from a dataset                              | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                          | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                           | ✅      |
| Create glossary terms                                   | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-terms.md)                                  | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-terms.md)                                   | ✅      |
| Read terms from a dataset                               | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                          | ✅                                                                                             | ✅      |
| Add terms to a column of a dataset                      | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-terms.md#add-temrms)                       | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-terms.md#add-temrms)                        | ✅      |
| Add terms to a dataset                                  | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-terms.md#add-temrms)                       | ✅                                                                                             | ✅      |
| Create domains                                          | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md)                                   | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md)                                    | ✅      |
| Read domains                                            | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md)                                   | ✅                                                                                             | ✅      |
| Add domains to a dataset                                | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                          | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                           | ✅      |
| Remove domains from a dataset                           | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                          | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-tags.md#add-tags)                           | ✅      |
| Crate users and groups                                  | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                      | ✅                                                                                             | ✅      |
| Read owners of a dataset                                | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                      | ✅                                                                                             | ✅      |
| Add owner to a dataset                                  | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                      | ✅                                                                                             | ✅      |
| Add owner to a column of a dataset                      | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                      | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                       | ✅      |
| Remove owner from a dataset                             | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                      | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-owners.md#add-owners)                       | ✅      |
| Add lineage                                             | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-lineage.md#add-lineage)                    | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-lineage.md#add-lineage)                     | ✅      |
| Add column level(Fine Grained) lineage                  | 🚫                                                                                           | ✅                                                                                             | ✅      |
| Add documentation(description) to a column of a dataset | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-descriptions.md#add-description-on-column) | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-descriptions.md#add-description-on-column)  | ✅      |
| Add documentation(description) to a dataset             | 🚫                                                                                           | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-descriptions.md#add-description-on-dataset) | ✅      |
| Add / Remove / Replace custom properties on a dataset   | 🚫 [[Guide]](/docs/api/tutorials/modifying-dataset-custom-properties.md)                     | ✅ [[Guide]](/docs/api/tutorials/modifying-dataset-custom-properties.md)                       | ✅       |
