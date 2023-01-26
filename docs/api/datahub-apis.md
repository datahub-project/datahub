# Which DataHub API is for me?

DataHub supplys several APIs to manipulate metadata on the platform:
- The [GraphQL API](docs/api/graphql/getting-started.md) is our recommended option for querying and manipulating the metadata graph. 
- The [Rest.li API](docs/api/restli/restli-overview.md) is a much more powerful, low level API intended only for advanced users. 
- We offer an [OpenAPI](docs/api/openapi/openapi-usage-guide.md) schema defining the Rest.li API used by our SDKs.


## GraphQL API

The GraphQL API serves as the primary public API for the platform. It can be used to fetch and update metadata programatically in the language of your choice.

<a
    className='button button--primary button--lg'
    href="graphql/getting-started">
    Get started with the GraphQL API
</a>



## Rest.li API
:::caution
The Rest.li API is intended only for advanced users. If you're just getting started with DataHub, we recommend the GraphQL API
:::

The Rest.li API represents the underlying persistence layer, and exposes the raw PDL models used in storage. Under the hood, it powers the GraphQL API. Aside from that, it is also used for system-specific ingestion of metadata, being used by the Metadata Ingestion Framework for pushing metadata into DataHub directly. For all intents and purposes, the Rest.li API is considered system-internal, meaning DataHub components are the only ones to consume this API directly.
<a
    className='button button--primary button--lg'
    href="restli/restli-overview">
    Get started with our Rest.li API
</a>

## OpenAPI

A schema defining the Rest.li API.
<a
    className='button button--primary button--lg'
    href="openapi/openapi-usage-guide">
    Get started with OpenAPI
</a>