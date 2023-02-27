# Which DataHub API is for me?

DataHub supplys several APIs to manipulate metadata on the platform. These are our most-to-least recommended approaches:

- Our most recommended tools for extending and customizing the behavior of your DataHub instance are our SDKs in [Python](metadata-ingestion/as-a-library.md) and [Java](metadata-integration/java/as-a-library.md).
- If you'd like to customize the DataHub client or roll your own; the [GraphQL API](docs/api/graphql/getting-started.md) is our what powers our frontend. We figure if it's good enough for us, it's good enough for everyone!
- If GraphQL doesn't cover everything in your usecase, drop into [our slack](docs/slack.md) and let us know how we can improve it! As an alternative, we offer an [OpenAPI](docs/api/openapi/openapi-usage-guide.md) schema defining the Rest.li API used by our SDKs.
- If you're a brave soul and know exactly what you are doing... are you sure you don't just want to use the SDK directly? If you insist, the [Rest.li API](docs/api/restli/restli-overview.md) is a much more powerful, low level API intended only for advanced users.

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

The GraphQL API serves as the primary public API for the platform. It can be used to fetch and update metadata programatically in the language of your choice.

<a
    className='button button--primary button--lg'
    href="/docs/api/graphql/getting-started">
Get started with the GraphQL API
</a>

## OpenAPI

The schema powering our SDKs, which defines the Rest.li API.
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
