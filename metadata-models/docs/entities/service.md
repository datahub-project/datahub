# Service

A Service is a catalog entry for an external service that exposes callable APIs — an MCP server, a
REST API, a GraphQL API, a gRPC service, and so on. A Service groups the [APIs](api.md) it exposes,
carries the full interface definition, and sits between the [Repository](repository.md) that produces
it and the [Applications](application.md)/agents that consume it in the
`repo → service → api → app → dataset` chain.

The specific kind of service is recorded with the standard `subTypes` aspect (e.g. `MCP`, `REST_API`,
`OPEN_API`, `GRPC`). Note that a Service holds only catalog information (identity, description,
interface); operational configuration such as authentication for DataHub's Ask DataHub integration
lives separately in `GlobalSettings.aiPlugins`, not on the entity.

## Identity

Services are identified by a single field:

- **id**: A unique identifier for the service, such as `glean-search`, `internal-tools`, or
  `weather-api`.

An example URN is `urn:li:service:glean-search`.

## Important Capabilities

### Service Properties

The `serviceProperties` aspect holds catalog identity and the service's place in the graph:

- **displayName**: Name shown in the UI and search results, searchable with autocomplete.
- **description**: What the service provides.
- **lifecycle**: Lifecycle stage — `EXPERIMENTAL`, `PRODUCTION`, or `DEPRECATED` — rendered as a badge
  so consumers can see at a glance whether the service is safe to depend on.
- **apis**: The [APIs](api.md) this service composes/exposes (`ServiceComposesApi`). This is a lineage
  edge with the API downstream of the service, so endpoints nest correctly under their service.
- **sourceRepository**: The [Repository](repository.md) the service is produced from (`SourcedFrom`).

### Service Definition

The `serviceDefinition` aspect carries the full interface document — the source of truth from which
the operation-level API entities are parsed. It records:

- **format**: How to parse `rawSpec` — `OPENAPI`, `GRAPHQL_SDL`, `GRPC_PROTO`, `ASYNCAPI`,
  `JSON_SCHEMA`, or `OTHER`.
- **rawSpec**: The entire spec document, verbatim. Stored as a `LargeString` (optionally compressed)
  so large specs fit under the aspect-size limit; readers receive the decompressed text. Rendered
  read-only on the service profile's Definition tab.
- **version** / **externalUrl**: Optional spec version and a link to the canonical hosted source.

It is kept as its own aspect (rather than a field on `serviceProperties`) because the spec blob is
large and only needed on the profile, whereas `serviceProperties` is fetched on every search card and
header.

### MCP Server Properties

For services of subtype `MCP`, the `mcpServerProperties` aspect holds connection details: the server
`url`, the `transport` (`HTTP`, `SSE`, or `WEBSOCKET`), an optional connection `timeout`, and any
non-auth `customHeaders` (e.g. `X-Tenant-ID`). Authentication headers are configured in
`GlobalSettings.aiPlugins`, not here.

### Health and Governance

Services surface operational health through the shared incidents subsystem (`incidentsSummary`) and
support the standard ownership, tags, and status aspects.
