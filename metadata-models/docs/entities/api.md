# API

An API is a named callable with a typed input and output schema — an MCP tool, a REST endpoint, a
gRPC method, a GraphQL operation, a function, and so on. Cataloging APIs as first-class entities lets
both humans and agents discover existing callables before building new ones, and makes
caller → API dependencies (the services that compose an API, the agents that invoke it) visible in
the graph.

The kind of API is captured with the standard `subTypes` aspect. The canonical subtypes are
`MCP_TOOL`, `REST_ENDPOINT`, `GRPC_METHOD`, `GRAPHQL_OPERATION`, and `FUNCTION` — each grounded in a
concrete callable kind rather than in how the API happens to be used ("being an agent's tool" is a
relationship, not an intrinsic kind — an MCP tool is just an API).

## Identity

APIs are identified by a single field:

- **id**: A unique identifier for the API, such as `order-lookup-mcp` or a generated UUID.

An example URN is `urn:li:api:order-lookup-mcp`. For REST endpoints the id is minted from the owning
service, HTTP method, and path (e.g. `order-entry-api/GET/orders/{orderId}`) so the same operation
resolves to the same URN whether it is registered by hand or scraped from an OpenAPI spec. Because a
`(method, path)` pair identifies one REST operation, an endpoint served under multiple methods
(GET vs POST on the same path) is modeled as a distinct API entity per method.

## Important Capabilities

### API Properties

The `apiProperties` aspect holds catalog identity:

- **name**: Display name, searchable with autocomplete.
- **description**: What the API does and when to use it.
- **externalUrl**: Optional link to the API's registry entry, documentation, or source.
- **sourceRepository**: The [Repository](repository.md) the API is produced from
  (`SourcedFrom`), lighting up the `repo → service → api → app → dataset` chain.
- **created** / **lastModified**: Audit stamps.

### Signature

The input/output contract lives on a separate `apiSignature` aspect, kept apart from `apiProperties`
because the signature is scraped from the endpoint (an OpenAPI/JSON-Schema doc, an MCP tool manifest,
a function definition) and evolves on its own cadence — when the contract changes, only this aspect is
re-ingested, leaving catalog identity untouched. It carries:

- **schemaDefinition**: The full input/output schema as an opaque string (typically JSON Schema) — the
  source-of-truth representation that round-trips even constructs the structured fields can't capture.
- **inputFields** / **outputFields**: A structured, typed view reusing DataHub's schema-field model,
  so nested/struct/array types, nullability, and field descriptions render with the standard schema
  components. Each field's `fieldPath` is the parameter name; `nullable=false` means a required argument.

### REST Properties

For APIs of subtype `REST_ENDPOINT`, the `restApiProperties` aspect records the `method` (HTTP verb)
and `path` (route template relative to the owning service's base URL, e.g. `/orders/{orderId}`). These
are kept on a subtype-specific aspect so callers can filter by method and group endpoints that share
a path.

### Relationships

An API is connected to the rest of the software-to-data graph through incoming edges:

- A [Service](service.md) composes it (`ServiceComposesApi`).
- [AI Agents](aiAgent.md) invoke it as a tool (`AgentUsesTool`).
- [Agent Skills](agentSkill.md) require it (`SkillRequiresTool`).

### Governance and Versioning

APIs support the standard governance aspects — ownership, tags, glossary terms, domains, structured
properties, and institutional memory — plus native versioning (`versionProperties`).
