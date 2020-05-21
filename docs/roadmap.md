# DataHub Roadmap

Below is DataHub's roadmap for the short and medium term. We'll revise this on a regular basis and welcome suggestions from the communities.

## Short term (3-6 months)
### Aspect-specific MCE & MAE [*WIP*]
- Split up unified events to improve scalability & modularity
### Metrics as entities [*LinkedIn-internal, waiting to open source*]
- Models + UI
### Dashboards as entities [*LinkedIn-internal, waiting to open source*]
- Models + UI
### Jobs & Flows as entities [*WIP*]
- Link datasets to jobs & flows
### Schemas as an entity [*WIP*]
- Make schemas searchable
- Support GraphQL schemas
### Data privacy management for datasets [*LinkedIn-internal, waiting to open source*]
- Simple tag-based data privacy metadata
### Strongly consistent local index [*WIP*]
- Add query-after-write capability to local DAO 
### Gremlin-based Query DAO [*WIP*]
- Support majority of gremlin-compatible graph DBs
### Templatized UI [*WIP*]
- Config-driven UI
- Generate TypeScript types from Pegasus 
### Entity Insights [*WIP*]
- UI to highlight high value information about Entities within Search and Entity Pages
### Kubernetes migration
- Migration from docker-compose to [Kubernetes](https://kubernetes.io/) for Docker container orchestration
### Azure deployment
- Run DataHub in [Azure](https://azure.microsoft.com/en-us/) and provide how-to guides

## Medium term (6 months - 1 year)
### Dataset field-level lineage
- Models + impact analysis
### Operational metadata [*WIP*]
- Indexing in OLAP store ([Pinot](https://github.com/apache/incubator-pinot)) with TTL
### Social features [*WIP*]
- Users will be able to like and follow entities
- Dataset & field-level commenting
### Microservices as an entity [*WIP*]
- Initially focus on rest.li services & GraphQL integration
### Kundera-based Local DAO
- Support a wide range of document stores
### Add GraphQL endpoint to GMS
- Use GraphQL exclusively for frontend queries
### Adopt Redux
- Use Redux exclusively for UI state management
### Integration tests
- Add docker-based integration tests
### AWS & GCP deployment
- Run DataHub in [AWS](https://aws.amazon.com/) & [GCP](https://cloud.google.com/gcp) and provide how-to guides
### Apache incubation
- Donate code to Apache foundation

## Long term (1+ year)
### Rewrite midtier in Node
- TypeScript-only frontend development
### gRPC + protobuf
- Modeling in protobuf + serving in gRPC
### UI for metadata graph exploration
