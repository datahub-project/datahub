# DataHub Roadmap

Below is DataHub's roadmap for the short and medium term. We'll revise this on a regular basis and welcome suggestions from the communities.

## Short term (3-6 months)
### Replace PDSC with PDL
- Simplified syntax + annotations
### Entity-specific MCE & MAE
- Spilt up unified events to improve scalability & modularity
### Jobs & Flows as entities
- Link datasets to jobs & flows
### Schemas as an entity
- Make schemas searchable
- Support GraphQL shcmeas
### Compliance management for datasets
- Simple tag-based compliance metadata
### Strongly consistent local index
- Add query-after-write capability to local DAO 
### Gremlin-based Query DAO
- Support majority of gremlin-compatible graph DBs
### Templatized UI
- Config-driven UI
- Generate TypeScript types from Pegasus 
### Kubernetes migration
- Migration from docker-compose to [Kubernetes](https://kubernetes.io/) for Docker container orchestration
### Azure deployment
- Run DataHub in [Azure](https://azure.microsoft.com/en-us/) and provide how-to guides

## Medium term (6 months - 1 year)
### Fine grain lineage
- Models + impact analysis
### Operational metadata
- Indexing in OLAP store ([Pinot](https://github.com/apache/incubator-pinot)) with TTL
### Social features
- Users will be able to like and follow entities
- Dataset & field-level commenting
### Microservices as an entity
- Initially focus on rest.li services & GraphQL integration
### Kundera-based Local DAO
- Support a wide range of document stores
### Adopt Redux
- Use Redux exclusively for UI state management
### Integration tests
- Add docker-based integration tests
### AWS & GCP deployment
- Run DataHub in [AWS](https://aws.amazon.com/) & [GCP](https://cloud.google.com/gcp) and provide how-to guides
### Apache incubation

## Long term (1+ year)
### Rewrite midtier in Node
- TypeScript-only frontend development
### gRPC + protobuf
- Modeling in protobuf + serveing in gRPC
