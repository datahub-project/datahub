# DataHub Roadmap

Below is DataHub's roadmap for the short, medium and long term. We welcome suggestions from the community.

ETAs are revisted on a regular basis and are subject to change. If you would like to see something prioritized, please reach out to us on [Slack](https://join.slack.com/t/datahubspace/shared_invite/zt-dkzbxfck-dzNl96vBzB06pJpbRwP6RA) or attend the [town hall](townhalls.md) to discuss!

## Short term (3 months) [ETA October 2020]
### Dashboards as entities 
- Models + UI
### Jobs & Flows as entities
- Link datasets to jobs & flows
### AI models as entities
- Models + UI
### Strongly consistent secondary index (SCSI)
- Add query-after-write capability to local DAO 
### Gremlin-based Query DAO
- Support majority of gremlin-compatible graph DBs
### Integration tests
- Add docker-based integration tests
### Kubernetes migration
- Migration from docker-compose to [Kubernetes](https://kubernetes.io/) for Docker container orchestration

## Medium term (3 - 6 months) [ETA January 2021]
### Aspect-specific MCE & MAE
- Split up unified events to improve scalability & modularity
### Dataset field-level lineage
- Models + impact analysis
### Data Concepts as an entity
- Models + UI
### Metrics as entities
- Models + UI
### Schemas as an entity
- Make schemas searchable
- Support GraphQL schemas
### Entity Insights
- UI to highlight high value information about Entities within Search and Entity Pages
### Data privacy management for datasets
- Simple tag-based data privacy metadata
### Social features
- Users will be able to like and follow entities
- Dataset & field-level commenting
### Templatized UI
- Config-driven UI
- Generate TypeScript types from Pegasus 
### Add GraphQL endpoint to GMS
- Use GraphQL exclusively for frontend queries
### Adopt Redux
- Use Redux exclusively for UI state management
### JNoSQL-based Local DAO
- Support a wide range of document stores
### Ownership Transfer
- Donate code to a foundation, e.g. Apache, Linux Foundation.
### Azure deployment
- Run DataHub in [Azure](https://azure.microsoft.com/en-us/) and provide how-to guides

## Long term (6 months - 1 year)
### Operational metadata
- Indexing in OLAP store ([Pinot](https://github.com/apache/incubator-pinot)) with TTL
### Microservices as an entity
- Initially focus on rest.li services & GraphQL integration
### AWS & GCP deployment
- Run DataHub in [AWS](https://aws.amazon.com/) & [GCP](https://cloud.google.com/gcp) and provide how-to guides

## Visionary Goals (1 year+)
### Rewrite midtier in Node
- TypeScript-only frontend development
### gRPC + protobuf
- Modeling in protobuf + serving in gRPC
### UI for metadata graph exploration
