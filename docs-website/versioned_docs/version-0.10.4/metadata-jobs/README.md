---
title: MXE Processing Jobs
slug: /metadata-jobs
custom_edit_url: "https://github.com/datahub-project/datahub/blob/master/metadata-jobs/README.md"
---

# MXE Processing Jobs

DataHub uses Kafka as the pub-sub message queue in the backend. There are 2 Kafka topics used by DataHub which are
`MetadataChangeEvent` and `MetadataAuditEvent`.

- `MetadataChangeEvent:` This message is emitted by any data platform or crawler in which there is a change in the metadata.
- `MetadataAuditEvent:` This message is emitted by [DataHub GMS](https://github.com/datahub-project/datahub/blob/master/gms) to notify that metadata change is registered.

To be able to consume from these two topics, there are two Spring
jobs DataHub uses:

- [MCE Consumer Job](https://github.com/datahub-project/datahub/blob/master/metadata-jobs/mce-consumer-job): Writes to [DataHub GMS](https://github.com/datahub-project/datahub/blob/master/gms)
- [MAE Consumer Job](https://github.com/datahub-project/datahub/blob/master/metadata-jobs/mae-consumer-job): Writes to [Elasticsearch](https://github.com/datahub-project/datahub/blob/master/docker/elasticsearch) & [Neo4j](https://github.com/datahub-project/datahub/blob/master/docker/neo4j)
