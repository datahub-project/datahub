# How to onboard a new data source?

In the [metadata-ingestion](https://github.com/linkedin/datahub/tree/master/metadata-ingestion), DataHub provides various kinds of metadata sources onboarding, including [Hive](https://github.com/linkedin/datahub/tree/master/metadata-ingestion/hive-etl), [Kafka](https://github.com/linkedin/datahub/tree/master/metadata-ingestion/kafka-etl), [LDAP](https://github.com/linkedin/datahub/tree/master/metadata-ingestion/ldap-etl), [mySQL](https://github.com/linkedin/datahub/tree/master/metadata-ingestion/mysql-etl), and generic [RDBMS](https://github.com/linkedin/datahub/tree/master/metadata-ingestion/rdbms-etl) as ETL scripts to feed the metadata to the [GMS](../what/gms.md).

## 1. Extract
The extract process will be specific tight to the data source, hence, the [data accessor](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/ldap-etl/ldap_etl.py#L103) should be able to reflect the correctness of the metadata from underlying data platforms.

## 2. Transform
In the transform stage, the extracted metadata should be [encapsulated in a valid MetadataChangeEvent](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/ldap-etl/ldap_etl.py#L56) under the defined aspects and snapshots. 

## 3. Load
The load part will leverage the [Kafka producer](https://github.com/linkedin/datahub/blob/master/metadata-ingestion/ldap-etl/ldap_etl.py#L80) to enable the pub-sub event-based ingestion. Meanwhile, the schema validation will be involved to check metadata quality.
