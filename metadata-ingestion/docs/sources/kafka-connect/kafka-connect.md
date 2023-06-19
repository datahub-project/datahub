## Advanced Configurations

Kafka Connect supports pluggable configuration providers which can load configuration data from external sources at runtime. These values are not available to DataHub ingestion source through Kafka Connect APIs. If you are using such provided configurations to specify connection url (database, etc) in Kafka Connect connector configuration then you will need also add these in `provided_configs` section in recipe for DataHub to generate correct lineage.

```yml
    # Optional mapping of provider configurations if using
    provided_configs:
      - provider: env
        path_key: MYSQL_CONNECTION_URL
        value: jdbc:mysql://test_mysql:3306/librarydb
```
