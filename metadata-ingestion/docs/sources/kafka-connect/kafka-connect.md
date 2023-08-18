## Advanced Configurations

### Working with Platform Instances
If you've multiple instances of kafka OR source/sink systems that are referred in your `kafka-connect` setup, you'd need to configure platform instance for these systems in `kafka-connect` recipe to generate correct lineage edges. You must have already set `platform_instance` in recipes of original source/sink systems. Refer the document [Working with Platform Instances](https://datahubproject.io/docs/platform-instances) to understand more about this.

There are two options available to declare source/sink system's `platform_instance` in `kafka-connect` recipe. If single instance of platform is used across all `kafka-connect` connectors, you can use `platform_instance_map` to specify platform_instance to use for a platform when constructing URNs for lineage.

Example:
```yml
    # Map of platform name to platform instance
    platform_instance_map:
      snowflake: snowflake_platform_instance
      mysql: mysql_platform_instance

```
If multiple instances of platform are used across `kafka-connect` connectors, you'd need to specify platform_instance to use for platform for every connector.

#### Example - Multiple MySQL Source Connectors each reading from different mysql instance
```yml
    # Map of platform name to platform instance per connector
    connect_to_platform_map:
      mysql_connector1: 
        mysql: mysql_instance1 

      mysql_connector2:
        mysql: mysql_instance2
```
Here mysql_connector1 and mysql_connector2 are names of MySQL source connectors as defined in `kafka-connect` connector config.

#### Example - Multiple MySQL Source Connectors each reading from difference mysql instance and writing to different kafka cluster
```yml
    connect_to_platform_map:
      mysql_connector1:
        mysql: mysql_instance1
        kafka: kafka_instance1

      mysql_connector2:
        mysql: mysql_instance2
        kafka: kafka_instance2
```
You can also use combination of `platform_instance_map` and `connect_to_platform_map` in your recipe. Note that, the platform_instance specified for the connector in `connect_to_platform_map` will always take higher precedance even if platform_instance for same platform is set in `platform_instance_map`.

If you do not use `platform_instance` in original source/sink recipes, you do not need to specify them in above configurations.

Note that, you do not need to specify platform_instance for BigQuery.

#### Example - Multiple BigQuery Sink Connectors each writing to different kafka cluster
```yml
    connect_to_platform_map:
      bigquery_connector1:
        kafka: kafka_instance1

      bigquery_connector2:
        kafka: kafka_instance2
```

### Provided Configurations from External Sources
Kafka Connect supports pluggable configuration providers which can load configuration data from external sources at runtime. These values are not available to DataHub ingestion source through Kafka Connect APIs. If you are using such provided configurations to specify connection url (database, etc) in Kafka Connect connector configuration then you will need also add these in `provided_configs` section in recipe for DataHub to generate correct lineage.

```yml
    # Optional mapping of provider configurations if using
    provided_configs:
      - provider: env
        path_key: MYSQL_CONNECTION_URL
        value: jdbc:mysql://test_mysql:3306/librarydb
```
