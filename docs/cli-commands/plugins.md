# Installing plugins

We use a plugin architecture so that you can install only the dependencies you actually need. Click the plugin name to learn more about the specific source recipe and any FAQs!

## Sources

Please see our [Integrations page](https://docs.datahub.com/integrations) if you want to filter on the features offered by each source.

| Plugin Name                                                                                     | Install Command                                            | Provides                                |
| ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | --------------------------------------- |
| [metadata-file](../generated/ingestion/sources/metadata-file.md)                                | _included by default_                                      | File source and sink                    |
| [athena](../generated/ingestion/sources/athena.md)                                              | `pip install 'acryl-datahub[athena]'`                      | AWS Athena source                       |
| [bigquery](../generated/ingestion/sources/bigquery.md)                                          | `pip install 'acryl-datahub[bigquery]'`                    | BigQuery source                         |
| [csv-enricher](../generated/ingestion/sources/csv-enricher.md)                                  | _no additional dependencies_                               | CSV Enricher source                     |
| [datahub-lineage-file](../generated/ingestion/sources/file-based-lineage.md)                    | _no additional dependencies_                               | Lineage File source                     |
| [datahub-business-glossary](../generated/ingestion/sources/business-glossary.md)                | _no additional dependencies_                               | Business Glossary File source           |
| [dbt](../generated/ingestion/sources/dbt.md)                                                    | `pip install 'acryl-datahub[dbt]'`                         | dbt source                              |
| [dremio](../generated/ingestion/sources/dremio.md)                                              | `pip install 'acryl-datahub[dremio]'`                      | Dremio Source                           |
| [druid](../generated/ingestion/sources/druid.md)                                                | `pip install 'acryl-datahub[druid]'`                       | Druid Source                            |
| [feast](../generated/ingestion/sources/feast.md)                                                | `pip install 'acryl-datahub[feast]'`                       | Feast source (0.26.0)                   |
| [glue](../generated/ingestion/sources/glue.md)                                                  | `pip install 'acryl-datahub[glue]'`                        | AWS Glue source                         |
| [hana](../generated/ingestion/sources/hana.md)                                                  | `pip install 'acryl-datahub[hana]'`                        | SAP HANA source                         |
| [hive](../generated/ingestion/sources/hive.md)                                                  | `pip install 'acryl-datahub[hive]'`                        | Hive source                             |
| [kafka](../generated/ingestion/sources/kafka.md)                                                | `pip install 'acryl-datahub[kafka]'`                       | Kafka source                            |
| [kafka-connect](../generated/ingestion/sources/kafka-connect.md)                                | `pip install 'acryl-datahub[kafka-connect]'`               | Kafka connect source                    |
| [ldap](../generated/ingestion/sources/ldap.md)                                                  | `pip install 'acryl-datahub[ldap]'` ([extra requirements]) | LDAP source                             |
| [looker](../generated/ingestion/sources/looker.md)                                              | `pip install 'acryl-datahub[looker]'`                      | Looker source                           |
| [lookml](../generated/ingestion/sources/looker.md#module-lookml)                                | `pip install 'acryl-datahub[lookml]'`                      | LookML source                           |
| [metabase](../generated/ingestion/sources/metabase.md)                                          | `pip install 'acryl-datahub[metabase]'`                    | Metabase source                         |
| [mode](../generated/ingestion/sources/mode.md)                                                  | `pip install 'acryl-datahub[mode]'`                        | Mode Analytics source                   |
| [mongodb](../generated/ingestion/sources/mongodb.md)                                            | `pip install 'acryl-datahub[mongodb]'`                     | MongoDB source                          |
| [mssql](../generated/ingestion/sources/mssql.md)                                                | `pip install 'acryl-datahub[mssql]'`                       | SQL Server source                       |
| [mysql](../generated/ingestion/sources/mysql.md)                                                | `pip install 'acryl-datahub[mysql]'`                       | MySQL source                            |
| [mariadb](../generated/ingestion/sources/mariadb.md)                                            | `pip install 'acryl-datahub[mariadb]'`                     | MariaDB source                          |
| [openapi](../generated/ingestion/sources/openapi.md)                                            | `pip install 'acryl-datahub[openapi]'`                     | OpenApi Source                          |
| [oracle](../generated/ingestion/sources/oracle.md)                                              | `pip install 'acryl-datahub[oracle]'`                      | Oracle source                           |
| [postgres](../generated/ingestion/sources/postgres.md)                                          | `pip install 'acryl-datahub[postgres]'`                    | Postgres source                         |
| [redash](../generated/ingestion/sources/redash.md)                                              | `pip install 'acryl-datahub[redash]'`                      | Redash source                           |
| [redshift](../generated/ingestion/sources/redshift.md)                                          | `pip install 'acryl-datahub[redshift]'`                    | Redshift source                         |
| [sagemaker](../generated/ingestion/sources/sagemaker.md)                                        | `pip install 'acryl-datahub[sagemaker]'`                   | AWS SageMaker source                    |
| [salesforce](../generated/ingestion/sources/salesforce.md)                                      | `pip install 'acryl-datahub[salesforce]'`                  | Salesforce source                       |
| [snowflake](../generated/ingestion/sources/snowflake.md)                                        | `pip install 'acryl-datahub[snowflake]'`                   | Snowflake source                        |
| [sqlalchemy](../generated/ingestion/sources/sqlalchemy.md)                                      | `pip install 'acryl-datahub[sqlalchemy]'`                  | Generic SQLAlchemy source               |
| [superset](../generated/ingestion/sources/superset.md)                                          | `pip install 'acryl-datahub[superset]'`                    | Superset source                         |
| [tableau](../generated/ingestion/sources/tableau.md)                                            | `pip install 'acryl-datahub[tableau]'`                     | Tableau source                          |
| [trino](../generated/ingestion/sources/trino.md)                                                | `pip install 'acryl-datahub[trino]'`                       | Trino source                            |
| [starburst-trino-usage](../generated/ingestion/sources/trino.md)                                | `pip install 'acryl-datahub[starburst-trino-usage]'`       | Starburst Trino usage statistics source |
| [nifi](../generated/ingestion/sources/nifi.md)                                                  | `pip install 'acryl-datahub[nifi]'`                        | NiFi source                             |
| [powerbi](../generated/ingestion/sources/powerbi.md#module-powerbi)                             | `pip install 'acryl-datahub[powerbi]'`                     | Microsoft Power BI source               |
| [powerbi-report-server](../generated/ingestion/sources/powerbi.md#module-powerbi-report-server) | `pip install 'acryl-datahub[powerbi-report-server]'`       | Microsoft Power BI Report Server source |

## Debug/Utility Plugins

| Plugin Name     | Install Command                                | Provides                                              |
| --------------- | ---------------------------------------------- | ----------------------------------------------------- |
| debug-recording | `pip install 'acryl-datahub[debug-recording]'` | Record and replay ingestion runs for debugging (Beta) |

## Sinks

| Plugin Name                                                          | Install Command                              | Provides                   |
| -------------------------------------------------------------------- | -------------------------------------------- | -------------------------- |
| [metadata-file](../../metadata-ingestion/sink_docs/metadata-file.md) | _included by default_                        | File source and sink       |
| [console](../../metadata-ingestion/sink_docs/console.md)             | _included by default_                        | Console sink               |
| [datahub-rest](../../metadata-ingestion/sink_docs/datahub.md)        | `pip install 'acryl-datahub[datahub-rest]'`  | DataHub sink over REST API |
| [datahub-kafka](../../metadata-ingestion/sink_docs/datahub.md)       | `pip install 'acryl-datahub[datahub-kafka]'` | DataHub sink over Kafka    |

These plugins can be mixed and matched as desired. For example:

```shell
pip install 'acryl-datahub[bigquery,datahub-rest]'
```

## Check the active plugins

```shell
datahub check plugins
```

[extra requirements]: https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites
