import snowflakeLogo from '../../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../../images/bigquerylogo.png';
import redshiftLogo from '../../../../images/redshiftlogo.png';
import kafkaLogo from '../../../../images/kafkalogo.png';
import lookerLogo from '../../../../images/lookerlogo.png';
import tableauLogo from '../../../../images/tableaulogo.png';
import mysqlLogo from '../../../../images/mysqllogo-2.png';
import postgresLogo from '../../../../images/postgreslogo.png';
import mongodbLogo from '../../../../images/mongodblogo.png';
import azureLogo from '../../../../images/azure-ad.png';
import oktaLogo from '../../../../images/oktalogo.png';
import glueLogo from '../../../../images/gluelogo.png';
import oracleLogo from '../../../../images/oraclelogo.png';
import hiveLogo from '../../../../images/hivelogo.png';
import supersetLogo from '../../../../images/supersetlogo.png';
import athenaLogo from '../../../../images/awsathenalogo.png';
import mssqlLogo from '../../../../images/mssqllogo.png';
import clickhouseLogo from '../../../../images/clickhouselogo.png';
import trinoLogo from '../../../../images/trinologo.png';
import dbtLogo from '../../../../images/dbtlogo.png';
import druidLogo from '../../../../images/druidlogo.png';
import elasticsearchLogo from '../../../../images/elasticsearchlogo.png';
import feastLogo from '../../../../images/feastlogo.png';
import mariadbLogo from '../../../../images/mariadblogo.png';
import metabaseLogo from '../../../../images/metabaselogo.png';

export const SNOWFLAKE_URN = 'urn:li:dataPlatform:snowflake';
export const SNOWFLAKE = 'snowflake';
export const BIGQUERY_URN = 'urn:li:dataPlatform:bigquery';
export const BIGQUERY = 'bigquery';
export const REDSHIFT_URN = 'urn:li:dataPlatform:redshift';
export const REDSHIFT = 'redshift';
export const KAFKA_URN = 'urn:li:dataPlatform:kafka';
export const KAFKA = 'kafka';
export const LOOKER_URN = 'urn:li:dataPlatform:looker';
export const LOOKER = 'looker';
export const TABLEAU_URN = 'urn:li:dataPlatform:tableau';
export const TABLEAU = 'tableau';
export const MYSQL_URN = 'urn:li:dataPlatform:mysql';
export const MYSQL = 'mysql';
export const POSTGRES_URN = 'urn:li:dataPlatform:postgres';
export const POSTGRES = 'postgres';
export const MONGO_DB_URN = 'urn:li:dataPlatform:mongodb';
export const MONGO_DB = 'mongodb';
export const AZURE_URN = 'urn:li:dataPlatform:azure-ad';
export const AZURE = 'azure-ad';
export const OKTA_URN = 'urn:li:dataPlatform:okta';
export const OKTA = 'okta';
export const GLUE_URN = 'urn:li:dataPlatform:glue';
export const GLUE = 'glue';
export const ORACLE_URN = 'urn:li:dataPlatform:oracle';
export const ORACLE = 'oracle';
export const HIVE_URN = 'urn:li:dataPlatform:hive';
export const HIVE = 'hive';
export const SUPERSET_URN = 'urn:li:dataPlatform:superset';
export const SUPERSET = 'superset';
export const ATHENA_URN = 'urn:li:dataPlatform:athena';
export const ATHENA = 'athena';
export const MSSQL_URN = 'urn:li:dataPlatform:mssql';
export const MSSQL = 'mssql';
export const CLICKHOUSE_URN = 'urn:li:dataPlatform:clickhouse';
export const CLICKHOUSE = 'clickhouse';
export const TRINO_URN = 'urn:li:dataPlatform:trino';
export const TRINO = 'trino';
export const DBT_URN = 'urn:li:dataPlatform:dbt';
export const DBT = 'dbt';
export const DRUID_URN = 'urn:li:dataPlatform:druid';
export const DRUID = 'druid';
export const ELASTICSEARCH_URN = 'urn:li:dataPlatform:elasticsearch';
export const ELASTICSEARCH = 'elasticsearch';
export const FEAST_URN = 'urn:li:dataPlatform:feast';
export const FEAST = 'feast';
export const MARIA_DB_URN = 'urn:li:dataPlatform:mariadb';
export const MARIA_DB = 'mariadb';
export const METABASE_URN = 'urn:li:dataPlatform:metabase';
export const METABASE = 'metabase';
export const CUSTOM_URN = 'urn:li:dataPlatform:custom';
export const CUSTOM = 'custom';

export const SOURCE_URN_TO_LOGO = {
    [SNOWFLAKE_URN]: snowflakeLogo,
    [BIGQUERY_URN]: bigqueryLogo,
    [REDSHIFT_URN]: redshiftLogo,
    [KAFKA_URN]: kafkaLogo,
    [LOOKER_URN]: lookerLogo,
    [TABLEAU_URN]: tableauLogo,
    [MYSQL_URN]: mysqlLogo,
    [POSTGRES_URN]: postgresLogo,
    [MONGO_DB_URN]: mongodbLogo,
    [AZURE_URN]: azureLogo,
    [OKTA_URN]: oktaLogo,
    [GLUE_URN]: glueLogo,
    [ORACLE_URN]: oracleLogo,
    [HIVE_URN]: hiveLogo,
    [SUPERSET_URN]: supersetLogo,
    [ATHENA_URN]: athenaLogo,
    [MSSQL_URN]: mssqlLogo,
    [CLICKHOUSE_URN]: clickhouseLogo,
    [TRINO_URN]: trinoLogo,
    [DBT_URN]: dbtLogo,
    [DRUID_URN]: druidLogo,
    [ELASTICSEARCH_URN]: elasticsearchLogo,
    [FEAST_URN]: feastLogo,
    [MARIA_DB_URN]: mariadbLogo,
    [METABASE_URN]: metabaseLogo,
};

export const SOURCE_TO_SOURCE_URN = {
    [SNOWFLAKE]: SNOWFLAKE_URN,
    [BIGQUERY]: BIGQUERY_URN,
    [REDSHIFT]: REDSHIFT_URN,
    [KAFKA]: KAFKA_URN,
    [LOOKER]: LOOKER_URN,
    [TABLEAU]: TABLEAU_URN,
    [MYSQL]: MYSQL_URN,
    [POSTGRES]: POSTGRES_URN,
    [MONGO_DB]: MONGO_DB_URN,
    [AZURE]: AZURE_URN,
    [OKTA]: OKTA_URN,
    [GLUE]: GLUE_URN,
    [ORACLE]: ORACLE_URN,
    [HIVE]: HIVE_URN,
    [SUPERSET]: SUPERSET_URN,
    [ATHENA]: ATHENA_URN,
    [MSSQL]: MSSQL_URN,
    [CLICKHOUSE]: CLICKHOUSE_URN,
    [TRINO]: TRINO_URN,
    [DBT]: DBT_URN,
    [DRUID]: DRUID_URN,
    [ELASTICSEARCH]: ELASTICSEARCH_URN,
    [FEAST]: FEAST_URN,
    [MARIA_DB]: MARIA_DB_URN,
    [METABASE]: METABASE_URN,
    [CUSTOM]: CUSTOM_URN,
};
