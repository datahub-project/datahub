import athenaLogo from '@images/awsathenalogo.png';
import azureLogo from '@images/azure-ad.png';
import bigqueryLogo from '@images/bigquerylogo.png';
import cassandraLogo from '@images/cassandralogo.png';
import clickhouseLogo from '@images/clickhouselogo.png';
import cockroachdbLogo from '@images/cockroachdblogo.png';
import csvLogo from '@images/csv-logo.png';
import databricksLogo from '@images/databrickslogo.png';
import datahubLogo from '@images/datahublogo.png';
import dbtLogo from '@images/dbtlogo.png';
import dremioLogo from '@images/dremiologo.png';
import druidLogo from '@images/druidlogo.png';
import dynamodbLogo from '@images/dynamodblogo.png';
import elasticsearchLogo from '@images/elasticsearchlogo.png';
import feastLogo from '@images/feastlogo.png';
import fivetranLogo from '@images/fivetranlogo.png';
import glueLogo from '@images/gluelogo.png';
import hiveLogo from '@images/hivelogo.png';
import kafkaLogo from '@images/kafkalogo.png';
import lookerLogo from '@images/lookerlogo.svg';
import mariadbLogo from '@images/mariadblogo.png';
import metabaseLogo from '@images/metabaselogo.png';
import mlflowLogo2 from '@images/mlflowlogo2.png';
import modeLogo from '@images/modelogo.png';
import mongodbLogo from '@images/mongodblogo.png';
import mssqlLogo from '@images/mssqllogo.png';
import mysqlLogo from '@images/mysqllogo-2.png';
import neo4j from '@images/neo4j.png';
import oktaLogo from '@images/oktalogo.png';
import oracleLogo from '@images/oraclelogo.png';
import postgresLogo from '@images/postgreslogo.png';
import powerbiLogo from '@images/powerbilogo.png';
import presetLogo from '@images/presetlogo.svg';
import qlikLogo from '@images/qliklogo.png';
import redshiftLogo from '@images/redshiftlogo.png';
import sacLogo from '@images/saclogo.svg';
import sigmaLogo from '@images/sigmalogo.png';
import snowflakeLogo from '@images/snowflakelogo.png';
import supersetLogo from '@images/supersetlogo.png';
import tableauLogo from '@images/tableaulogo.png';
import trinoLogo from '@images/trinologo.png';
import vertexAI from '@images/vertexai.png';
import verticaLogo from '@images/verticalogo.png';

const ATHENA = 'athena';
const ATHENA_URN = `urn:li:dataPlatform:${ATHENA}`;
export const AZURE = 'azure-ad';
const AZURE_URN = `urn:li:dataPlatform:${AZURE}`;
const BIGQUERY = 'bigquery';
const BIGQUERY_BETA = 'bigquery-beta';
const BIGQUERY_URN = `urn:li:dataPlatform:${BIGQUERY}`;
const CLICKHOUSE = 'clickhouse';
const CLICKHOUSE_USAGE = 'clickhouse-usage';
const CLICKHOUSE_URN = `urn:li:dataPlatform:${CLICKHOUSE}`;
const COCKROACHDB = 'cockroachdb';
const COCKROACHDB_URN = `urn:li:dataPlatform:${COCKROACHDB}`;
const DBT = 'dbt';
export const DBT_URN = `urn:li:dataPlatform:${DBT}`;
const DREMIO = 'dremio';
const DREMIO_URN = `urn:li:dataPlatform:${DREMIO}`;
const DRUID = 'druid';
const DRUID_URN = `urn:li:dataPlatform:${DRUID}`;
const DYNAMODB = 'dynamodb';
const DYNAMODB_URN = `urn:li:dataPlatform:${DYNAMODB}`;
const ELASTICSEARCH = 'elasticsearch';
const ELASTICSEARCH_URN = `urn:li:dataPlatform:${ELASTICSEARCH}`;
const FEAST = 'feast';
const FEAST_LEGACY = 'feast-legacy';
const FEAST_URN = `urn:li:dataPlatform:${FEAST}`;
const GLUE = 'glue';
const GLUE_URN = `urn:li:dataPlatform:${GLUE}`;
const HIVE = 'hive';
const HIVE_URN = `urn:li:dataPlatform:${HIVE}`;
const KAFKA = 'kafka';
const KAFKA_URN = `urn:li:dataPlatform:${KAFKA}`;
export const LOOKER = 'looker';
export const LOOK_ML = 'lookml';
const LOOKER_URN = `urn:li:dataPlatform:${LOOKER}`;
const MARIA_DB = 'mariadb';
const MARIA_DB_URN = `urn:li:dataPlatform:${MARIA_DB}`;
const METABASE = 'metabase';
const METABASE_URN = `urn:li:dataPlatform:${METABASE}`;
const MLFLOW = 'mlflow';
const MLFLOW_URN = `urn:li:dataPlatform:${MLFLOW}`;
const MODE = 'mode';
const MODE_URN = `urn:li:dataPlatform:${MODE}`;
const MONGO_DB = 'mongodb';
const MONGO_DB_URN = `urn:li:dataPlatform:${MONGO_DB}`;
const MSSQL = 'mssql';
const MSSQL_URN = `urn:li:dataPlatform:${MSSQL}`;
export const MYSQL = 'mysql';
const MYSQL_URN = `urn:li:dataPlatform:${MYSQL}`;
export const OKTA = 'okta';
const OKTA_URN = `urn:li:dataPlatform:${OKTA}`;
const ORACLE = 'oracle';
const ORACLE_URN = `urn:li:dataPlatform:${ORACLE}`;
const POSTGRES = 'postgres';
const POSTGRES_URN = `urn:li:dataPlatform:${POSTGRES}`;
export const POWER_BI = 'powerbi';
const POWER_BI_URN = `urn:li:dataPlatform:${POWER_BI}`;
const PRESET = 'preset';
const PRESET_URN = `urn:li:dataPlatform:${PRESET}`;
const REDSHIFT = 'redshift';
const REDSHIFT_USAGE = 'redshift-usage';
const REDSHIFT_URN = `urn:li:dataPlatform:${REDSHIFT}`;
const SNOWFLAKE = 'snowflake';
const SNOWFLAKE_BETA = 'snowflake-beta';
const SNOWFLAKE_USAGE = 'snowflake-usage';
const SNOWFLAKE_URN = `urn:li:dataPlatform:${SNOWFLAKE}`;
const STARBURST_TRINO_USAGE = 'starburst-trino-usage';
const SUPERSET = 'superset';
const SUPERSET_URN = `urn:li:dataPlatform:${SUPERSET}`;
const TABLEAU = 'tableau';
const TABLEAU_URN = `urn:li:dataPlatform:${TABLEAU}`;
const TRINO = 'trino';
const TRINO_URN = `urn:li:dataPlatform:${TRINO}`;
export const CUSTOM = 'custom';
const CUSTOM_URN = `urn:li:dataPlatform:${CUSTOM}`;
const UNITY_CATALOG = 'unity-catalog';
const UNITY_CATALOG_URN = `urn:li:dataPlatform:${UNITY_CATALOG}`;
export const DATABRICKS = 'databricks';
const DATABRICKS_URN = `urn:li:dataPlatform:${DATABRICKS}`;
export const DBT_CLOUD = 'dbt-cloud';
const DBT_CLOUD_URN = `urn:li:dataPlatform:dbt`;
export const VERTICA = 'vertica';
const VERTICA_URN = `urn:li:dataPlatform:${VERTICA}`;
const FIVETRAN = 'fivetran';
const FIVETRAN_URN = `urn:li:dataPlatform:${FIVETRAN}`;
export const CSV = 'csv-enricher';
const CSV_URN = `urn:li:dataPlatform:${CSV}`;
const QLIK_SENSE = 'qlik-sense';
const QLIK_SENSE_URN = `urn:li:dataPlatform:${QLIK_SENSE}`;
const SIGMA = 'sigma';
const SIGMA_URN = `urn:li:dataPlatform:${SIGMA}`;
export const SAC = 'sac';
const SAC_URN = `urn:li:dataPlatform:${SAC}`;
const CASSANDRA = 'cassandra';
const CASSANDRA_URN = `urn:li:dataPlatform:${CASSANDRA}`;
const DATAHUB = 'datahub';
const DATAHUB_GC = 'datahub-gc';
const DATAHUB_LINEAGE_FILE = 'datahub-lineage-file';
const DATAHUB_BUSINESS_GLOSSARY = 'datahub-business-glossary';
const DATAHUB_URN = `urn:li:dataPlatform:${DATAHUB}`;
const NEO4J = 'neo4j';
const NEO4J_URN = `urn:li:dataPlatform:${NEO4J}`;
const VERTEX_AI = 'vertexai';
const VERTEXAI_URN = `urn:li:dataPlatform:${VERTEX_AI}`;

export const PLATFORM_URN_TO_LOGO = {
    [ATHENA_URN]: athenaLogo,
    [AZURE_URN]: azureLogo,
    [BIGQUERY_URN]: bigqueryLogo,
    [CLICKHOUSE_URN]: clickhouseLogo,
    [COCKROACHDB_URN]: cockroachdbLogo,
    [DBT_URN]: dbtLogo,
    [DREMIO_URN]: dremioLogo,
    [DRUID_URN]: druidLogo,
    [DYNAMODB_URN]: dynamodbLogo,
    [ELASTICSEARCH_URN]: elasticsearchLogo,
    [FEAST_URN]: feastLogo,
    [GLUE_URN]: glueLogo,
    [HIVE_URN]: hiveLogo,
    [KAFKA_URN]: kafkaLogo,
    [LOOKER_URN]: lookerLogo,
    [MARIA_DB_URN]: mariadbLogo,
    [METABASE_URN]: metabaseLogo,
    [MLFLOW_URN]: mlflowLogo2,
    [MODE_URN]: modeLogo,
    [MONGO_DB_URN]: mongodbLogo,
    [MSSQL_URN]: mssqlLogo,
    [MYSQL_URN]: mysqlLogo,
    [OKTA_URN]: oktaLogo,
    [ORACLE_URN]: oracleLogo,
    [POSTGRES_URN]: postgresLogo,
    [POWER_BI_URN]: powerbiLogo,
    [PRESET_URN]: presetLogo,
    [REDSHIFT_URN]: redshiftLogo,
    [SNOWFLAKE_URN]: snowflakeLogo,
    [TABLEAU_URN]: tableauLogo,
    [TRINO_URN]: trinoLogo,
    [SUPERSET_URN]: supersetLogo,
    [UNITY_CATALOG_URN]: databricksLogo,
    [DATABRICKS_URN]: databricksLogo,
    [VERTICA_URN]: verticaLogo,
    [FIVETRAN_URN]: fivetranLogo,
    [CSV_URN]: csvLogo,
    [QLIK_SENSE_URN]: qlikLogo,
    [SIGMA_URN]: sigmaLogo,
    [SAC_URN]: sacLogo,
    [CASSANDRA_URN]: cassandraLogo,
    [DATAHUB_URN]: datahubLogo,
    [NEO4J_URN]: neo4j,
    [VERTEXAI_URN]: vertexAI,
};

export const SOURCE_TO_PLATFORM_URN = {
    [CLICKHOUSE_USAGE]: CLICKHOUSE_URN,
    [FEAST_LEGACY]: FEAST_URN,
    [LOOK_ML]: LOOKER_URN,
    [REDSHIFT_USAGE]: REDSHIFT_URN,
    [SNOWFLAKE_BETA]: SNOWFLAKE_URN,
    [SNOWFLAKE_USAGE]: SNOWFLAKE_URN,
    [STARBURST_TRINO_USAGE]: TRINO_URN,
    [DBT_CLOUD]: DBT_URN,
    [DATAHUB_GC]: DATAHUB_URN,
    [DATAHUB_LINEAGE_FILE]: DATAHUB_URN,
    [DATAHUB_BUSINESS_GLOSSARY]: DATAHUB_URN,
};
