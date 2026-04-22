import airflowLogo from '@images/airflowlogo.png';
import athenaLogo from '@images/awsathenalogo.png';
import azureLogo from '@images/azure-ad.png';
import azureDataFactoryLogo from '@images/azuredatafactorylogo.svg';
import bigqueryLogo from '@images/bigquerylogo.png';
import cassandraLogo from '@images/cassandralogo.png';
import clickhouseLogo from '@images/clickhouselogo.png';
import cockroachdbLogo from '@images/cockroachdblogo.png';
import confluenceLogo from '@images/confluencelogo.svg';
import csvLogo from '@images/csv-logo.png';
import dagsterLogo from '@images/dagsterlogo.svg';
import databricksLogo from '@images/databrickslogo.png';
import datahubLogo from '@images/datahublogo.png';
import dbtLogo from '@images/dbtlogo.png';
import dremioLogo from '@images/dremiologo.png';
import druidLogo from '@images/druidlogo.png';
import dynamodbLogo from '@images/dynamodblogo.png';
import elasticsearchLogo from '@images/elasticsearchlogo.png';
import fabricDataFactoryLogo from '@images/fabricdatafactorylogo.svg';
import fabricLogo from '@images/fabriclogo.svg';
import fabricOnelakeLogo from '@images/fabriconelakelogo.png';
import feastLogo from '@images/feastlogo.png';
import fivetranLogo from '@images/fivetranlogo.png';
import flinkLogo from '@images/flinklogo.svg';
import glueLogo from '@images/gluelogo.png';
import grafanaLogo from '@images/grafana.png';
import hexLogo from '@images/hex.png';
import hiveLogo from '@images/hivelogo.png';
import icebergLogo from '@images/iceberglogo.png';
import kafkaLogo from '@images/kafkalogo.png';
import lookerLogo from '@images/lookerlogo.svg';
import mariadbLogo from '@images/mariadblogo.svg';
import metabaseLogo from '@images/metabaselogo.png';
import mlflowLogo2 from '@images/mlflowlogo2.png';
import modeLogo from '@images/modelogo.png';
import mongodbLogo from '@images/mongodblogo.png';
import mssqlLogo from '@images/mssqllogo.png';
import mysqlLogo from '@images/mysqllogo-2.png';
import neo4j from '@images/neo4j.svg';
import notionLogo from '@images/notionlogo.png';
import oktaLogo from '@images/oktalogo.png';
import oracleLogo from '@images/oraclelogo.png';
import postgresLogo from '@images/postgreslogo.png';
import powerbiLogo from '@images/powerbilogo.svg';
import presetLogo from '@images/presetlogo.svg';
import prestoLogo from '@images/prestologo.png';
import qlikLogo from '@images/qliklogo.png';
import redshiftLogo from '@images/redshiftlogo.png';
import s3Logo from '@images/s3logo.png';
import sacLogo from '@images/saclogo.svg';
import sageMakerLogo from '@images/sagemakerlogo.png';
import sigmaLogo from '@images/sigmalogo.png';
import snaplogicLogo from '@images/snaplogic.svg';
import snowflakeLogo from '@images/snowflakelogo.png';
import snowplowLogo from '@images/snowplowlogo.png';
import sparkLogo from '@images/sparklogo.png';
import streamlitLogo from '@images/streamlitlogo.png';
import supersetLogo from '@images/supersetlogo.png';
import tableauLogo from '@images/tableaulogo.svg';
import trinoLogo from '@images/trinologo.png';
import vertexAI from '@images/vertexai.png';
import verticaLogo from '@images/verticalogo.png';

const AIRFLOW = 'airflow';
const AIRFLOW_URN = `urn:li:dataPlatform:${AIRFLOW}`;
const ATHENA = 'athena';
const ATHENA_URN = `urn:li:dataPlatform:${ATHENA}`;
export const AZURE = 'azure-ad';
const AZURE_URN = `urn:li:dataPlatform:${AZURE}`;
const AZURE_DATA_FACTORY = 'azure-data-factory';
const AZURE_DATA_FACTORY_URN = `urn:li:dataPlatform:${AZURE_DATA_FACTORY}`;
const BIGQUERY = 'bigquery';
const BIGQUERY_URN = `urn:li:dataPlatform:${BIGQUERY}`;
const CLICKHOUSE = 'clickhouse';
const CLICKHOUSE_USAGE = 'clickhouse-usage';
const CLICKHOUSE_URN = `urn:li:dataPlatform:${CLICKHOUSE}`;
const COCKROACHDB = 'cockroachdb';
const COCKROACHDB_URN = `urn:li:dataPlatform:${COCKROACHDB}`;
const DAGSTER = 'dagster';
const DAGSTER_URN = `urn:li:dataPlatform:${DAGSTER}`;
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
const FLINK = 'flink';
const FLINK_URN = `urn:li:dataPlatform:${FLINK}`;
const GRAFANA = 'grafana';
const GRAFANA_URN = `urn:li:dataPlatform:${GRAFANA}`;
const GLUE = 'glue';
const GLUE_URN = `urn:li:dataPlatform:${GLUE}`;
const HEX = 'hex';
const HEX_URN = `urn:li:dataPlatform:${HEX}`;
const HIVE = 'hive';
const HIVE_URN = `urn:li:dataPlatform:${HIVE}`;
const ICEBERG = 'iceberg';
const ICEBERG_URN = `urn:li:dataPlatform:${ICEBERG}`;
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
export const CONFLUENCE = 'confluence';
const CONFLUENCE_URN = `urn:li:dataPlatform:${CONFLUENCE}`;
export const NOTION = 'notion';
const NOTION_URN = `urn:li:dataPlatform:${NOTION}`;
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
const PRESTO = 'presto';
const PRESTO_URN = `urn:li:dataPlatform:${PRESTO}`;
const REDSHIFT = 'redshift';
const REDSHIFT_USAGE = 'redshift-usage';
const REDSHIFT_URN = `urn:li:dataPlatform:${REDSHIFT}`;
const SNOWFLAKE = 'snowflake';
const SNOWFLAKE_BETA = 'snowflake-beta';
const SNOWFLAKE_USAGE = 'snowflake-usage';
const SNOWFLAKE_URN = `urn:li:dataPlatform:${SNOWFLAKE}`;
const STARBURST_TRINO_USAGE = 'starburst-trino-usage';
const STREAMLIT = 'streamlit';
const STREAMLIT_URN = `urn:li:dataPlatform:${STREAMLIT}`;
const SUPERSET = 'superset';
const SUPERSET_URN = `urn:li:dataPlatform:${SUPERSET}`;
const TABLEAU = 'tableau';
const TABLEAU_URN = `urn:li:dataPlatform:${TABLEAU}`;
const TRINO = 'trino';
const TRINO_URN = `urn:li:dataPlatform:${TRINO}`;
export const CUSTOM = 'custom';
const UNITY_CATALOG = 'unity-catalog';
const UNITY_CATALOG_URN = `urn:li:dataPlatform:${UNITY_CATALOG}`;
export const DATABRICKS = 'databricks';
const DATABRICKS_URN = `urn:li:dataPlatform:${DATABRICKS}`;
export const DBT_CLOUD = 'dbt-cloud';
export const VERTICA = 'vertica';
const VERTICA_URN = `urn:li:dataPlatform:${VERTICA}`;
const FIVETRAN = 'fivetran';
const FIVETRAN_URN = `urn:li:dataPlatform:${FIVETRAN}`;
export const CSV = 'csv-enricher';
const CSV_URN = `urn:li:dataPlatform:${CSV}`;
const SPARK = 'spark';
const SPARK_URN = `urn:li:dataPlatform:${SPARK}`;
const QLIK_SENSE = 'qlik-sense';
const QLIK_SENSE_URN = `urn:li:dataPlatform:${QLIK_SENSE}`;
const S3 = 's3';
const S3_URN = `urn:li:dataPlatform:${S3}`;
const SAGE_MAKER = 'sagemaker';
const SAGE_MAKER_URN = `urn:li:dataPlatform:${SAGE_MAKER}`;
const SIGMA = 'sigma';
const SIGMA_URN = `urn:li:dataPlatform:${SIGMA}`;
export const SAC = 'sac';
export const SAC_URN = `urn:li:dataPlatform:${SAC}`;
export const CASSANDRA = 'cassandra';
export const CASSANDRA_URN = `urn:li:dataPlatform:${CASSANDRA}`;
export const DATAHUB = 'datahub';
export const DATAHUB_GC = 'datahub-gc';
export const DATAHUB_LINEAGE_FILE = 'datahub-lineage-file';
export const DATAHUB_BUSINESS_GLOSSARY = 'datahub-business-glossary';
export const DATAHUB_URN = `urn:li:dataPlatform:${DATAHUB}`;
export const NEO4J = 'neo4j';
export const NEO4J_URN = `urn:li:dataPlatform:${NEO4J}`;
export const VERTEX_AI = 'vertexai';
export const VERTEXAI_URN = `urn:li:dataPlatform:${VERTEX_AI}`;
export const SNAPLOGIC = 'snaplogic';
export const SNAPLOGIC_URN = `urn:li:dataPlatform:${SNAPLOGIC}`;
export const SNOWPLOW = 'snowplow';
export const SNOWPLOW_URN = `urn:li:dataPlatform:${SNOWPLOW}`;
export const FABRIC = 'fabric';
export const FABRIC_URN = `urn:li:dataPlatform:${FABRIC}`;
export const FABRIC_DATA_FACTORY = 'fabric-data-factory';
export const FABRIC_DATA_FACTORY_URN = `urn:li:dataPlatform:${FABRIC_DATA_FACTORY}`;
export const FABRIC_ONELAKE = 'fabric-onelake';
export const FABRIC_ONELAKE_URN = `urn:li:dataPlatform:${FABRIC_ONELAKE}`;
export const RDF = 'rdf';

export const PLATFORM_URN_TO_LOGO = {
    [AIRFLOW_URN]: airflowLogo,
    [ATHENA_URN]: athenaLogo,
    [AZURE_URN]: azureLogo,
    [AZURE_DATA_FACTORY_URN]: azureDataFactoryLogo,
    [BIGQUERY_URN]: bigqueryLogo,
    [CLICKHOUSE_URN]: clickhouseLogo,
    [COCKROACHDB_URN]: cockroachdbLogo,
    [DAGSTER_URN]: dagsterLogo,
    [DBT_URN]: dbtLogo,
    [DREMIO_URN]: dremioLogo,
    [DRUID_URN]: druidLogo,
    [DYNAMODB_URN]: dynamodbLogo,
    [ELASTICSEARCH_URN]: elasticsearchLogo,
    [FEAST_URN]: feastLogo,
    [FLINK_URN]: flinkLogo,
    [GRAFANA_URN]: grafanaLogo,
    [GLUE_URN]: glueLogo,
    [HEX_URN]: hexLogo,
    [HIVE_URN]: hiveLogo,
    [ICEBERG_URN]: icebergLogo,
    [KAFKA_URN]: kafkaLogo,
    [LOOKER_URN]: lookerLogo,
    [MARIA_DB_URN]: mariadbLogo,
    [METABASE_URN]: metabaseLogo,
    [MLFLOW_URN]: mlflowLogo2,
    [MODE_URN]: modeLogo,
    [MONGO_DB_URN]: mongodbLogo,
    [MSSQL_URN]: mssqlLogo,
    [MYSQL_URN]: mysqlLogo,
    [CONFLUENCE_URN]: confluenceLogo,
    [NOTION_URN]: notionLogo,
    [OKTA_URN]: oktaLogo,
    [ORACLE_URN]: oracleLogo,
    [POSTGRES_URN]: postgresLogo,
    [POWER_BI_URN]: powerbiLogo,
    [PRESET_URN]: presetLogo,
    [PRESTO_URN]: prestoLogo,
    [REDSHIFT_URN]: redshiftLogo,
    [S3_URN]: s3Logo,
    [SAGE_MAKER_URN]: sageMakerLogo,
    [SNOWFLAKE_URN]: snowflakeLogo,
    [SNOWPLOW_URN]: snowplowLogo,
    [STREAMLIT_URN]: streamlitLogo,
    [SPARK_URN]: sparkLogo,
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
    [SNAPLOGIC_URN]: snaplogicLogo,
    [FABRIC_URN]: fabricLogo,
    [FABRIC_DATA_FACTORY_URN]: fabricDataFactoryLogo,
    [FABRIC_ONELAKE_URN]: fabricOnelakeLogo,
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
