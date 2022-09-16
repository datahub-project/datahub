import { FormOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import arangoDBConfig from './arangodb/arangodb';
import azureAdConfig from './azure/azure';
import bigqueryConfig from './bigquery/bigquery';
import glueConfig from './glue/glue';
import hdfsConfig from './hdfs/hdfs';
import hiveConfig from './hive/hive';
import kafkaConfig from './kafka/kafka';
import ldapConfig from './ldap/ldap';
import lookerConfig from './looker/looker';
import mongoConfig from './mongodb/mongodb';
import mysqlConfig from './mysql/mysql';
import oktaConfig from './okta/okta';
import oracleConfig from './oracle/oracle';
import postgresConfig from './postgres/postgres';
import redshiftConfig from './redshift/redshift';
import snowflakeConfig from './snowflake/snowflake';
import tableauConfig from './tableau/tableau';
import tidbConfig from './tidb/tidb';
import { SourceConfig } from './types';

const baseUrl = window.location.origin;

const DEFAULT_PLACEHOLDER_RECIPE = `\
source:
  type: <source-type>
  config:
    # Source-type specifics config
    <source-configs> 

sink:
  type: datahub-rest
  config:
    server: "${baseUrl}/api/gms"`;

export const SOURCE_TEMPLATE_CONFIGS: Array<SourceConfig> = [    
    hdfsConfig,
    arangoDBConfig,
    hiveConfig,
    tidbConfig,
    ldapConfig,
    bigqueryConfig,
    redshiftConfig,
    snowflakeConfig,
    kafkaConfig,
    ldapConfig,
    lookerConfig,
    tableauConfig,
    mysqlConfig,
    postgresConfig,
    mongoConfig,
    azureAdConfig,
    oktaConfig,
    glueConfig,
    oracleConfig,
    hiveConfig,
    {
        type: 'custom',
        placeholderRecipe: DEFAULT_PLACEHOLDER_RECIPE,
        displayName: 'Custom',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/',
        logoComponent: <FormOutlined style={{ color: ANTD_GRAY[8], fontSize: 28 }} />,
    },
];
