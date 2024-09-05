import React from 'react';
import { FormOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import bigqueryConfig from './bigquery/bigquery';
import redshiftConfig from './redshift/redshift';
import snowflakeConfig from './snowflake/snowflake';
import lookerConfig from './looker/looker';
import mysqlConfig from './mysql/mysql';
import postgresConfig from './postgres/postgres';
import kafkaConfig from './kafka/kafka';
import azureAdConfig from './azure/azure';
import glueConfig from './glue/glue';
import mongoConfig from './mongodb/mongodb';
import oktaConfig from './okta/okta';
import { SourceConfig } from './types';
import hiveConfig from './hive/hive';
import oracleConfig from './oracle/oracle';
import tableauConfig from './tableau/tableau';
import csvConfig from './csv/csv';
import sacConfig from './sac/sac';

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
    bigqueryConfig,
    redshiftConfig,
    snowflakeConfig,
    kafkaConfig,
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
    csvConfig,
    sacConfig,
    {
        type: 'custom',
        placeholderRecipe: DEFAULT_PLACEHOLDER_RECIPE,
        displayName: 'Other',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/',
        logoComponent: <FormOutlined style={{ color: ANTD_GRAY[8], fontSize: 28 }} />,
    },
];
