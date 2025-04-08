import { FormOutlined } from '@ant-design/icons';
import React from 'react';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import azureAdConfig from '@app/ingest/source/conf/azure/azure';
import bigqueryConfig from '@app/ingest/source/conf/bigquery/bigquery';
import csvConfig from '@app/ingest/source/conf/csv/csv';
import glueConfig from '@app/ingest/source/conf/glue/glue';
import hiveConfig from '@app/ingest/source/conf/hive/hive';
import kafkaConfig from '@app/ingest/source/conf/kafka/kafka';
import lookerConfig from '@app/ingest/source/conf/looker/looker';
import mongoConfig from '@app/ingest/source/conf/mongodb/mongodb';
import mysqlConfig from '@app/ingest/source/conf/mysql/mysql';
import oktaConfig from '@app/ingest/source/conf/okta/okta';
import oracleConfig from '@app/ingest/source/conf/oracle/oracle';
import postgresConfig from '@app/ingest/source/conf/postgres/postgres';
import redshiftConfig from '@app/ingest/source/conf/redshift/redshift';
import sacConfig from '@app/ingest/source/conf/sac/sac';
import snowflakeConfig from '@app/ingest/source/conf/snowflake/snowflake';
import tableauConfig from '@app/ingest/source/conf/tableau/tableau';
import { SourceConfig } from '@app/ingest/source/conf/types';

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
