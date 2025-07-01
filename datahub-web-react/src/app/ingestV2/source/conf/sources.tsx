import { FormOutlined } from '@ant-design/icons';
import React from 'react';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import azureAdConfig from '@app/ingestV2/source/conf/azure/azure';
import bigqueryConfig from '@app/ingestV2/source/conf/bigquery/bigquery';
import csvConfig from '@app/ingestV2/source/conf/csv/csv';
import glueConfig from '@app/ingestV2/source/conf/glue/glue';
import hiveConfig from '@app/ingestV2/source/conf/hive/hive';
import kafkaConfig from '@app/ingestV2/source/conf/kafka/kafka';
import lookerConfig from '@app/ingestV2/source/conf/looker/looker';
import mongoConfig from '@app/ingestV2/source/conf/mongodb/mongodb';
import mysqlConfig from '@app/ingestV2/source/conf/mysql/mysql';
import oktaConfig from '@app/ingestV2/source/conf/okta/okta';
import oracleConfig from '@app/ingestV2/source/conf/oracle/oracle';
import postgresConfig from '@app/ingestV2/source/conf/postgres/postgres';
import redshiftConfig from '@app/ingestV2/source/conf/redshift/redshift';
import sacConfig from '@app/ingestV2/source/conf/sac/sac';
import snowflakeConfig from '@app/ingestV2/source/conf/snowflake/snowflake';
import tableauConfig from '@app/ingestV2/source/conf/tableau/tableau';
import { SourceConfig } from '@app/ingestV2/source/conf/types';

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
        docsUrl: 'https://docs.datahub.com/docs/metadata-ingestion/',
        logoComponent: <FormOutlined style={{ color: ANTD_GRAY[8], fontSize: 28 }} />,
    },
];
