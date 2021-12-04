import React from 'react';
import YAML from 'yamljs';
import { FormOutlined } from '@ant-design/icons';
import mysqlLogo from '../../../images/mysqllogo-2.png';
import redshiftLogo from '../../../images/redshiftlogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import lookerLogo from '../../../images/lookerlogo.png';
import postgresLogo from '../../../images/postgreslogo.png';
import { ANTD_GRAY } from '../../entity/shared/constants';

const baseUrl = window.location.origin;

// todo: auto generate this.
export const SOURCE_TEMPLATE_CONFIGS = [
    {
        type: 'mysql',
        recipe: `source: 
    type: mysql
    config: 
        # Coordinates
        host_port: <mysql-host>
        database: <mysql-database>
    
        # Credentials
        username: <mysql-username>
        password: <mysql-password>
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        logoUrl: mysqlLogo,
        displayName: 'MySQL',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/mysql/',
    },
    {
        type: 'bigquery',
        recipe: `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        displayName: 'BigQuery',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/bigquery/',
        logoUrl: bigqueryLogo,
    },
    {
        type: 'redshift',
        recipe: `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        displayName: 'Redshift',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/redshift/',
        logoUrl: redshiftLogo,
    },
    {
        type: 'snowflake',
        recipe: `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        displayName: 'Snowflake',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/snowflake/',
        logoUrl: snowflakeLogo,
    },
    {
        type: 'looker',
        recipe: `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        displayName: 'Looker',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/looker/',
        logoUrl: lookerLogo,
    },
    {
        type: 'postgres',
        recipe: `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        displayName: 'Postgres',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/postgres/',
        logoUrl: postgresLogo,
    },
    {
        type: 'custom',
        recipe: `source: 
    # Source type, e.g. 'bigquery' 
    type: <source-type> 
    # Source-specific configs 
    config: 
        <source-configs> 
sink: 
    type: "datahub-rest" 
    config: 
        server: "${baseUrl}/api/gms"`,
        displayName: 'Custom',
        docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/',
        logoComponent: <FormOutlined style={{ color: ANTD_GRAY[8], fontSize: 28 }} />,
    },
];

export const sourceTypeToIconUrl = (type: string) => {
    return SOURCE_TEMPLATE_CONFIGS.find((config) => config.type === type)?.logoUrl;
};

export const getSourceConfigs = (sourceType: string) => {
    const sourceConfigs = SOURCE_TEMPLATE_CONFIGS.find((configs) => configs.type === sourceType);
    if (!sourceConfigs) {
        throw new Error(`Failed to find source configs with source type ${sourceType}`);
    }
    return sourceConfigs;
};

export const yamlToJson = (yaml: string): string => {
    const obj = YAML.parse(yaml);
    const jsonStr = JSON.stringify(obj);
    return jsonStr;
};

export const jsonToYaml = (json: string): string => {
    const obj = JSON.parse(json);
    const yamlStr = YAML.stringify(obj, 6);
    return yamlStr;
};

export enum IntervalType {
    DAILY,
    WEEKLY,
    MONTHLY,
}
