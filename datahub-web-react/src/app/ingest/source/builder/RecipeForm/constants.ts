import { SNOWFLAKE } from '../../conf/snowflake/snowflake';
import { BIGQUERY } from '../../conf/bigquery/bigquery';
import { REDSHIFT } from '../../conf/redshift/redshift';
import { LOOKER } from '../../conf/looker/looker';
import { TABLEAU } from '../../conf/tableau/tableau';
import { KAFKA } from '../../conf/kafka/kafka';
import {
    INCLUDE_LINEAGE,
    PROFILING_ENABLED,
    STATEFUL_INGESTION_ENABLED,
    DATABASE_ALLOW,
    DATABASE_DENY,
    UPSTREAM_LINEAGE_IN_REPORT,
    TABLE_LINEAGE_MODE,
    INGEST_TAGS,
    INGEST_OWNER,
    DASHBOARD_ALLOW,
    DASHBOARD_DENY,
    GITHUB_INFO_REPO,
    EXTRACT_USAGE_HISTORY,
    EXTRACT_OWNERS,
    SKIP_PERSONAL_FOLDERS,
    RecipeField,
} from './common';
import {
    SNOWFLAKE_ACCOUNT_ID,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_USERNAME,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA_ALLOW,
    SNOWFLAKE_SCHEMA_DENY,
    SNOWFLAKE_TABLE_ALLOW,
    SNOWFLAKE_TABLE_DENY,
    SNOWFLAKE_VIEW_ALLOW,
    SNOWFLAKE_VIEW_DENY,
} from './snowflake';
import {
    BIGQUERY_PROJECT_ID,
    BIGQUERY_CREDENTIAL_PROJECT_ID,
    BIGQUERY_PRIVATE_KEY,
    BIGQUERY_PRIVATE_KEY_ID,
    BIGQUERY_CLIENT_EMAIL,
    BIGQUERY_CLIENT_ID,
    BIGQUERY_SCHEMA_ALLOW,
    BIGQUERY_SCHEMA_DENY,
    BIGQUERY_TABLE_ALLOW,
    BIGQUERY_TABLE_DENY,
    BIGQUERY_VIEW_ALLOW,
    BIGQUERY_VIEW_DENY,
} from './bigquery';
import {
    REDSHIFT_HOST_PORT,
    REDSHIFT_DATABASE,
    REDSHIFT_USERNAME,
    REDSHIFT_PASSWORD,
    REDSHIFT_SCHEMA_ALLOW,
    REDSHIFT_SCHEMA_DENY,
    REDSHIFT_TABLE_ALLOW,
    REDSHIFT_TABLE_DENY,
    REDSHIFT_VIEW_ALLOW,
    REDSHIFT_VIEW_DENY,
} from './redshift';
import { TABLEAU_CONNECTION_URI, TABLEAU_PROJECT, TABLEAU_SITE, TABLEAU_USERNAME, TABLEAU_PASSWORD } from './tableau';
import { CHART_ALLOW, CHART_DENY, LOOKER_BASE_URL, LOOKER_CLIENT_ID, LOOKER_CLIENT_SECRET } from './looker';
import {
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_BOOTSTRAP,
    KAFKA_SCHEMA_REGISTRY_URL,
    KAFKA_SCHEMA_REGISTRY_USER_CREDENTIAL,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    TOPIC_ALLOW,
    TOPIC_DENY,
} from './kafka';
import { POSTGRES } from '../../conf/postgres/postgres';
import { POSTGRES_HOST_PORT, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD } from './postgres';
import { HIVE } from '../../conf/hive/hive';
import { HIVE_HOST_PORT, HIVE_DATABASE, HIVE_USERNAME, HIVE_PASSWORD } from './hive';

interface RecipeFields {
    [key: string]: {
        fields: RecipeField[];
        filterFields: RecipeField[];
        advancedFields: RecipeField[];
        connectionSectionTooltip?: string;
        filterSectionTooltip?: string;
        advancedSectionTooltip?: string;
    };
}

export const RECIPE_FIELDS: RecipeFields = {
    [SNOWFLAKE]: {
        fields: [SNOWFLAKE_ACCOUNT_ID, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE],
        advancedFields: [INCLUDE_LINEAGE, PROFILING_ENABLED, STATEFUL_INGESTION_ENABLED],
        filterFields: [
            DATABASE_ALLOW,
            DATABASE_DENY,
            SNOWFLAKE_SCHEMA_ALLOW,
            SNOWFLAKE_SCHEMA_DENY,
            SNOWFLAKE_TABLE_ALLOW,
            SNOWFLAKE_TABLE_DENY,
            SNOWFLAKE_VIEW_ALLOW,
            SNOWFLAKE_VIEW_DENY,
        ],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
    [BIGQUERY]: {
        fields: [
            BIGQUERY_PROJECT_ID,
            BIGQUERY_CREDENTIAL_PROJECT_ID,
            BIGQUERY_PRIVATE_KEY,
            BIGQUERY_PRIVATE_KEY_ID,
            BIGQUERY_CLIENT_EMAIL,
            BIGQUERY_CLIENT_ID,
        ],
        advancedFields: [INCLUDE_LINEAGE, PROFILING_ENABLED, STATEFUL_INGESTION_ENABLED, UPSTREAM_LINEAGE_IN_REPORT],
        filterFields: [
            BIGQUERY_SCHEMA_ALLOW,
            BIGQUERY_SCHEMA_DENY,
            BIGQUERY_TABLE_ALLOW,
            BIGQUERY_TABLE_DENY,
            BIGQUERY_VIEW_ALLOW,
            BIGQUERY_VIEW_DENY,
        ],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
    [REDSHIFT]: {
        fields: [REDSHIFT_HOST_PORT, REDSHIFT_DATABASE, REDSHIFT_USERNAME, REDSHIFT_PASSWORD],
        advancedFields: [INCLUDE_LINEAGE, PROFILING_ENABLED, STATEFUL_INGESTION_ENABLED, TABLE_LINEAGE_MODE],
        filterFields: [
            REDSHIFT_SCHEMA_ALLOW,
            REDSHIFT_SCHEMA_DENY,
            REDSHIFT_TABLE_ALLOW,
            REDSHIFT_TABLE_DENY,
            REDSHIFT_VIEW_ALLOW,
            REDSHIFT_VIEW_DENY,
        ],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
    [TABLEAU]: {
        fields: [TABLEAU_CONNECTION_URI, TABLEAU_PROJECT, TABLEAU_SITE, TABLEAU_USERNAME, TABLEAU_PASSWORD],
        filterFields: [],
        advancedFields: [INGEST_TAGS, INGEST_OWNER],
    },
    [LOOKER]: {
        fields: [LOOKER_BASE_URL, LOOKER_CLIENT_ID, LOOKER_CLIENT_SECRET],
        filterFields: [DASHBOARD_ALLOW, DASHBOARD_DENY, CHART_ALLOW, CHART_DENY],
        advancedFields: [GITHUB_INFO_REPO, EXTRACT_USAGE_HISTORY, EXTRACT_OWNERS, SKIP_PERSONAL_FOLDERS],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
    [KAFKA]: {
        fields: [
            KAFKA_SECURITY_PROTOCOL,
            KAFKA_SASL_MECHANISM,
            KAFKA_SASL_USERNAME,
            KAFKA_SASL_PASSWORD,
            KAFKA_BOOTSTRAP,
            KAFKA_SCHEMA_REGISTRY_URL,
            KAFKA_SCHEMA_REGISTRY_USER_CREDENTIAL,
        ],
        filterFields: [TOPIC_ALLOW, TOPIC_DENY],
        advancedFields: [STATEFUL_INGESTION_ENABLED],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
    [POSTGRES]: {
        fields: [POSTGRES_HOST_PORT, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD],
        filterFields: [
            REDSHIFT_SCHEMA_ALLOW,
            REDSHIFT_SCHEMA_DENY,
            REDSHIFT_TABLE_ALLOW,
            REDSHIFT_TABLE_DENY,
            REDSHIFT_VIEW_ALLOW,
            REDSHIFT_VIEW_DENY,
        ],
        advancedFields: [STATEFUL_INGESTION_ENABLED, PROFILING_ENABLED],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
    [HIVE]: {
        fields: [HIVE_HOST_PORT, HIVE_DATABASE, HIVE_USERNAME, HIVE_PASSWORD],
        filterFields: [
            REDSHIFT_SCHEMA_ALLOW,
            REDSHIFT_SCHEMA_DENY,
            REDSHIFT_TABLE_ALLOW,
            REDSHIFT_TABLE_DENY,
            REDSHIFT_VIEW_ALLOW,
            REDSHIFT_VIEW_DENY,
        ],
        advancedFields: [STATEFUL_INGESTION_ENABLED, PROFILING_ENABLED],
        filterSectionTooltip:
            'Filter out data assets based on allow/deny regex patterns we match against. Deny patterns take precedence over allow patterns.',
    },
};

export const CONNECTORS_WITH_FORM = new Set(Object.keys(RECIPE_FIELDS));
