import {
    AZURE_AUTHORITY_URL,
    AZURE_CLIENT_ID,
    AZURE_CLIENT_SECRET,
    AZURE_GRAPH_URL,
    AZURE_INGEST_GROUPS,
    AZURE_INGEST_USERS,
    AZURE_REDIRECT_URL,
    AZURE_TENANT_ID,
    AZURE_TOKEN_URL,
    GROUP_ALLOW,
    GROUP_DENY,
    USER_ALLOW,
    USER_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/azure';
import {
    BIGQUERY_CLIENT_EMAIL,
    BIGQUERY_CLIENT_ID,
    BIGQUERY_PRIVATE_KEY,
    BIGQUERY_PRIVATE_KEY_ID,
} from '@app/ingestV2/source/builder/RecipeForm/bigquery';
import {
    BIGQUERY_BETA_PROJECT_ID,
    DATASET_ALLOW,
    DATASET_DENY,
    PROJECT_ALLOW,
    PROJECT_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/bigqueryBeta';
import {
    COLUMN_PROFILING_ENABLED,
    DATABASE_ALLOW,
    DATABASE_DENY,
    ENV,
    EXTRACT_OWNERS,
    EXTRACT_USAGE_HISTORY,
    FieldType,
    FilterRecipeField,
    INCLUDE_LINEAGE,
    INCLUDE_TABLES,
    INCLUDE_TABLE_LINEAGE,
    INCLUDE_VIEWS,
    INGEST_OWNER,
    INGEST_TAGS,
    PROFILING_ENABLED,
    PROFILING_TABLE_LEVEL_ONLY,
    REMOVE_STALE_METADATA_ENABLED,
    RecipeField,
    SCHEMA_ALLOW,
    SCHEMA_DENY,
    SKIP_PERSONAL_FOLDERS,
    START_TIME,
    STATEFUL_INGESTION_ENABLED,
    TABLE_ALLOW,
    TABLE_DENY,
    TABLE_LINEAGE_MODE,
    TABLE_PROFILING_ENABLED,
    VIEW_ALLOW,
    VIEW_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/common';
import {
    CONFLUENCE_API_TOKEN,
    CONFLUENCE_DEPLOYMENT_TYPE,
    CONFLUENCE_PAGE_ALLOW,
    CONFLUENCE_PAGE_DENY,
    CONFLUENCE_PERSONAL_ACCESS_TOKEN,
    CONFLUENCE_SPACE_ALLOW,
    CONFLUENCE_SPACE_DENY,
    CONFLUENCE_URL,
    CONFLUENCE_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/confluence';
import {
    CSV_ARRAY_DELIMITER,
    CSV_DELIMITER,
    CSV_FILE_URL,
    CSV_WRITE_SEMANTICS,
} from '@app/ingestV2/source/builder/RecipeForm/csv';
import {
    DBT_CLOUD_ACCOUNT_ID,
    DBT_CLOUD_JOB_ID,
    DBT_CLOUD_PROJECT_ID,
    DBT_CLOUD_TOKEN,
    EXTRACT_OWNERS as DBT_EXTRACT_OWNERS,
    INCLUDE_MODELS,
    INCLUDE_SEEDS,
    INCLUDE_SOURCES,
    INCLUDE_TEST_DEFINITIONS,
    INCLUDE_TEST_RESULTS,
    NODE_ALLOW,
    NODE_DENY,
    TARGET_PLATFORM,
    TARGET_PLATFORM_INSTANCE,
} from '@app/ingestV2/source/builder/RecipeForm/dbt_cloud';
import {
    DORIS,
    DORIS_DATABASE,
    DORIS_HOST_PORT,
    DORIS_PASSWORD,
    DORIS_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/doris';
import {
    DREMIO,
    DREMIO_AUTHENTICATION_METHOD,
    DREMIO_DREMIO_CLOUD_PROJECT_ID,
    DREMIO_DREMIO_CLOUD_REGION,
    DREMIO_HOSTNAME,
    DREMIO_INCLUDE_QUERY_LINEAGE,
    DREMIO_INGEST_OWNER,
    DREMIO_IS_DREMIO_CLOUD,
    DREMIO_PASSWORD,
    DREMIO_PLATFORM_INSTANCE,
    DREMIO_PORT,
    DREMIO_PROFILE_ALLOW,
    DREMIO_PROFILE_DENY,
    DREMIO_TLS,
    DREMIO_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/dremio';
import {
    GLUE,
    GLUE_AWS_ACCESS_KEY_ID,
    GLUE_AWS_AUTHORIZATION_METHOD,
    GLUE_AWS_REGION,
    GLUE_AWS_ROLE,
    GLUE_AWS_SECRET_ACCESS_KEY,
    GLUE_AWS_SESSION_TOKEN,
    GLUE_CATALOG_ID,
    GLUE_EMIT_S3_LINEAGE,
    GLUE_EXTRACT_OWNERS,
    GLUE_EXTRACT_TRANSFORMS,
    GLUE_INCLUDE_COLUMN_LINEAGE,
    GLUE_PLATFORM_INSTANCE,
    GLUE_PROFILING_ENABLED,
    GLUE_REMOVE_STALE_METADATA_ENABLED,
} from '@app/ingestV2/source/builder/RecipeForm/glue';
import {
    HIVE_DATABASE,
    HIVE_HOST_PORT,
    HIVE_PASSWORD,
    HIVE_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/hive';
import {
    KAFKA_BOOTSTRAP,
    KAFKA_SASL_MECHANISM,
    KAFKA_SASL_PASSWORD,
    KAFKA_SASL_USERNAME,
    KAFKA_SCHEMA_REGISTRY_URL,
    KAFKA_SCHEMA_REGISTRY_USER_CREDENTIAL,
    KAFKA_SECURITY_PROTOCOL,
    TOPIC_ALLOW,
    TOPIC_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/kafka';
import {
    CHART_ALLOW,
    CHART_DENY,
    LOOKER_BASE_URL,
    LOOKER_CLIENT_ID,
    LOOKER_CLIENT_SECRET,
    DASHBOARD_ALLOW as LOOKER_DASHBOARD_ALLOW,
    DASHBOARD_DENY as LOOKER_DASHBOARD_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/looker';
import {
    CONNECTION_TO_PLATFORM_MAP,
    LOOKML,
    LOOKML_BASE_URL,
    LOOKML_CLIENT_ID,
    LOOKML_CLIENT_SECRET,
    LOOKML_GIT_INFO_DEPLOY_KEY,
    LOOKML_GIT_INFO_REPO,
    LOOKML_GIT_INFO_REPO_SSH_LOCATOR,
    PARSE_TABLE_NAMES_FROM_SQL,
    PROJECT_NAME,
} from '@app/ingestV2/source/builder/RecipeForm/lookml';
import {
    MARIADB,
    MARIADB_DATABASE,
    MARIADB_HOST_PORT,
    MARIADB_PASSWORD,
    MARIADB_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/mariadb';
import {
    MSSQL,
    MSSQL_DATABASE,
    MSSQL_HOST_PORT,
    MSSQL_PASSWORD,
    MSSQL_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/mssql';
import { MYSQL_HOST_PORT, MYSQL_PASSWORD, MYSQL_USERNAME } from '@app/ingestV2/source/builder/RecipeForm/mysql';
import { NOTION_API_KEY, NOTION_PAGE_IDS } from '@app/ingestV2/source/builder/RecipeForm/notion';
import {
    INCLUDE_DEPROVISIONED_USERS,
    INCLUDE_SUSPENDED_USERS,
    INGEST_GROUPS,
    INGEST_USERS,
    OKTA_API_TOKEN,
    OKTA_DOMAIN_URL,
    PROFILE_TO_GROUP,
    PROFILE_TO_GROUP_REGX_ALLOW,
    PROFILE_TO_GROUP_REGX_DENY,
    PROFILE_TO_USER,
    PROFILE_TO_USER_REGEX_DENY,
    PROFILE_TO_USER_REGX_ALLOW,
    SKIP_USERS_WITHOUT_GROUP,
} from '@app/ingestV2/source/builder/RecipeForm/okta';
import {
    ORACLE,
    ORACLE_CONVERT_URNS_TO_LOWERCASE,
    ORACLE_DATABASE,
    ORACLE_EXTRACT_USAGE_HISTORY,
    ORACLE_HOST_PORT,
    ORACLE_IDENTIFIER,
    ORACLE_INCLUDE_TABLES,
    ORACLE_INCLUDE_VIEWS,
    ORACLE_INCLUDE_VIEW_COLUMN_LINEAGE,
    ORACLE_INCLUDE_VIEW_LINEAGE,
    ORACLE_PASSWORD,
    ORACLE_PLATFORM_INSTANCE,
    ORACLE_PROFILING_ENABLED,
    ORACLE_SERVICE_NAME,
    ORACLE_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/oracle';
import {
    POSTGRES_DATABASE,
    POSTGRES_HOST_PORT,
    POSTGRES_PASSWORD,
    POSTGRES_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/postgres';
import {
    ADMIN_APIS_ONLY,
    EXTRACT_ENDORSEMENTS_AS_TAGS,
    EXTRACT_OWNERSHIP,
    INCLUDE_POWERBI_LINEAGE,
    INCLUDE_REPORTS,
    INCLUDE_WORKSPACES,
    POWERBI_CLIENT_ID,
    POWERBI_CLIENT_SECRET,
    POWERBI_TENANT_ID,
    WORKSPACE_ID_ALLOW,
    WORKSPACE_ID_DENY,
} from '@app/ingestV2/source/builder/RecipeForm/powerbi';
import {
    PRESTO,
    PRESTO_DATABASE,
    PRESTO_HOST_PORT,
    PRESTO_PASSWORD,
    PRESTO_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/presto';
import {
    REDSHIFT_DATABASE,
    REDSHIFT_HOST_PORT,
    REDSHIFT_PASSWORD,
    REDSHIFT_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/redshift';
import {
    FOLDER_ALLOW,
    FOLDER_DENY,
    INGEST_APPLICATIONS,
    INGEST_STORIES,
    RESOURCE_ID_ALLOW,
    RESOURCE_ID_DENY,
    RESOURCE_NAME_ALLOW,
    RESOURCE_NAME_DENY,
    SAC_CLIENT_ID,
    SAC_CLIENT_SECRET,
    SAC_TENANT_URL,
    SAC_TOKEN_URL,
} from '@app/ingestV2/source/builder/RecipeForm/sac';
import {
    SNOWFLAKE_ACCOUNT_ID,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_USERNAME,
    SNOWFLAKE_WAREHOUSE,
} from '@app/ingestV2/source/builder/RecipeForm/snowflake';
import {
    TABLEAU_CONNECTION_URI,
    TABLEAU_PASSWORD,
    TABLEAU_PROJECT,
    TABLEAU_SITE,
    TABLEAU_TOKEN_NAME,
    TABLEAU_TOKEN_VALUE,
    TABLEAU_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/tableau';
import {
    TRINO,
    TRINO_DATABASE,
    TRINO_HOST_PORT,
    TRINO_PASSWORD,
    TRINO_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/trino';
import {
    INCLUDE_COLUMN_LINEAGE,
    TOKEN,
    UNITY_CATALOG_ALLOW,
    UNITY_CATALOG_DENY,
    UNITY_METASTORE_ID_ALLOW,
    UNITY_METASTORE_ID_DENY,
    UNITY_TABLE_ALLOW,
    UNITY_TABLE_DENY,
    WORKSPACE_URL,
} from '@app/ingestV2/source/builder/RecipeForm/unity_catalog';
import {
    INCLUDE_MLMODELS,
    INCLUDE_PROJECTIONS,
    INCLUDE_PROJECTIONS_LINEAGE,
    INCLUDE_VIEW_LINEAGE,
    VERTICA_DATABASE,
    VERTICA_HOST_PORT,
    VERTICA_PASSWORD,
    VERTICA_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/vertica';
import {
    AZURE,
    CONFLUENCE,
    CSV,
    DATABRICKS,
    DBT_CLOUD,
    MYSQL,
    NOTION,
    OKTA,
    POWER_BI,
    SAC,
    VERTICA,
} from '@app/ingestV2/source/builder/constants';
import { BIGQUERY } from '@app/ingestV2/source/conf/bigquery/bigquery';
import { HIVE } from '@app/ingestV2/source/conf/hive/hive';
import { KAFKA } from '@app/ingestV2/source/conf/kafka/kafka';
import { LOOKER } from '@app/ingestV2/source/conf/looker/looker';
import { POSTGRES } from '@app/ingestV2/source/conf/postgres/postgres';
import { REDSHIFT } from '@app/ingestV2/source/conf/redshift/redshift';
import { SNOWFLAKE } from '@app/ingestV2/source/conf/snowflake/snowflake';
import { TABLEAU } from '@app/ingestV2/source/conf/tableau/tableau';

export enum RecipeSections {
    Connection = 0,
    Filter = 1,
    Advanced = 2,
}

interface RecipeFields {
    [key: string]: {
        fields: RecipeField[];
        filterFields: FilterRecipeField[];
        advancedFields: RecipeField[];
        connectionSectionTooltip?: string;
        filterSectionTooltip?: string;
        advancedSectionTooltip?: string;
        defaultOpenSections?: RecipeSections[];
        hasDynamicFields?: boolean;
    };
}

export const PLATFORM: RecipeField = {
    name: 'platform',
    label: 'Platform',
    helper: 'Data Platform ID in DataHub',
    tooltip: 'The Data Platform ID in DataHub (e.g. snowflake, bigquery, redshift, mysql, postgres)',
    type: FieldType.TEXT,
    fieldPath: 'platform',
    placeholder: 'snowflake',
    rules: [{ required: true, message: 'Platform is required' }],
};

export const DEFAULT_DB: RecipeField = {
    name: 'default_db',
    label: 'Default Database',
    helper: 'Database for assets from connection',
    tooltip: 'The Database associated with assets from the Looker connection.',
    type: FieldType.TEXT,
    fieldPath: 'default_db',
    placeholder: 'default_db',
    rules: [{ required: true, message: 'Default Database is required' }],
};

export const RECIPE_FIELDS: RecipeFields = {
    [SNOWFLAKE]: {
        fields: [SNOWFLAKE_ACCOUNT_ID, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            INCLUDE_LINEAGE,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterFields: [
            DATABASE_ALLOW,
            DATABASE_DENY,
            SCHEMA_ALLOW,
            SCHEMA_DENY,
            TABLE_ALLOW,
            TABLE_DENY,
            VIEW_ALLOW,
            VIEW_DENY,
        ],
        filterSectionTooltip: 'Include or exclude specific Databases, Schemas, Tables and Views from ingestion.',
    },
    [BIGQUERY]: {
        fields: [
            BIGQUERY_BETA_PROJECT_ID,
            BIGQUERY_PRIVATE_KEY,
            BIGQUERY_PRIVATE_KEY_ID,
            BIGQUERY_CLIENT_EMAIL,
            BIGQUERY_CLIENT_ID,
        ],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            INCLUDE_TABLE_LINEAGE,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            START_TIME,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterFields: [
            PROJECT_ALLOW,
            PROJECT_DENY,
            DATASET_ALLOW,
            DATASET_DENY,
            TABLE_ALLOW,
            TABLE_DENY,
            VIEW_ALLOW,
            VIEW_DENY,
        ],
        filterSectionTooltip: 'Include or exclude specific Projects, Datasets, Tables and Views from ingestion.',
    },
    [REDSHIFT]: {
        fields: [REDSHIFT_HOST_PORT, REDSHIFT_DATABASE, REDSHIFT_USERNAME, REDSHIFT_PASSWORD],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            INCLUDE_TABLE_LINEAGE,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            TABLE_LINEAGE_MODE,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [TABLEAU]: {
        fields: [
            TABLEAU_CONNECTION_URI,
            TABLEAU_PROJECT,
            TABLEAU_SITE,
            TABLEAU_TOKEN_NAME,
            TABLEAU_TOKEN_VALUE,
            STATEFUL_INGESTION_ENABLED,
            TABLEAU_USERNAME,
            TABLEAU_PASSWORD,
        ],
        filterFields: [],
        advancedFields: [INGEST_TAGS, INGEST_OWNER],
    },
    [LOOKER]: {
        fields: [LOOKER_BASE_URL, LOOKER_CLIENT_ID, LOOKER_CLIENT_SECRET],
        filterFields: [LOOKER_DASHBOARD_ALLOW, LOOKER_DASHBOARD_DENY, CHART_ALLOW, CHART_DENY],
        advancedFields: [EXTRACT_USAGE_HISTORY, EXTRACT_OWNERS, SKIP_PERSONAL_FOLDERS, STATEFUL_INGESTION_ENABLED],
        filterSectionTooltip: 'Include or exclude specific Dashboard, Charts from Looker ingestion.',
    },
    [LOOKML]: {
        fields: [
            LOOKML_GIT_INFO_REPO,
            LOOKML_GIT_INFO_REPO_SSH_LOCATOR,
            LOOKML_GIT_INFO_DEPLOY_KEY,
            PROJECT_NAME,
            LOOKML_BASE_URL,
            LOOKML_CLIENT_ID,
            LOOKML_CLIENT_SECRET,
        ],
        filterFields: [],
        advancedFields: [PARSE_TABLE_NAMES_FROM_SQL, CONNECTION_TO_PLATFORM_MAP, STATEFUL_INGESTION_ENABLED],
        advancedSectionTooltip:
            'In order to ingest LookML data properly, you must either fill out Looker API client information (Base URL, Client ID, Client Secret) or an offline specification of the connection to platform mapping and the project name (Connection To Platform Map, Project Name).',
        defaultOpenSections: [RecipeSections.Connection, RecipeSections.Advanced],
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
        fields: [POSTGRES_HOST_PORT, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [MYSQL]: {
        fields: [MYSQL_HOST_PORT, MYSQL_USERNAME, MYSQL_PASSWORD],
        filterFields: [
            DATABASE_ALLOW,
            DATABASE_DENY,
            SCHEMA_ALLOW,
            SCHEMA_DENY,
            TABLE_ALLOW,
            TABLE_DENY,
            VIEW_ALLOW,
            VIEW_DENY,
        ],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Databases, Schemas, Tables and Views from ingestion.',
    },
    [HIVE]: {
        fields: [HIVE_HOST_PORT, HIVE_USERNAME, HIVE_PASSWORD, HIVE_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [INCLUDE_TABLES, TABLE_PROFILING_ENABLED, COLUMN_PROFILING_ENABLED, STATEFUL_INGESTION_ENABLED],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [PRESTO]: {
        fields: [PRESTO_HOST_PORT, PRESTO_USERNAME, PRESTO_PASSWORD, PRESTO_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [MSSQL]: {
        fields: [MSSQL_HOST_PORT, MSSQL_USERNAME, MSSQL_PASSWORD, MSSQL_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [TRINO]: {
        fields: [TRINO_HOST_PORT, TRINO_USERNAME, TRINO_PASSWORD, TRINO_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [MARIADB]: {
        fields: [MARIADB_HOST_PORT, MARIADB_USERNAME, MARIADB_PASSWORD, MARIADB_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [DORIS]: {
        fields: [DORIS_HOST_PORT, DORIS_USERNAME, DORIS_PASSWORD, DORIS_DATABASE],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            TABLE_PROFILING_ENABLED,
            COLUMN_PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables and Views from ingestion.',
    },
    [DATABRICKS]: {
        fields: [WORKSPACE_URL, TOKEN],
        filterFields: [
            UNITY_METASTORE_ID_ALLOW,
            UNITY_METASTORE_ID_DENY,
            UNITY_CATALOG_ALLOW,
            UNITY_CATALOG_DENY,
            SCHEMA_ALLOW,
            SCHEMA_DENY,
            UNITY_TABLE_ALLOW,
            UNITY_TABLE_DENY,
        ],
        advancedFields: [INCLUDE_TABLE_LINEAGE, INCLUDE_COLUMN_LINEAGE, STATEFUL_INGESTION_ENABLED],
        filterSectionTooltip: 'Include or exclude specific Metastores, Catalogs, Schemas, and Tables from ingestion.',
    },
    [DBT_CLOUD]: {
        fields: [
            DBT_CLOUD_ACCOUNT_ID,
            DBT_CLOUD_PROJECT_ID,
            DBT_CLOUD_JOB_ID,
            DBT_CLOUD_TOKEN,
            TARGET_PLATFORM,
            TARGET_PLATFORM_INSTANCE,
        ],
        filterFields: [NODE_ALLOW, NODE_DENY],
        advancedFields: [
            INCLUDE_MODELS,
            INCLUDE_SOURCES,
            INCLUDE_SEEDS,
            INCLUDE_TEST_DEFINITIONS,
            INCLUDE_TEST_RESULTS,
            DBT_EXTRACT_OWNERS,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific dbt Node (resources) from ingestion.',
    },
    [POWER_BI]: {
        fields: [POWERBI_TENANT_ID, POWERBI_CLIENT_ID, POWERBI_CLIENT_SECRET],
        filterFields: [WORKSPACE_ID_ALLOW, WORKSPACE_ID_DENY],
        advancedFields: [
            INCLUDE_WORKSPACES,
            INCLUDE_REPORTS,
            INCLUDE_POWERBI_LINEAGE,
            EXTRACT_OWNERSHIP,
            EXTRACT_ENDORSEMENTS_AS_TAGS,
            ADMIN_APIS_ONLY,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific PowerBI Workspaces from ingestion.',
    },
    [VERTICA]: {
        fields: [VERTICA_HOST_PORT, VERTICA_DATABASE, VERTICA_USERNAME, VERTICA_PASSWORD],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            INCLUDE_TABLES,
            INCLUDE_VIEWS,
            INCLUDE_PROJECTIONS,
            INCLUDE_MLMODELS,
            INCLUDE_VIEW_LINEAGE,
            INCLUDE_PROJECTIONS_LINEAGE,
            TABLE_PROFILING_ENABLED,
        ],
        filterSectionTooltip: 'Include or exclude specific Schemas, Tables, Views and Projections from ingestion.',
    },
    [CSV]: {
        fields: [CSV_FILE_URL],
        filterFields: [],
        advancedFields: [CSV_ARRAY_DELIMITER, CSV_DELIMITER, CSV_WRITE_SEMANTICS],
    },
    [OKTA]: {
        fields: [OKTA_DOMAIN_URL, OKTA_API_TOKEN, PROFILE_TO_USER, PROFILE_TO_GROUP],
        filterFields: [
            PROFILE_TO_GROUP_REGX_ALLOW,
            PROFILE_TO_GROUP_REGX_DENY,
            PROFILE_TO_USER_REGX_ALLOW,
            PROFILE_TO_USER_REGEX_DENY,
        ],
        advancedFields: [
            INGEST_USERS,
            INGEST_GROUPS,
            INCLUDE_DEPROVISIONED_USERS,
            INCLUDE_SUSPENDED_USERS,
            STATEFUL_INGESTION_ENABLED,
            SKIP_USERS_WITHOUT_GROUP,
        ],
    },
    [NOTION]: {
        fields: [NOTION_API_KEY, NOTION_PAGE_IDS],
        filterFields: [],
        advancedFields: [],
    },
    [CONFLUENCE]: {
        fields: [
            CONFLUENCE_DEPLOYMENT_TYPE,
            CONFLUENCE_URL,
            CONFLUENCE_USERNAME,
            CONFLUENCE_API_TOKEN,
            CONFLUENCE_PERSONAL_ACCESS_TOKEN,
        ],
        filterFields: [CONFLUENCE_SPACE_ALLOW, CONFLUENCE_SPACE_DENY, CONFLUENCE_PAGE_ALLOW, CONFLUENCE_PAGE_DENY],
        advancedFields: [],
        filterSectionTooltip:
            'Control which Confluence content is ingested by filtering spaces and pages. Leave empty to ingest all accessible content.',
        defaultOpenSections: [RecipeSections.Filter],
    },
    [AZURE]: {
        fields: [
            AZURE_CLIENT_ID,
            AZURE_TENANT_ID,
            AZURE_CLIENT_SECRET,
            AZURE_REDIRECT_URL,
            AZURE_AUTHORITY_URL,
            AZURE_TOKEN_URL,
            AZURE_GRAPH_URL,
        ],
        filterFields: [GROUP_ALLOW, GROUP_DENY, USER_ALLOW, USER_DENY],
        advancedFields: [AZURE_INGEST_USERS, AZURE_INGEST_GROUPS, STATEFUL_INGESTION_ENABLED, SKIP_USERS_WITHOUT_GROUP],
    },
    [SAC]: {
        fields: [SAC_TENANT_URL, SAC_TOKEN_URL, SAC_CLIENT_ID, SAC_CLIENT_SECRET],
        filterFields: [
            FOLDER_ALLOW,
            FOLDER_DENY,
            RESOURCE_NAME_ALLOW,
            RESOURCE_NAME_DENY,
            RESOURCE_ID_ALLOW,
            RESOURCE_ID_DENY,
        ],
        advancedFields: [INGEST_STORIES, INGEST_APPLICATIONS, STATEFUL_INGESTION_ENABLED],
    },
    [GLUE]: {
        fields: [
            GLUE_AWS_REGION,
            GLUE_AWS_AUTHORIZATION_METHOD,
            GLUE_AWS_ACCESS_KEY_ID,
            GLUE_AWS_SECRET_ACCESS_KEY,
            GLUE_AWS_SESSION_TOKEN,
            GLUE_AWS_ROLE,
            GLUE_CATALOG_ID,
        ],
        filterFields: [DATABASE_ALLOW, DATABASE_DENY, TABLE_ALLOW, TABLE_DENY],
        advancedFields: [
            GLUE_PLATFORM_INSTANCE,
            ENV,
            GLUE_EXTRACT_OWNERS,
            GLUE_EXTRACT_TRANSFORMS,
            GLUE_EMIT_S3_LINEAGE,
            GLUE_INCLUDE_COLUMN_LINEAGE,
            GLUE_PROFILING_ENABLED,
            PROFILING_TABLE_LEVEL_ONLY,
            GLUE_REMOVE_STALE_METADATA_ENABLED,
        ],
        hasDynamicFields: true,
    },
    [ORACLE]: {
        fields: [
            ORACLE_HOST_PORT,
            ORACLE_USERNAME,
            ORACLE_PASSWORD,
            ORACLE_IDENTIFIER,
            ORACLE_DATABASE,
            ORACLE_SERVICE_NAME,
        ],
        filterFields: [SCHEMA_ALLOW, SCHEMA_DENY, TABLE_ALLOW, TABLE_DENY, VIEW_ALLOW, VIEW_DENY],
        advancedFields: [
            ORACLE_PLATFORM_INSTANCE,
            ENV,
            ORACLE_INCLUDE_TABLES,
            ORACLE_INCLUDE_VIEWS,
            ORACLE_INCLUDE_VIEW_LINEAGE,
            ORACLE_INCLUDE_VIEW_COLUMN_LINEAGE,
            ORACLE_PROFILING_ENABLED,
            PROFILING_TABLE_LEVEL_ONLY,
            ORACLE_EXTRACT_USAGE_HISTORY,
            ORACLE_CONVERT_URNS_TO_LOWERCASE,
            REMOVE_STALE_METADATA_ENABLED,
        ],
        hasDynamicFields: true,
    },
    [DREMIO]: {
        fields: [
            DREMIO_IS_DREMIO_CLOUD,
            DREMIO_DREMIO_CLOUD_REGION,
            DREMIO_DREMIO_CLOUD_PROJECT_ID,
            DREMIO_HOSTNAME,
            DREMIO_PORT,
            DREMIO_TLS,
            DREMIO_AUTHENTICATION_METHOD,
            DREMIO_USERNAME,
            DREMIO_PASSWORD,
        ],
        filterFields: [
            SCHEMA_ALLOW,
            SCHEMA_DENY,
            DATASET_ALLOW,
            DATASET_DENY,
            DREMIO_PROFILE_ALLOW,
            DREMIO_PROFILE_DENY,
        ],
        advancedFields: [
            DREMIO_PLATFORM_INSTANCE,
            ENV,
            DREMIO_INGEST_OWNER,
            DREMIO_INCLUDE_QUERY_LINEAGE,
            PROFILING_ENABLED,
            PROFILING_TABLE_LEVEL_ONLY,
            REMOVE_STALE_METADATA_ENABLED,
        ],
        hasDynamicFields: true,
    },
};

const ALL_CONNECTORS_WITH_FORM = Object.keys(RECIPE_FIELDS);
export const CONNECTORS_WITH_FORM_INCLUDING_DYNAMIC_FIELDS = new Set(ALL_CONNECTORS_WITH_FORM);
export const CONNECTORS_WITH_FORM_NO_DYNAMIC_FIELDS = new Set(
    ALL_CONNECTORS_WITH_FORM.filter((sourceType) => !RECIPE_FIELDS[sourceType].hasDynamicFields),
);
