import React from 'react';
import { set, get } from 'lodash';
import { SNOWFLAKE } from '../../conf/snowflake/snowflake';
import { BIGQUERY } from '../../conf/bigquery/bigquery';
import { REDSHIFT } from '../../conf/redshift/redshift';
import { LOOKER } from '../../conf/looker/looker';
import { TABLEAU } from '../../conf/tableau/tableau';

export enum FieldType {
    TEXT,
    BOOLEAN,
    LIST,
    SELECT,
    SECRET,
}

interface Option {
    label: string;
    value: string;
}

export interface RecipeField {
    name: string;
    label: string;
    tooltip: string | React.ReactNode;
    type: FieldType;
    fieldPath: string;
    rules: any[] | null;
    section?: string;
    options?: Option[];
    buttonLabel?: string;
    getValueFromRecipeOverride?: (recipe: any) => any;
    setValueOnRecipeOverride?: (recipe: any, value: any) => any;
}

function clearFieldAndParents(recipe: any, fieldPath: string) {
    set(recipe, fieldPath, undefined);

    const updatedFieldPath = fieldPath.split('.').slice(0, -1).join('.'); // remove last item from fieldPath
    if (updatedFieldPath) {
        const parentKeys = Object.keys(get(recipe, updatedFieldPath));

        // only child left is what we just set as undefined
        if (parentKeys.length === 1) {
            clearFieldAndParents(recipe, updatedFieldPath);
        }
    }
    return recipe;
}

export function setFieldValueOnRecipe(recipe: any, value: any, fieldPath: string) {
    const updatedRecipe = { ...recipe };
    if (value !== undefined) {
        if (value === null) {
            clearFieldAndParents(updatedRecipe, fieldPath);
            return updatedRecipe;
        }
        set(updatedRecipe, fieldPath, value);
    }
    return updatedRecipe;
}

export function setListValuesOnRecipe(recipe: any, values: string[] | undefined, fieldPath: string) {
    const updatedRecipe = { ...recipe };
    if (values !== undefined) {
        const filteredValues: string[] | undefined = values.filter((v) => !!v);
        return filteredValues.length
            ? setFieldValueOnRecipe(updatedRecipe, filteredValues, fieldPath)
            : setFieldValueOnRecipe(updatedRecipe, null, fieldPath);
    }
    return updatedRecipe;
}

export const SNOWFLAKE_ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: 'Snowflake account. e.g. abc48144',
    type: FieldType.TEXT,
    fieldPath: 'source.config.account_id',
    rules: null,
};

export const SNOWFLAKE_WAREHOUSE: RecipeField = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'Snowflake warehouse.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse',
    rules: null,
};

export const SNOWFLAKE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.username',
    rules: null,
};

export const SNOWFLAKE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    rules: null,
};

export const SNOWFLAKE_ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    tooltip: 'Snowflake role.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.role',
    rules: null,
};

export const BIGQUERY_PROJECT_ID: RecipeField = {
    name: 'project_id',
    label: 'BigQuery Project ID',
    tooltip: 'Project ID where you have rights to run queries and create tables.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_id',
    rules: null,
};

export const BIGQUERY_CREDENTIAL_PROJECT_ID: RecipeField = {
    name: 'credential.project_id',
    label: 'Credentials Project ID',
    tooltip: 'Project id to set the credentials.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.project_id',
    rules: null,
};

export const BIGQUERY_PRIVATE_KEY_ID: RecipeField = {
    name: 'credential.private_key_id',
    label: 'Private Key Id',
    tooltip: 'Private key id.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.private_key_id',
    rules: null,
};

export const BIGQUERY_PRIVATE_KEY: RecipeField = {
    name: 'credential.private_key',
    label: 'Private Key',
    tooltip: 'Private key in a form of "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n".',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.private_key',
    rules: null,
};

export const BIGQUERY_CLIENT_EMAIL: RecipeField = {
    name: 'credential.client_email',
    label: 'Client Email',
    tooltip: 'Client email.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.client_email',
    rules: null,
};

export const BIGQUERY_CLIENT_ID: RecipeField = {
    name: 'credential.client_id',
    label: 'Client ID',
    tooltip: 'Client ID.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.client_id',
    rules: null,
};

export const REDSHIFT_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host Port',
    tooltip: 'Host URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    rules: null,
};

export const REDSHIFT_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Database (catalog).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    rules: null,
};

export const REDSHIFT_USERNAME: RecipeField = {
    name: 'redshift.username',
    label: 'Redshift username',
    tooltip: 'Username',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    rules: null,
};

export const REDSHIFT_PASSWORD: RecipeField = {
    name: 'redshift.password',
    label: 'Redshift password',
    tooltip: 'Password',
    type: FieldType.TEXT,
    fieldPath: 'source.config.password',
    rules: null,
};

export const TABLEAU_CONNECTION_URI: RecipeField = {
    name: 'connect_uri',
    label: 'Connection URI',
    tooltip: 'Tableau host URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connect_uri',
    rules: null,
};

const tableauProjectFieldPath = 'source.config.projects';
export const TABLEAU_PROJECT: RecipeField = {
    name: 'projects',
    label: 'Projects',
    tooltip: 'List of projects',
    type: FieldType.LIST,
    buttonLabel: 'Add project',
    fieldPath: tableauProjectFieldPath,
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableauProjectFieldPath),
};

export const TABLEAU_SITE: RecipeField = {
    name: 'site',
    label: 'Tableau Site',
    tooltip:
        'Tableau Site. Always required for Tableau Online. Use empty string to connect with Default site on Tableau Server.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.site',
    rules: null,
};

export const TABLEAU_USERNAME: RecipeField = {
    name: 'tableau.username',
    label: 'Username',
    tooltip: 'Tableau username, must be set if authenticating using username/password.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    rules: null,
};

export const TABLEAU_PASSWORD: RecipeField = {
    name: 'tableau.password',
    label: 'Password',
    tooltip: 'Tableau password, must be set if authenticating using username/password.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.password',
    rules: null,
};

export const LOOKER_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    tooltip:
        'Url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar.Used for making API calls to Looker and constructing clickable dashboard and chart urls.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    rules: null,
};

export const LOOKER_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Looker API client id.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    rules: null,
};

export const LOOKER_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'Looker API client secret.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_secret',
    rules: null,
};

const includeLineageFieldPathA = 'source.config.include_table_lineage';
const includeLineageFieldPathB = 'source.config.include_view_lineage';
export const INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    label: 'Include Lineage',
    tooltip: 'Include Table and View lineage in your ingestion.',
    type: FieldType.BOOLEAN,
    fieldPath: includeLineageFieldPathA,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) =>
        get(recipe, includeLineageFieldPathA) && get(recipe, includeLineageFieldPathB),
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        let updatedRecipe = setFieldValueOnRecipe(recipe, value, includeLineageFieldPathA);
        updatedRecipe = setFieldValueOnRecipe(updatedRecipe, value, includeLineageFieldPathB);
        return updatedRecipe;
    },
};

export const PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Profiling',
    tooltip: 'Whether profiling should be done.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.profiling.enabled',
    rules: null,
};

export const STATEFUL_INGESTION_ENABLED: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Enable the type of the ingestion state provider registered with datahub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.enabled',
    rules: null,
};

export const UPSTREAM_LINEAGE_IN_REPORT: RecipeField = {
    name: 'upstream_lineage_in_report',
    label: 'Include Upstream Lineage In Report.',
    tooltip: 'Remove stale datasets from datahub once they have been deleted in the source.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.upstream_lineage_in_report',
    rules: null,
};

export const TABLE_LINEAGE_MODE: RecipeField = {
    name: 'table_lineage_mode',
    label: 'Table Lineage Mode',
    tooltip: (
        <div>
            <p>
                Which table lineage collector mode to use. Check out{' '}
                <a
                    href="https://datahubproject.io/docs/generated/ingestion/sources/redshift/#config-details"
                    target="_blank"
                    rel="noreferrer"
                >
                    the documentation
                </a>{' '}
                explaining the difference between the three available modes.
            </p>
        </div>
    ),
    type: FieldType.SELECT,
    fieldPath: 'source.config.table_lineage_mode',
    rules: null,
    options: [
        { label: 'stl_scan_based', value: 'stl_scan_based' },
        { label: 'sql_based', value: 'sql_based' },
        { label: 'mixed', value: 'mixed' },
    ],
};

export const INGEST_TAGS: RecipeField = {
    name: 'ingest_tags',
    label: 'Ingest Tags',
    tooltip: 'Ingest Tags from source. This will override Tags entered from UI',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_tags',
    rules: null,
};

export const INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Ingest Owner',
    tooltip: 'Ingest Owner from source. This will override Owner info entered from UI',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_owner',
    rules: null,
};

export const GITHUB_INFO_REPO: RecipeField = {
    name: 'github_info.repo',
    label: 'GitHub Repo',
    tooltip: (
        <div>
            <p>
                Name of your github repo. e.g. repo for{' '}
                <a href="https://github.com/datahub-project/datahub" target="_blank" rel="noreferrer">
                    https://github.com/datahub-project/datahub
                </a>{' '}
                is datahub-project/datahub.
            </p>
        </div>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.github_info.repo',
    rules: null,
};

export const EXTRACT_USAGE_HISTORY: RecipeField = {
    name: 'extract_usage_history',
    label: 'Extract Usage History',
    tooltip:
        'Experimental (Subject to breaking change) -- Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_usage_history',
    rules: null,
};

export const EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    label: 'Extract Owners',
    tooltip:
        'When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_owners',
    rules: null,
};

export const SKIP_PERSONAL_FOLDERS: RecipeField = {
    name: 'skip_personal_folders',
    label: 'Skip Personal Folders',
    tooltip:
        'Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.skip_personal_folders',
    rules: null,
};

const databaseAllowFieldPath = 'source.config.database_pattern.allow';
export const DATABASE_ALLOW: RecipeField = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: databaseAllowFieldPath,
    rules: null,
    section: 'Databases',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseAllowFieldPath),
};

const databaseDenyFieldPath = 'source.config.database_pattern.deny';
export const DATABASE_DENY: RecipeField = {
    name: 'database_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: databaseDenyFieldPath,
    rules: null,
    section: 'Databases',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.schema_pattern.deny';
export const SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewAllowFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewDenyFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

const chartAllowFieldPath = 'source.config.chart_pattern.allow';
export const CHART_ALLOW: RecipeField = {
    name: 'chart_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: chartAllowFieldPath,
    rules: null,
    section: 'Charts',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartAllowFieldPath),
};

const chartDenyFieldPath = 'source.config.chart_pattern.deny';
export const CHART_DENY: RecipeField = {
    name: 'chart_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: chartDenyFieldPath,
    rules: null,
    section: 'Charts',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartDenyFieldPath),
};

const dashboardAllowFieldPath = 'source.config.dashboard_pattern.allow';
export const DASHBOARD_ALLOW: RecipeField = {
    name: 'dashboard_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardAllowFieldPath,
    rules: null,
    section: 'Dashboards',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardAllowFieldPath),
};

const dashboardDenyFieldPath = 'source.config.dashboard_pattern.deny';
export const DASHBOARD_DENY: RecipeField = {
    name: 'dashboard_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardDenyFieldPath,
    rules: null,
    section: 'Dashboards',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardDenyFieldPath),
};

export const RECIPE_FIELDS = {
    [SNOWFLAKE]: {
        fields: [SNOWFLAKE_ACCOUNT_ID, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_USERNAME, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE],
        advancedFields: [INCLUDE_LINEAGE, PROFILING_ENABLED, STATEFUL_INGESTION_ENABLED],
        filterFields: [
            TABLE_ALLOW,
            TABLE_DENY,
            DATABASE_ALLOW,
            DATABASE_DENY,
            SCHEMA_ALLOW,
            SCHEMA_DENY,
            VIEW_ALLOW,
            VIEW_DENY,
        ],
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
        filterFields: [TABLE_ALLOW, TABLE_DENY, SCHEMA_ALLOW, SCHEMA_DENY, VIEW_ALLOW, VIEW_DENY],
    },
    [REDSHIFT]: {
        fields: [REDSHIFT_HOST_PORT, REDSHIFT_DATABASE, REDSHIFT_USERNAME, REDSHIFT_PASSWORD],
        advancedFields: [INCLUDE_LINEAGE, PROFILING_ENABLED, STATEFUL_INGESTION_ENABLED, TABLE_LINEAGE_MODE],
        filterFields: [TABLE_ALLOW, TABLE_DENY, SCHEMA_ALLOW, SCHEMA_DENY, VIEW_ALLOW, VIEW_DENY],
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
    },
};

export const CONNECTORS_WITH_FORM = new Set(Object.keys(RECIPE_FIELDS));
