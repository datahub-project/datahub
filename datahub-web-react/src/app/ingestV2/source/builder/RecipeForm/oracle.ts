import { get, omit } from 'lodash';

import { FieldType, PROFILING_ENABLED, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const ORACLE = 'oracle';

// Core section

const hostPortFieldPath = 'source.config.host_port';
export const ORACLE_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip: 'Oracle host and port (format: oracle.company.com:1521). Network must allow DataHub to connect.',
    type: FieldType.TEXT,
    fieldPath: hostPortFieldPath,
    placeholder: 'oracle.company.com:1521',
    required: true,
    rules: null,
};

const usernameFieldPath = 'source.config.username';
export const ORACLE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Oracle username for authentication. User needs SELECT on tables/views and system tables.',
    type: FieldType.TEXT,
    fieldPath: usernameFieldPath,
    placeholder: 'oracle_user',
    required: true,
    rules: null,
};

const passwordFieldPath = 'source.config.password';
export const ORACLE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Password for Oracle authentication. Consider using encrypted passwords or Oracle Wallet.',
    type: FieldType.SECRET,
    fieldPath: passwordFieldPath,
    placeholder: 'oracle_password',
    required: true,
    rules: null,
};

const oracleIdentifierFieldPath = 'source.config.connection_identifier_type';
const oracleIdentifierFieldName = 'connection_identifier_type';
const oracleIdentifierDatabase = 'database';
const oracleIdentifierServiceName = 'service_name';
const databaseFieldPath = 'source.config.database';
const serviceNameFieldPath = 'source.config.service_name';
export const ORACLE_IDENTIFIER: RecipeField = {
    name: oracleIdentifierFieldName,
    label: 'Oracle Identifier',
    tooltip: 'Oracle Connection Identifier Type',
    type: FieldType.SELECT,
    options: [
        { label: 'Database (SID)', value: oracleIdentifierDatabase },
        { label: 'Service Name', value: oracleIdentifierServiceName },
    ],
    fieldPath: oracleIdentifierFieldPath,
    placeholder: 'ORCL',
    required: true,
    rules: null,
    getValueFromRecipeOverride: (recipe) => {
        const isDatabaseFilled = !!get(recipe, databaseFieldPath);
        const isServiceNameFilled = !!get(recipe, serviceNameFieldPath);

        if (isDatabaseFilled) return oracleIdentifierDatabase;
        if (isServiceNameFilled) return oracleIdentifierServiceName;

        return oracleIdentifierDatabase;
    },
    setValueOnRecipeOverride: (recipe, value) => {
        let updatedRecipe = { ...recipe };
        if (value === oracleIdentifierDatabase) {
            updatedRecipe = omit(updatedRecipe, serviceNameFieldPath);
        } else {
            updatedRecipe = omit(updatedRecipe, databaseFieldPath);
        }

        return updatedRecipe;
    },
};

export const ORACLE_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database (SID)',
    tooltip: 'Oracle database name (SID). Leave empty if using Service Name. Do not use both.',
    type: FieldType.TEXT,
    fieldPath: databaseFieldPath,
    placeholder: 'ORCL',
    dynamicRequired: (values) => get(values, oracleIdentifierFieldName) === oracleIdentifierDatabase,
    dynamicHidden: (values) => get(values, oracleIdentifierFieldName) !== oracleIdentifierDatabase,
    rules: null,
};

export const ORACLE_SERVICE_NAME: RecipeField = {
    name: 'service_name',
    label: 'Service Name',
    tooltip: 'Oracle service name. Use either service_name OR database not both.',
    type: FieldType.TEXT,
    fieldPath: serviceNameFieldPath,
    placeholder: 'XEPDB1',
    dynamicRequired: (values) => get(values, oracleIdentifierFieldName) === oracleIdentifierServiceName,
    dynamicHidden: (values) => get(values, oracleIdentifierFieldName) !== oracleIdentifierServiceName,
    rules: null,
};

// Settings section

const platformInstanceFieldPath = 'source.config.platform_instance';
export const ORACLE_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'Optional instance identifier (e.g. prod_oracle). Critical for multi-database deployments and lineage.',
    type: FieldType.TEXT,
    fieldPath: platformInstanceFieldPath,
    placeholder: 'prod-oracle',
    rules: null,
};

const includeTablesFieldPath = 'source.config.include_tables';
export const ORACLE_INCLUDE_TABLES: RecipeField = {
    name: 'include_tables',
    label: 'Extract Tables',
    tooltip: 'Extract Oracle tables. Disable only if you want views without tables.',
    type: FieldType.BOOLEAN,
    fieldPath: includeTablesFieldPath,
    rules: null,
};

const includeViewsFieldPath = 'source.config.include_views';
export const ORACLE_INCLUDE_VIEWS: RecipeField = {
    name: 'include_views',
    label: 'Extract Views',
    tooltip: 'Extract Oracle views. Disable only if you want tables without views.',
    type: FieldType.BOOLEAN,
    fieldPath: includeViewsFieldPath,
    rules: null,
};

const includeViewLineageFieldPath = 'source.config.include_view_lineage';
export const ORACLE_INCLUDE_VIEW_LINEAGE: RecipeField = {
    name: 'include_view_lineage',
    label: 'Extract View Lineage',
    tooltip:
        'Extract view-to-table lineage by parsing view definitions. Shows which tables/views each view references.',
    type: FieldType.BOOLEAN,
    fieldPath: includeViewLineageFieldPath,
    rules: null,
};

const includeViewColumnLineageFieldPath = 'source.config.include_view_column_lineage';
export const ORACLE_INCLUDE_VIEW_COLUMN_LINEAGE: RecipeField = {
    name: 'include_view_column_lineage',
    label: 'Extract View Column Lineage',
    tooltip: 'Extract column-level lineage for views. More granular than table lineage. Increases ingestion time.',
    type: FieldType.BOOLEAN,
    fieldPath: includeViewColumnLineageFieldPath,
    rules: null,
};

export const ORACLE_PROFILING_ENABLED: RecipeField = {
    ...PROFILING_ENABLED,
    tooltip: 'Run profiling queries for table-level statistics (row counts, table size). May increase run time.',
};

const extractUsageHistoryFieldPath = 'source.config.extract_usage_history';
export const ORACLE_EXTRACT_USAGE_HISTORY: RecipeField = {
    name: 'extract_usage_history',
    label: 'Extract Usage',
    tooltip: 'Extract usage statistics from Oracle query logs. Shows which users/queries access tables.',
    type: FieldType.BOOLEAN,
    fieldPath: extractUsageHistoryFieldPath,
    rules: null,
};

const convertUrnsToLowercaseFieldPath = 'source.config.convert_urns_to_lowercase';
export const ORACLE_CONVERT_URNS_TO_LOWERCASE: RecipeField = {
    name: 'convert_urns_to_lowercase',
    label: 'Standardize URN Generation',
    tooltip: 'Convert URNs to lowercase for consistency. Recommended for accurate lineage.',
    type: FieldType.BOOLEAN,
    fieldPath: convertUrnsToLowercaseFieldPath,
    rules: null,
};
