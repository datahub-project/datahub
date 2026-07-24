import { get } from 'lodash';

import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const AAS_SERVER: RecipeField = {
    name: 'server',
    label: 'Server',
    helper: 'Azure Analysis Services or Power BI Premium XMLA endpoint',
    tooltip:
        'The connection string for the endpoint, e.g. asazure://<region>.asazure.windows.net/<server> or powerbi://api.powerbi.com/v1.0/myorg/<workspace>.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.server',
    placeholder: 'asazure://westus.asazure.windows.net/myserver',
    required: true,
    rules: null,
};

export const AAS_AUTH_TYPE: RecipeField = {
    name: 'auth_type',
    label: 'Authentication Type',
    helper: 'How to authenticate against Azure AD',
    tooltip:
        'Service principal uses a client id and secret. Device code and interactive drive a user login. Username & password uses resource owner credentials.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.auth_type',
    options: [
        { label: 'Service Principal', value: 'service_principal' },
        { label: 'Device Code', value: 'device_code' },
        { label: 'Interactive', value: 'interactive' },
        { label: 'Username & Password', value: 'username_password' },
    ],
    placeholder: 'service_principal',
    rules: null,
};

export const AAS_TENANT_ID: RecipeField = {
    name: 'tenant_id',
    label: 'Tenant ID',
    helper: 'Azure AD tenant (directory) id',
    tooltip: 'The Azure AD tenant id. Required for service principal authentication.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.tenant_id',
    placeholder: 'a949d688-67c0-4bf1-a344-e939411c6c0a',
    rules: null,
};

export const AAS_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    helper: 'Azure AD application (client) id',
    tooltip: 'The Azure AD application (client) id used to authenticate.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    placeholder: 'client id',
    rules: null,
};

export const AAS_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    helper: 'Client secret for the service principal',
    tooltip: 'The client secret for the service principal. Add it in the Secrets tab and reference it by name.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'client secret',
    rules: null,
};

const extractLineagePath = 'source.config.extract_lineage';
export const AAS_EXTRACT_LINEAGE: RecipeField = {
    name: 'extract_lineage',
    label: 'Extract Lineage',
    helper: 'Extract upstream lineage',
    tooltip: 'Extract upstream lineage from partition M/Power Query and native SQL.',
    type: FieldType.BOOLEAN,
    fieldPath: extractLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, extractLineagePath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return true;
    },
    rules: null,
};

const extractColumnLineagePath = 'source.config.extract_column_level_lineage';
export const AAS_EXTRACT_COLUMN_LINEAGE: RecipeField = {
    name: 'extract_column_level_lineage',
    label: 'Extract Column-Level Lineage',
    helper: 'Extract column-level lineage',
    tooltip: 'Extract column-level lineage, including intra-model DAX dependencies.',
    type: FieldType.BOOLEAN,
    fieldPath: extractColumnLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, extractColumnLineagePath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return true;
    },
    rules: null,
};

const databaseAllowFieldPath = 'source.config.database_pattern.allow';
export const AAS_DATABASE_ALLOW: FilterRecipeField = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific models',
    tooltip:
        'Only include specific tabular models (catalogs) by name or Regular Expression (REGEX). If not provided, all models are included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: databaseAllowFieldPath,
    rules: null,
    section: 'Models',
    filteringResource: 'Model',
    placeholder: 'SalesModel',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseAllowFieldPath),
};

const databaseDenyFieldPath = 'source.config.database_pattern.deny';
export const AAS_DATABASE_DENY: FilterRecipeField = {
    name: 'database_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific models',
    tooltip:
        'Exclude specific tabular models (catalogs) by name or Regular Expression (REGEX). Deny patterns always take precedence over allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: databaseDenyFieldPath,
    rules: null,
    section: 'Models',
    filteringResource: 'Model',
    placeholder: 'SalesModel',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const AAS_TABLE_ALLOW: FilterRecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific tables',
    tooltip:
        'Only include specific tables by name or Regular Expression (REGEX). If not provided, all tables are included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    placeholder: 'Sales',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const AAS_TABLE_DENY: FilterRecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific tables',
    tooltip:
        'Exclude specific tables by name or Regular Expression (REGEX). Deny patterns always take precedence over allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    placeholder: 'Sales',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};
