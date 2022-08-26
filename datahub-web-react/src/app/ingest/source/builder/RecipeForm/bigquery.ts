import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

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

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const BIGQUERY_SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.schema_pattern.deny';
export const BIGQUERY_SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const BIGQUERY_TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.table_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const BIGQUERY_TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.table_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const BIGQUERY_VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.view_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewAllowFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const BIGQUERY_VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.view_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewDenyFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};
