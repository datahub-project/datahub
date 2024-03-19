import { validateURL } from '../../utils';
import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const AZURE_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Application ID. Found in your app registration on Azure AD Portal',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    placeholder: '00000000-0000-0000-0000-000000000000',
    required: true,
    rules: null,
};

export const AZURE_TENANT_ID: RecipeField = {
    name: 'tenant_id',
    label: 'Tenant ID',
    tooltip: 'Directory ID. Found in your app registration on Azure AD Portal',
    type: FieldType.TEXT,
    fieldPath: 'source.config.tenant_id',
    placeholder: '00000000-0000-0000-0000-000000000000',
    required: true,
    rules: null,
};

export const AZURE_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'The Azure client secret.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: '00000000-0000-0000-0000-000000000000',
    required: true,
    rules: null,
};

export const AZURE_REDIRECT_URL: RecipeField = {
    name: 'redirect',
    label: 'Redirect URL',
    tooltip: 'Redirect URL. Found in your app registration on Azure AD Portal.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.redirect',
    placeholder: 'https://login.microsoftonline.com/common/oauth2/nativeclient',
    required: true,
    rules: [() => validateURL('Redirect URI')],
};

export const AZURE_AUTHORITY_URL: RecipeField = {
    name: 'authority',
    label: 'Authority URL',
    tooltip: 'Is a URL that indicates a directory that MSAL can request tokens from..',
    type: FieldType.TEXT,
    fieldPath: 'source.config.authority',
    placeholder: 'https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000',
    required: true,
    rules: [() => validateURL('Azure authority URL')],
};

export const AZURE_TOKEN_URL: RecipeField = {
    name: 'token_url',
    label: 'Token URL',
    tooltip:
        'The token URL that acquires a token from Azure AD for authorizing requests. This source will only work with v1.0 endpoint.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.token_url',
    placeholder: 'https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/token',
    required: true,
    rules: [() => validateURL('Azure token URL')],
};

export const AZURE_GRAPH_URL: RecipeField = {
    name: 'graph_url',
    label: 'Graph URL',
    tooltip: 'Microsoft Graph API endpoint',
    type: FieldType.TEXT,
    fieldPath: 'source.config.graph_url',
    placeholder: 'https://graph.microsoft.com/v1.0',
    required: true,
    rules: [() => validateURL('Graph url URL')],
};

export const AZURE_INGEST_USERS: RecipeField = {
    name: 'ingest_users',
    label: 'Ingest Users',
    tooltip: 'Flag to determine whether to ingest users from Azure AD or not.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_users',
    rules: null,
};

export const AZURE_INGEST_GROUPS: RecipeField = {
    name: 'ingest_groups',
    label: 'Ingest Groups',
    tooltip: 'Flag to determine whether to ingest groups from Azure AD or not.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_groups',
    rules: null,
};

const schemaAllowFieldPathGroup = 'source.config.groups_pattern.allow';
export const GROUP_ALLOW: RecipeField = {
    name: 'groups.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'group_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPathGroup,
    rules: null,
    section: 'Group',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPathGroup),
};

const schemaDenyFieldPathGroup = 'source.config.groups_pattern.deny';
export const GROUP_DENY: RecipeField = {
    name: 'groups.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific schemas by providing the name of a schema, or a regular expression (regex). If not provided, all schemas inside allowed databases will be included. Deny patterns always take precedence over allow patterns.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPathGroup,
    rules: null,
    section: 'Group',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPathGroup),
};

const schemaAllowFieldPathUser = 'source.config.users_pattern.allow';
export const USER_ALLOW: RecipeField = {
    name: 'user.allow',
    label: 'Allow Patterns',
    tooltip:
        'Exclude specific schemas by providing the name of a schema, or a regular expression (regex). If not provided, all schemas inside allowed databases will be included. Deny patterns always take precedence over allow patterns.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPathUser,
    rules: null,
    section: 'User',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPathUser),
};

const schemaDenyFieldPathUser = 'source.config.users_pattern.deny';
export const USER_DENY: RecipeField = {
    name: 'user.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific schemas by providing the name of a schema, or a regular expression (regex). If not provided, all schemas inside allowed databases will be included. Deny patterns always take precedence over allow patterns.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPathUser,
    rules: null,
    section: 'User',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPathUser),
};

export const SKIP_USERS_WITHOUT_GROUP: RecipeField = {
    name: 'skip_users_without_a_group',
    label: 'Skip users without group',
    tooltip: 'Whether to skip users without group from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.skip_users_without_a_group',
    rules: null,
};
