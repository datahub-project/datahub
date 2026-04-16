import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

export const SHAREPOINT_MODE: RecipeField = {
    name: 'mode',
    label: 'Ingestion Mode',
    tooltip:
        "Select 'Data Lake' to ingest structured files as Datasets, 'Document' to ingest pages and Office files as Documents, or 'Both' to run both in a single pipeline.",
    type: FieldType.SELECT,
    fieldPath: 'source.config.mode',
    options: [
        { label: 'Data Lake', value: 'data_lake' },
        { label: 'Document', value: 'document' },
        { label: 'Both', value: 'both' },
    ],
    rules: null,
    required: true,
};

export const SHAREPOINT_TENANT_ID: RecipeField = {
    name: 'tenant_id',
    label: 'Tenant ID',
    tooltip: 'Azure AD tenant ID for the registered application.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.auth.tenant_id',
    placeholder: 'a949d688-67c0-4bf1-a344-e939411c6c0a',
    required: true,
    rules: null,
};

export const SHAREPOINT_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Azure AD client (application) ID for the registered application.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.auth.client_id',
    placeholder: '00000000-0000-0000-0000-000000000000',
    required: true,
    rules: null,
};

export const SHAREPOINT_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'Azure AD client secret for the registered application.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.auth.client_secret',
    placeholder: 'client secret',
    required: true,
    rules: null,
};

export const SHAREPOINT_HOSTNAME: RecipeField = {
    name: 'hostname',
    label: 'SharePoint Hostname',
    tooltip: "Your organisation's SharePoint hostname, e.g. myorg.sharepoint.com",
    type: FieldType.TEXT,
    fieldPath: 'source.config.site.hostname',
    placeholder: 'myorg.sharepoint.com',
    required: true,
    rules: null,
};

const siteAllowFieldPath = 'source.config.site.site_pattern.allow';
export const SHAREPOINT_SITE_ALLOW: RecipeField = {
    name: 'site_pattern.allow',
    label: 'Allow Sites',
    tooltip: 'Site URL paths to include. Leave empty to ingest all accessible sites.',
    type: FieldType.LIST,
    fieldPath: siteAllowFieldPath,
    placeholder: '/sites/Engineering',
    buttonLabel: 'Add site',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, siteAllowFieldPath),
};

const siteDenyFieldPath = 'source.config.site.site_pattern.deny';
export const SHAREPOINT_SITE_DENY: RecipeField = {
    name: 'site_pattern.deny',
    label: 'Deny Sites',
    tooltip: 'Site URL paths to exclude.',
    type: FieldType.LIST,
    fieldPath: siteDenyFieldPath,
    placeholder: '/sites/Archive',
    buttonLabel: 'Add site',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, siteDenyFieldPath),
};
