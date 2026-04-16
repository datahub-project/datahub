import React from 'react';

import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const SHAREPOINT_MODE: RecipeField = {
    name: 'mode',
    label: 'Ingestion Mode',
    helper: React.createElement(
        React.Fragment,
        null,
        React.createElement('strong', null, 'Data Lake'),
        ' — ingest structured files (CSV, Parquet, Excel, etc.) as Datasets. ',
        React.createElement('strong', null, 'Document'),
        ' — ingest SharePoint pages and Office files as Documents with optional semantic search. ',
        React.createElement('strong', null, 'Both'),
        ' — run both in a single pipeline.',
    ),
    tooltip:
        "Select 'Data Lake' to ingest structured files as Datasets, 'Document' to ingest pages and Office files as Documents, or 'Both' to run both in a single pipeline.",
    type: FieldType.SELECT,
    fieldPath: 'source.config.mode',
    options: [
        { label: 'Data Lake', value: 'data_lake' },
        { label: 'Document', value: 'document' },
        { label: 'Both', value: 'both' },
    ],
    rules: [{ required: true, message: 'Ingestion mode is required' }],
    required: true,
};

export const SHAREPOINT_TENANT_ID: RecipeField = {
    name: 'tenant_id',
    label: 'Tenant ID',
    helper: 'Azure AD tenant ID for the registered application.',
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
    helper: 'Azure AD client (application) ID for the registered application.',
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
    helper: React.createElement(
        React.Fragment,
        null,
        'Azure AD client secret. Create one in ',
        React.createElement(
            'a',
            {
                href: 'https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade',
                target: '_blank',
                rel: 'noreferrer',
            },
            'Azure App Registrations',
        ),
        ' under Certificates & Secrets.',
    ),
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
    helper: React.createElement(
        React.Fragment,
        null,
        "Your organisation's SharePoint hostname. Example: ",
        React.createElement('code', null, 'myorg.sharepoint.com'),
    ),
    tooltip: "Your organisation's SharePoint hostname, e.g. myorg.sharepoint.com",
    type: FieldType.TEXT,
    fieldPath: 'source.config.site.hostname',
    placeholder: 'myorg.sharepoint.com',
    required: true,
    rules: [{ required: true, message: 'SharePoint hostname is required' }],
};

const siteAllowFieldPath = 'source.config.site.site_pattern.allow';
export const SHAREPOINT_SITE_ALLOW: FilterRecipeField = {
    name: 'site_pattern.allow',
    label: 'Include',
    tooltip: 'Site URL paths to include. Leave empty to ingest all accessible sites.',
    helper: React.createElement(
        React.Fragment,
        null,
        'Specify site paths to include (e.g., ',
        React.createElement('code', null, '/sites/Engineering'),
        '). Leave empty to ingest all accessible sites.',
    ),
    type: FieldType.LIST,
    fieldPath: siteAllowFieldPath,
    placeholder: '/sites/Engineering',
    buttonLabel: 'Add site',
    rules: null,
    section: 'Sites',
    filteringResource: 'Site',
    rule: FilterRule.INCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, siteAllowFieldPath),
};

const siteDenyFieldPath = 'source.config.site.site_pattern.deny';
export const SHAREPOINT_SITE_DENY: FilterRecipeField = {
    name: 'site_pattern.deny',
    label: 'Exclude',
    tooltip: 'Site URL paths to exclude. Exclude always takes precedence over Include.',
    helper: React.createElement(
        React.Fragment,
        null,
        'Exclude specific sites by path (e.g., ',
        React.createElement('code', null, '/sites/Archive'),
        '). Exclude always takes precedence over Include.',
    ),
    type: FieldType.LIST,
    fieldPath: siteDenyFieldPath,
    placeholder: '/sites/Archive',
    buttonLabel: 'Add site',
    rules: null,
    section: 'Sites',
    filteringResource: 'Site',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, siteDenyFieldPath),
};
