import React from 'react';

import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';
import {
    AUTH_TYPE_AZURE_AD,
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_PAT,
    DatabricksFormValues,
    createDatabricksAuthValidator,
    getDatabricksAuthTypeFromRecipe,
    setDatabricksAuthTypeOnRecipe,
    shouldShowDatabricksField,
} from '@app/ingest/source/builder/RecipeForm/databricksAuth';

export const AUTHENTICATION_TYPE: RecipeField = {
    name: 'authentication_type',
    label: 'Authentication Type',
    tooltip: 'Choose the authentication method for connecting to Databricks.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.authentication_type',
    options: [
        { label: 'Personal Access Token', value: AUTH_TYPE_PAT },
        { label: 'OAuth M2M (Service Principal)', value: AUTH_TYPE_OAUTH_M2M },
        { label: 'Azure AD (Service Principal)', value: AUTH_TYPE_AZURE_AD },
    ],
    placeholder: 'Select authentication type',
    defaultValue: AUTH_TYPE_PAT,
    rules: null,
    required: true,
    setValueOnRecipeOverride: setDatabricksAuthTypeOnRecipe,
    getValueFromRecipeOverride: getDatabricksAuthTypeFromRecipe,
};

export const TOKEN: RecipeField = {
    name: 'token',
    label: 'Personal Access Token',
    tooltip: 'A personal access token associated with the Databricks account used to extract metadata.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.token',
    placeholder: 'dapi1a2b3c45d67890e1f234567a8bc9012d',
    required: true,
    rules: [createDatabricksAuthValidator(AUTH_TYPE_PAT, 'Token', 'Personal Access Token')],
    shouldShow: (formValues: DatabricksFormValues) => shouldShowDatabricksField('token', formValues),
};

export const CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'The client ID for a Databricks service principal. Used for OAuth M2M authentication.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    placeholder: '12345678-1234-1234-1234-123456789012',
    required: true,
    rules: [createDatabricksAuthValidator(AUTH_TYPE_OAUTH_M2M, 'Client ID', 'OAuth M2M')],
    shouldShow: (formValues: DatabricksFormValues) => shouldShowDatabricksField('client_id', formValues),
};

export const CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'The client secret for a Databricks service principal. Used for OAuth M2M authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'dose1234567890abcdef1234567890abcdef',
    required: true,
    rules: [createDatabricksAuthValidator(AUTH_TYPE_OAUTH_M2M, 'Client Secret', 'OAuth M2M')],
    shouldShow: (formValues: DatabricksFormValues) => shouldShowDatabricksField('client_secret', formValues),
};

export const AZURE_TENANT_ID: RecipeField = {
    name: 'azure_auth.tenant_id',
    label: 'Azure Tenant ID',
    tooltip: 'Azure tenant (directory) ID. This identifies the Azure AD tenant where the application is registered.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.azure_auth.tenant_id',
    placeholder: '12345678-1234-1234-1234-123456789012',
    required: true,
    rules: [createDatabricksAuthValidator(AUTH_TYPE_AZURE_AD, 'Azure Tenant ID', 'Azure AD')],
    shouldShow: (formValues: DatabricksFormValues) => shouldShowDatabricksField('azure_auth.tenant_id', formValues),
};

export const AZURE_CLIENT_ID: RecipeField = {
    name: 'azure_auth.client_id',
    label: 'Azure Client ID',
    tooltip: 'Azure application (client) ID. This is the unique identifier for the registered Azure AD application.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.azure_auth.client_id',
    placeholder: '12345678-1234-1234-1234-123456789012',
    required: true,
    rules: [createDatabricksAuthValidator(AUTH_TYPE_AZURE_AD, 'Azure Client ID', 'Azure AD')],
    shouldShow: (formValues: DatabricksFormValues) => shouldShowDatabricksField('azure_auth.client_id', formValues),
};

export const AZURE_CLIENT_SECRET: RecipeField = {
    name: 'azure_auth.client_secret',
    label: 'Azure Client Secret',
    tooltip: 'Azure application client secret used for authentication. This is a confidential credential.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.azure_auth.client_secret',
    placeholder: 'your-azure-client-secret',
    required: true,
    rules: [createDatabricksAuthValidator(AUTH_TYPE_AZURE_AD, 'Azure Client Secret', 'Azure AD')],
    shouldShow: (formValues: DatabricksFormValues) => shouldShowDatabricksField('azure_auth.client_secret', formValues),
};

export const WORKSPACE_URL: RecipeField = {
    name: 'workspace_url',
    label: 'Workspace URL',
    tooltip: 'The URL for the Databricks workspace from which to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.workspace_url',
    placeholder: 'https://abcsales.cloud.databricks.com',
    required: true,
    rules: null,
};

export const WAREHOUSE_ID: RecipeField = {
    name: 'warehouse_id',
    label: 'Warehouse Id',
    tooltip: 'The id of the warehouse to run queries. If not provided, we will use the default for the workspace.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse_id',
    placeholder: 'fab3e5ee0bcbfc56',
    required: true,
    rules: null,
};

export const INCLUDE_COLUMN_LINEAGE: RecipeField = {
    name: 'include_column_lineage',
    label: 'Include Column Lineage',
    tooltip: (
        <div>
            Extract Column Lineage from Unity Catalog. Note that this requires that your Databricks accounts meets
            certain requirements. View them{' '}
            <a href="https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements">here.</a>
            Enabling this feature may increase the duration of ingestion.
        </div>
    ),
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_column_lineage',
    rules: null,
};

const metastoreIdAllowFieldPath = 'source.config.metastore_id_pattern.allow';
export const UNITY_METASTORE_ID_ALLOW: RecipeField = {
    name: 'metastore_id_pattern.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific Metastores by providing the id of a Metastore, or a Regular Expression (REGEX) to include specific Metastores. If not provided, all Metastores will be included.',
    placeholder: '11111-2222-33333-44-555555',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: metastoreIdAllowFieldPath,
    rules: null,
    section: 'Metastores',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, metastoreIdAllowFieldPath),
};

const metastoreIdDenyFieldPath = 'source.config.metastore_id_pattern.deny';
export const UNITY_METASTORE_ID_DENY: RecipeField = {
    name: 'metastore_id_pattern.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific Metastores by providing the id of a Metastores, or a Regular Expression (REGEX). If not provided, all Metastores will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: '11111-2222-33333-44-555555',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: metastoreIdDenyFieldPath,
    rules: null,
    section: 'Metastores',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, metastoreIdDenyFieldPath),
};

const catalogAllowFieldPath = 'source.config.catalog_pattern.allow';
export const UNITY_CATALOG_ALLOW: RecipeField = {
    name: 'catalog_pattern.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific Catalogs by providing the name of a Catalog, or a Regular Expression (REGEX) to include specific Catalogs. If not provided, all Catalogs will be included.',
    placeholder: 'metastore.my_catalog',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: catalogAllowFieldPath,
    rules: null,
    section: 'Catalogs',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, catalogAllowFieldPath),
};

const catalogDenyFieldPath = 'source.config.catalog_pattern.deny';
export const UNITY_CATALOG_DENY: RecipeField = {
    name: 'catalog_pattern.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific Catalogs by providing the name of a Catalog, or a Regular Expression (REGEX) to exclude specific Catalogs. If not provided, all Catalogs will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'metastore.my_catalog',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: catalogDenyFieldPath,
    rules: null,
    section: 'Catalogs',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, catalogDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const UNITY_TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific Tables by providing the fully-qualified name of a Table, or a Regular Expression (REGEX) to include specific Tables. If not provided, all Tables will be included.',
    placeholder: 'catalog.schema.table',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const UNITY_TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific Tables by providing the fully-qualified name of a Table, or a Regular Expression (REGEX) to exclude specific Tables. If not provided, all Tables will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'catalog.schema.table',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};
