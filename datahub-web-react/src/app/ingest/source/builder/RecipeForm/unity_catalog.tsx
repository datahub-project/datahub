import React from 'react';
import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const UNITY_CATALOG = 'unity-catalog';

export const TOKEN: RecipeField = {
    name: 'token',
    label: 'Token',
    tooltip: 'A personal access token associated with the Databricks account used to extract metadata.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.token',
    placeholder: 'dapi1a2b3c45d67890e1f234567a8bc9012d',
    required: true,
    rules: null,
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

export const INCLUDE_TABLE_LINEAGE: RecipeField = {
    name: 'include_table_lineage',
    label: 'Include Table Lineage',
    tooltip: (
        <div>
            Extract Table Lineage from Unity Catalog. Note that this requires that your Databricks accounts meets
            certain requirements. View them{' '}
            <a href="https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements">here</a>
        </div>
    ),
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_table_lineage',
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
    placeholder: 'my_catalog',
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
    name: 'catalog_pattern.allow',
    label: 'Allow Patterns',
    tooltip:
        'Exclude specific Catalogs by providing the name of a Catalog, or a Regular Expression (REGEX) to exclude specific Catalogs. If not provided, all Catalogs will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'my_catalog',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: catalogDenyFieldPath,
    rules: null,
    section: 'Catalogs',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, catalogDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.metastore_id_pattern.allow';
export const UNITY_TABLE_ALLOW: RecipeField = {
    name: 'catalog_pattern.allow',
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

const tableDenyFieldPath = 'source.config.metastore_id_pattern.deny';
export const UNITY_TABLE_DENY: RecipeField = {
    name: 'catalog_pattern.allow',
    label: 'Allow Patterns',
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
