import { get } from 'lodash';

import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

// Most Workday extraction toggles default to true in the connector config, so
// reflect that in the form when the recipe does not set the field explicitly.
const getBooleanValueWithTrueDefault = (fieldPath: string) => (recipe: any) => {
    const value = get(recipe, fieldPath);
    if (value !== undefined && value !== null) {
        return value;
    }
    return true;
};

export const WORKDAY_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Services Host URL',
    helper: 'Workday services host base URL (no tenant or API path)',
    tooltip: 'Workday services host base URL, for example https://wd2-impl-services1.workday.com.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    placeholder: 'https://wd2-impl-services1.workday.com',
    required: true,
    rules: null,
};

export const WORKDAY_TENANT: RecipeField = {
    name: 'tenant',
    label: 'Tenant',
    helper: 'Workday tenant identifier',
    tooltip: 'Workday tenant identifier (the tenant segment of your Workday URLs).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.tenant',
    placeholder: 'your_tenant',
    required: true,
    rules: null,
};

export const WORKDAY_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    helper: 'OAuth 2.0 client ID (Client Credentials grant)',
    tooltip: "OAuth 2.0 client ID from the Workday 'Register API Client for Integrations' task.",
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    placeholder: 'MIM...',
    required: true,
    rules: null,
};

export const WORKDAY_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    helper: 'OAuth 2.0 client secret paired with the Client ID',
    tooltip: 'OAuth 2.0 client secret paired with the Client ID.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'client secret',
    required: true,
    rules: null,
};

export const WORKDAY_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    helper: 'Optional identifier for this Workday tenant instance',
    tooltip: 'Optional instance identifier (e.g. production). Useful when ingesting from multiple Workday tenants.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.platform_instance',
    placeholder: 'production',
    rules: null,
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const WORKDAY_TABLE_ALLOW: FilterRecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Prism tables',
    tooltip:
        'Only include specific Prism tables by name or Regular Expression (REGEX). If not provided, all tables are included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    placeholder: 'Worker_Headcount',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const WORKDAY_TABLE_DENY: FilterRecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Prism tables',
    tooltip:
        'Exclude specific Prism tables by name or Regular Expression (REGEX). Deny patterns always take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    placeholder: 'Worker_Headcount',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

const reportAllowFieldPath = 'source.config.report_pattern.allow';
export const WORKDAY_REPORT_ALLOW: FilterRecipeField = {
    name: 'report_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Workday-sourced reports',
    tooltip:
        'Only include specific Workday-sourced data sources/reports by name or Regular Expression (REGEX). If not provided, all are included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: reportAllowFieldPath,
    rules: null,
    section: 'Reports',
    filteringResource: 'Report',
    placeholder: 'Headcount by Organization',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, reportAllowFieldPath),
};

const reportDenyFieldPath = 'source.config.report_pattern.deny';
export const WORKDAY_REPORT_DENY: FilterRecipeField = {
    name: 'report_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Workday-sourced reports',
    tooltip:
        'Exclude specific Workday-sourced data sources/reports by name or Regular Expression (REGEX). Deny patterns always take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: reportDenyFieldPath,
    rules: null,
    section: 'Reports',
    filteringResource: 'Report',
    placeholder: 'Headcount by Organization',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, reportDenyFieldPath),
};

const extractTablesFieldPath = 'source.config.extract_tables';
export const WORKDAY_EXTRACT_TABLES: RecipeField = {
    name: 'extract_tables',
    label: 'Extract Tables',
    helper: 'Extract Prism tables as Datasets with schemas',
    tooltip: 'Whether to extract Prism tables as DataHub datasets with schemas.',
    type: FieldType.BOOLEAN,
    fieldPath: extractTablesFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractTablesFieldPath),
};

const extractDatasetsFieldPath = 'source.config.extract_datasets';
export const WORKDAY_EXTRACT_DATASETS: RecipeField = {
    name: 'extract_datasets',
    label: 'Extract Datasets',
    helper: 'Extract Prism dataset (pipeline) definitions',
    tooltip: 'Whether to extract Prism dataset (pipeline) definitions as DataHub datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: extractDatasetsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractDatasetsFieldPath),
};

const extractDataSourcesFieldPath = 'source.config.extract_data_sources';
export const WORKDAY_EXTRACT_DATA_SOURCES: RecipeField = {
    name: 'extract_data_sources',
    label: 'Extract Data Sources',
    helper: 'Extract Prism data sources as Datasets',
    tooltip: 'Whether to extract Prism data sources as DataHub datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: extractDataSourcesFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractDataSourcesFieldPath),
};

const extractReportsFieldPath = 'source.config.extract_reports';
export const WORKDAY_EXTRACT_REPORTS: RecipeField = {
    name: 'extract_reports',
    label: 'Extract Reports',
    helper: 'Extract Workday-sourced data sources as reports',
    tooltip:
        'Whether to extract Workday-sourced Prism data sources (RaaS custom reports and business-object sources) as report datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: extractReportsFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractReportsFieldPath),
};

const extractLineageFieldPath = 'source.config.extract_lineage';
export const WORKDAY_EXTRACT_LINEAGE: RecipeField = {
    name: 'extract_lineage',
    label: 'Extract Lineage',
    helper: 'Emit table-to-source and external upstream lineage',
    tooltip:
        'Whether to emit lineage from Prism tables to the datasets and data sources they derive from, and from external data sources to mapped warehouse datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: extractLineageFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(extractLineageFieldPath),
};

const includeExternalFieldPath = 'source.config.include_external_tables';
export const WORKDAY_INCLUDE_EXTERNAL: RecipeField = {
    name: 'include_external_tables',
    label: 'Include External Tables',
    helper: 'Include tables/data sources uploaded from outside Workday',
    tooltip: 'Whether to include tables/data sources with sourceType External (data uploaded from outside Workday).',
    type: FieldType.BOOLEAN,
    fieldPath: includeExternalFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(includeExternalFieldPath),
};

const ingestOwnerFieldPath = 'source.config.ingest_owner';
export const WORKDAY_INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Extract Owners',
    helper: 'Extract owner metadata',
    tooltip: 'Whether to map Workday createdBy/owner fields to DataHub ownership aspects.',
    type: FieldType.BOOLEAN,
    fieldPath: ingestOwnerFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(ingestOwnerFieldPath),
};

const verifySslFieldPath = 'source.config.verify_ssl';
export const WORKDAY_VERIFY_SSL: RecipeField = {
    name: 'verify_ssl',
    label: 'Verify SSL',
    helper: 'Verify SSL certificates for API calls',
    tooltip: 'Whether to verify SSL certificates for Workday API calls.',
    type: FieldType.BOOLEAN,
    fieldPath: verifySslFieldPath,
    rules: null,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(verifySslFieldPath),
};
