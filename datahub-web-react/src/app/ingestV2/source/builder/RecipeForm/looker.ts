import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const LOOKER_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    helper: 'URL where Looker instance is hosted',
    tooltip: 'The URL where your Looker instance is hosted.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    placeholder: 'https://looker.company.com',
    required: true,
    rules: null,
};

export const LOOKER_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    helper: 'Looker API Client ID',
    tooltip: 'Looker API Client ID.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_id',
    placeholder: 'client_id',
    required: true,
    rules: null,
};

export const LOOKER_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    helper: 'Looker API Client Secret',
    tooltip: 'Looker API Client Secret.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'client_secret',
    required: true,
    rules: null,
};

const chartAllowFieldPath = 'source.config.chart_pattern.allow';
export const CHART_ALLOW: FilterRecipeField = {
    name: 'chart_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Charts',
    tooltip:
        'Only include specific Charts by providing the numeric id of a Chart, or a Regular Expression (REGEX). If not provided, all Charts will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: chartAllowFieldPath,
    rules: null,
    section: 'Charts',
    filteringResource: 'Chart',
    placeholder: '12',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartAllowFieldPath),
};

const chartDenyFieldPath = 'source.config.chart_pattern.deny';
export const CHART_DENY: FilterRecipeField = {
    name: 'chart_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Charts',
    tooltip:
        'Exclude specific Charts by providing the numeric id of a Chart, or a Regular Expression (REGEX). If not provided, all Charts will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: chartDenyFieldPath,
    rules: null,
    section: 'Charts',
    filteringResource: 'Chart',
    placeholder: '12',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartDenyFieldPath),
};

const dashboardAllowFieldPath = 'source.config.dashboard_pattern.allow';
export const DASHBOARD_ALLOW: FilterRecipeField = {
    name: 'dashboard_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Dashboards',
    tooltip:
        'Only include specific Dashboards by providing the numeric id of a Dashboard, or a Regular Expression (REGEX). If not provided, all Dashboards will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardAllowFieldPath,
    rules: null,
    section: 'Dashboards',
    filteringResource: 'Dashboard',
    placeholder: '1232',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardAllowFieldPath),
};

const dashboardDenyFieldPath = 'source.config.dashboard_pattern.deny';
export const DASHBOARD_DENY: FilterRecipeField = {
    name: 'dashboard_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Dashboards',
    tooltip:
        'Exclude specific Dashboards by providing the numeric id of a Dashboard, or a Regular Expression (REGEX). If not provided, all Dashboards will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardDenyFieldPath,
    rules: null,
    section: 'Dashboards',
    filteringResource: 'Dashboard',
    placeholder: '1232',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardDenyFieldPath),
};
