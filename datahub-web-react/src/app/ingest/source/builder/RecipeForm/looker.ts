import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const LOOKER_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
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
    tooltip: 'Looker API Client ID.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    placeholder: 'client_id',
    required: true,
    rules: null,
};

export const LOOKER_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'Looker API Client Secret.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'client_secret',
    required: true,
    rules: null,
};

const chartAllowFieldPath = 'source.config.chart_pattern.allow';
export const CHART_ALLOW: RecipeField = {
    name: 'chart_pattern.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific Charts by providing the name of a Chart, or a Regular Expresssion (REGEX). If not provided, all Charts will be included.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: chartAllowFieldPath,
    rules: null,
    section: 'Charts',
    placeholder: 'my_chart',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartAllowFieldPath),
};

const chartDenyFieldPath = 'source.config.chart_pattern.deny';
export const CHART_DENY: RecipeField = {
    name: 'chart_pattern.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific Charts by providing the name of a Charts, or a Regular Expresssion (REGEX). If not provided, all Charts will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: chartDenyFieldPath,
    rules: null,
    section: 'Charts',
    placeholder: 'my_chart',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartDenyFieldPath),
};
