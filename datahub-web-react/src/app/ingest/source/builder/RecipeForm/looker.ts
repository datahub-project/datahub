import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const LOOKER_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    tooltip:
        'Url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    rules: null,
};

export const LOOKER_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Looker API client id.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    rules: null,
};

export const LOOKER_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'Looker API client secret.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    rules: null,
};

const chartAllowFieldPath = 'source.config.chart_pattern.allow';
export const CHART_ALLOW: RecipeField = {
    name: 'chart_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: chartAllowFieldPath,
    rules: null,
    section: 'Charts',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartAllowFieldPath),
};

const chartDenyFieldPath = 'source.config.chart_pattern.deny';
export const CHART_DENY: RecipeField = {
    name: 'chart_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: chartDenyFieldPath,
    rules: null,
    section: 'Charts',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, chartDenyFieldPath),
};
