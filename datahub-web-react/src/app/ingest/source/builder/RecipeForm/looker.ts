import { RecipeField, FieldType } from './common';

export const LOOKER_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    tooltip:
        'Url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar.Used for making API calls to Looker and constructing clickable dashboard and chart urls.',
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
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_secret',
    rules: null,
};
