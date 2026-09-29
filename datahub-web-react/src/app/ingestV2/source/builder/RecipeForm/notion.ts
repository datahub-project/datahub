import React from 'react';

import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

const apiKeyFieldPath = 'source.config.api_key';
export const NOTION_API_KEY: RecipeField = {
    name: 'api_key',
    label: 'Internal Integration Secret',
    helper: React.createElement(
        React.Fragment,
        null,
        'Find your Integration Secret in ',
        React.createElement(
            'a',
            { href: 'https://notion.com/my-integrations', target: '_blank', rel: 'noreferrer' },
            'Notion Integrations',
        ),
        '.',
    ),
    tooltip: 'Create an internal Notion integration and copy the Internal Integration Secret.',
    type: FieldType.SECRET,
    fieldPath: apiKeyFieldPath,
    placeholder: 'secret_XXXXXXXXXXXXXXXXXXXXXXXX',
    rules: null,
    required: true,
};

const pageIdsFieldPath = 'source.config.page_ids';
export const NOTION_PAGE_IDS: RecipeField = {
    name: 'page_ids',
    label: 'Page URLs',
    tooltip: 'Optional. Add page URLs to limit ingestion. Parent pages act like folders and include children.',
    helper: 'Optional. Leave empty to ingest all pages your integration has access to.',
    type: FieldType.LIST,
    fieldPath: pageIdsFieldPath,
    placeholder: 'https://www.notion.so/Your-Page',
    buttonLabel: 'Add page URL',
    rules: null,
    required: false,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, pageIdsFieldPath),
};
