import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

const accessTokenFieldPath = 'source.config.access_token';
export const QUIP_ACCESS_TOKEN: RecipeField = {
    name: 'access_token',
    label: 'Access Token',
    tooltip: 'Quip Personal Access Token. Generate one at <base_url>/dev/token (e.g. https://quip.com/dev/token).',
    type: FieldType.SECRET,
    fieldPath: accessTokenFieldPath,
    placeholder: 'XXXXXXXXXXXXXXXXXXXXXXXX',
    rules: null,
    required: true,
};

const baseUrlFieldPath = 'source.config.base_url';
export const QUIP_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    tooltip:
        'Base URL of the Quip Automation API. Use https://platform.quip.com for quip.com, or your enterprise endpoint (e.g. https://platform.quip-amazon.com).',
    type: FieldType.TEXT,
    fieldPath: baseUrlFieldPath,
    placeholder: 'https://platform.quip.com',
    rules: null,
    required: true,
};

const folderIdsFieldPath = 'source.config.folder_ids';
export const QUIP_FOLDER_IDS: RecipeField = {
    name: 'folder_ids',
    label: 'Folder IDs',
    tooltip:
        'Quip folder IDs to ingest (crawled recursively). Leave empty to auto-discover the token owner\u2019s accessible folders.',
    type: FieldType.LIST,
    fieldPath: folderIdsFieldPath,
    placeholder: 'AbCdEfGhIjKl',
    buttonLabel: 'Add folder',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderIdsFieldPath),
};

const threadIdsFieldPath = 'source.config.thread_ids';
export const QUIP_THREAD_IDS: RecipeField = {
    name: 'thread_ids',
    label: 'Thread IDs',
    tooltip: 'Specific Quip thread (document) IDs to ingest in addition to any folders.',
    type: FieldType.LIST,
    fieldPath: threadIdsFieldPath,
    placeholder: 'MnOpQrStUvWx',
    buttonLabel: 'Add thread',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, threadIdsFieldPath),
};
