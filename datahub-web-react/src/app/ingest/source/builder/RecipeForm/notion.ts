import { FieldType, RecipeField, setListValuesOnRecipe } from './common';

const apiKeyFieldPath = 'source.config.api_key';
export const NOTION_API_KEY: RecipeField = {
    name: 'api_key',
    label: 'Internal Integration Secret',
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
    type: FieldType.LIST,
    fieldPath: pageIdsFieldPath,
    placeholder: 'https://www.notion.so/Your-Page',
    buttonLabel: 'Add page URL',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, pageIdsFieldPath),
};
