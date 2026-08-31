import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

const urlFieldPath = 'source.config.url';
export const CONFLUENCE_URL: RecipeField = {
    name: 'url',
    label: 'Confluence URL',
    tooltip: 'The base URL for your Confluence instance, e.g. https://your-domain.atlassian.net/wiki',
    type: FieldType.TEXT,
    fieldPath: urlFieldPath,
    placeholder: 'https://your-domain.atlassian.net/wiki',
    rules: null,
    required: true,
};

const usernameFieldPath = 'source.config.username';
export const CONFLUENCE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Email Address',
    tooltip: 'Email address associated with your Atlassian Cloud account.',
    type: FieldType.TEXT,
    fieldPath: usernameFieldPath,
    placeholder: 'user@example.com',
    rules: null,
};

const apiTokenFieldPath = 'source.config.api_token';
export const CONFLUENCE_API_TOKEN: RecipeField = {
    name: 'api_token',
    label: 'API Token',
    tooltip:
        'API token for Confluence Cloud authentication. Create one at https://id.atlassian.com/manage-profile/security/api-tokens',
    type: FieldType.SECRET,
    fieldPath: apiTokenFieldPath,
    placeholder: 'ATATT3xFfGF0...',
    rules: null,
};

const spaceAllowFieldPath = 'source.config.space_allow';
export const CONFLUENCE_SPACE_ALLOW: RecipeField = {
    name: 'space_allow',
    label: 'Allow Spaces',
    tooltip: 'Space keys or URLs to include. Leave empty to auto-discover all accessible spaces.',
    type: FieldType.LIST,
    fieldPath: spaceAllowFieldPath,
    placeholder: 'ENGINEERING',
    buttonLabel: 'Add space',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, spaceAllowFieldPath),
};

const spaceDenyFieldPath = 'source.config.space_deny';
export const CONFLUENCE_SPACE_DENY: RecipeField = {
    name: 'space_deny',
    label: 'Deny Spaces',
    tooltip: 'Space keys or URLs to exclude. Useful for excluding personal spaces or archived content.',
    type: FieldType.LIST,
    fieldPath: spaceDenyFieldPath,
    placeholder: '~johndoe',
    buttonLabel: 'Add space',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, spaceDenyFieldPath),
};

const pageAllowFieldPath = 'source.config.page_allow';
export const CONFLUENCE_PAGE_ALLOW: RecipeField = {
    name: 'page_allow',
    label: 'Allow Pages',
    tooltip: 'Page IDs or URLs to include. When specified, only these pages and their children will be ingested.',
    type: FieldType.LIST,
    fieldPath: pageAllowFieldPath,
    placeholder: '123456',
    buttonLabel: 'Add page',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, pageAllowFieldPath),
};

const pageDenyFieldPath = 'source.config.page_deny';
export const CONFLUENCE_PAGE_DENY: RecipeField = {
    name: 'page_deny',
    label: 'Deny Pages',
    tooltip: 'Page IDs or URLs to exclude from ingestion.',
    type: FieldType.LIST,
    fieldPath: pageDenyFieldPath,
    placeholder: '999999',
    buttonLabel: 'Add page',
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, pageDenyFieldPath),
};
