import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const SAC_TENANT_URL: RecipeField = {
    name: 'tenant_url',
    label: 'Tenant URL',
    tooltip: 'The URL of the SAP Analytics Cloud tenant.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.tenant_url',
    placeholder: 'https://company.eu10.sapanalytics.cloud',
    required: true,
    rules: null,
};

export const SAC_TOKEN_URL: RecipeField = {
    name: 'token_url',
    label: 'Token URL',
    tooltip: 'The OAuth 2.0 Token Service URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.token_url',
    placeholder: 'https://company.eu10.hana.ondemand.com/oauth/token',
    required: true,
    rules: null,
};

export const SAC_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Client ID.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_id',
    placeholder: 'client_id',
    required: true,
    rules: null,
};

export const SAC_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'Client Secret.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'client_secret',
    required: true,
    rules: null,
};

export const INGEST_STORIES: RecipeField = {
    name: 'ingest_stories',
    label: 'Ingest Stories',
    tooltip: 'Whether stories should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_stories',
    rules: null,
    section: 'Stories and Applications',
};

export const INGEST_APPLICATIONS: RecipeField = {
    name: 'ingest_applications',
    label: 'Ingest Applications',
    tooltip: 'Whether applications should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_applications',
    rules: null,
    section: 'Stories and Applications',
};

const resourceIdAllowFieldPath = 'source.config.resource_id_pattern.allow';
export const RESOURCE_ID_ALLOW: RecipeField = {
    name: 'resource_id_pattern.allow',
    label: 'Resource Id Allow Patterns',
    tooltip:
        'Only include specific Stories and Applications by providing the id of the ressource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: resourceIdAllowFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'LXTH4JCE36EOYLU41PIINLYPU9XRYM26',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceIdAllowFieldPath),
};

const resourceIdDenyFieldPath = 'source.config.resource_id_pattern.deny';
export const RESOURCE_ID_DENY: RecipeField = {
    name: 'resource_id_pattern.deny',
    label: 'Resource Id Deny Patterns',
    tooltip:
        'Exclude specific Stories and Applications by providing the id of the resource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: resourceIdDenyFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'LXTH4JCE36EOYLU41PIINLYPU9XRYM26',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceIdDenyFieldPath),
};

const resourceNameAllowFieldPath = 'source.config.resource_id_pattern.allow';
export const RESOURCE_NAME_ALLOW: RecipeField = {
    name: 'resource_name_pattern.allow',
    label: 'Resource Name Allow Patterns',
    tooltip:
        'Only include specific Stories and Applications by providing the name of the ressource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: resourceNameAllowFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'Name of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceNameAllowFieldPath),
};

const resourceNameDenyFieldPath = 'source.config.resource_name_pattern.deny';
export const RESOURCE_NAME_DENY: RecipeField = {
    name: 'resource_name_pattern.deny',
    label: 'Resource Name Deny Patterns',
    tooltip:
        'Exclude specific Stories and Applications by providing the name of the resource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: resourceNameDenyFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'Name of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceNameDenyFieldPath),
};

const folderAllowFieldPath = 'source.config.resource_id_pattern.allow';
export const FOLDER_ALLOW: RecipeField = {
    name: 'folder_pattern.allow',
    label: 'Folder Allow Patterns',
    tooltip:
        'Only include specific Stories and Applications by providing the folder containing the resources, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: folderAllowFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'Folder of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderAllowFieldPath),
};

const folderDenyFieldPath = 'source.config.folder_pattern.deny';
export const FOLDER_DENY: RecipeField = {
    name: 'folder_pattern.deny',
    label: 'Folder Deny Patterns',
    tooltip:
        'Exclude specific Stories and Applications by providing the folder containing the resources, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: folderDenyFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'Folder of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderDenyFieldPath),
};
