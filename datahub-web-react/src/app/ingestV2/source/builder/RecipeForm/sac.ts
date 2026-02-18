import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const SAC_TENANT_URL: RecipeField = {
    name: 'tenant_url',
    label: 'Tenant URL',
    helper: 'SAP Analytics Cloud tenant URL',
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
    helper: 'OAuth 2.0 Token Service URL',
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
    helper: 'Client ID for authentication',
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
    helper: 'Client Secret for authentication',
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
    helper: 'Ingest stories into DataHub',
    tooltip: 'Whether stories should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_stories',
    rules: null,
};

export const INGEST_APPLICATIONS: RecipeField = {
    name: 'ingest_applications',
    label: 'Ingest Applications',
    helper: 'Ingest applications into DataHub',
    tooltip: 'Whether applications should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_applications',
    rules: null,
};

const resourceIdAllowFieldPath = 'source.config.resource_id_pattern.allow';
export const RESOURCE_ID_ALLOW: FilterRecipeField = {
    name: 'resource_id_pattern.allow',
    label: 'Resource Id Allow Patterns',
    helper: 'Include specific Stories and Apps',
    tooltip:
        'Only include specific Stories and Applications by providing the id of the ressource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: resourceIdAllowFieldPath,
    rules: null,
    section: 'Stories and Applications',
    filteringResource: 'Resource ID',
    placeholder: 'LXTH4JCE36EOYLU41PIINLYPU9XRYM26',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceIdAllowFieldPath),
};

const resourceIdDenyFieldPath = 'source.config.resource_id_pattern.deny';
export const RESOURCE_ID_DENY: FilterRecipeField = {
    name: 'resource_id_pattern.deny',
    label: 'Resource Id Deny Patterns',
    helper: 'Exclude specific Stories and Apps',
    tooltip:
        'Exclude specific Stories and Applications by providing the id of the resource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: resourceIdDenyFieldPath,
    rules: null,
    section: 'Stories and Applications',
    filteringResource: 'Resource ID',
    placeholder: 'LXTH4JCE36EOYLU41PIINLYPU9XRYM26',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceIdDenyFieldPath),
};

const resourceNameAllowFieldPath = 'source.config.resource_name_pattern.allow';
export const RESOURCE_NAME_ALLOW: FilterRecipeField = {
    name: 'resource_name_pattern.allow',
    label: 'Resource Name Allow Patterns',
    helper: 'Include specific Stories and Apps',
    tooltip:
        'Only include specific Stories and Applications by providing the name of the ressource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: resourceNameAllowFieldPath,
    rules: null,
    section: 'Stories and Applications',
    filteringResource: 'Resource Name',
    placeholder: 'Name of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceNameAllowFieldPath),
};

const resourceNameDenyFieldPath = 'source.config.resource_name_pattern.deny';
export const RESOURCE_NAME_DENY: FilterRecipeField = {
    name: 'resource_name_pattern.deny',
    label: 'Resource Name Deny Patterns',
    helper: 'Exclude specific Stories and Apps',
    tooltip:
        'Exclude specific Stories and Applications by providing the name of the resource, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: resourceNameDenyFieldPath,
    rules: null,
    section: 'Stories and Applications',
    filteringResource: 'Resource Name',
    placeholder: 'Name of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, resourceNameDenyFieldPath),
};

const folderAllowFieldPath = 'source.config.resource_id_pattern.allow';
export const FOLDER_ALLOW: FilterRecipeField = {
    name: 'folder_pattern.allow',
    label: 'Folder Allow Patterns',
    helper: 'Include specific Stories and Apps',
    tooltip:
        'Only include specific Stories and Applications by providing the folder containing the resources, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: folderAllowFieldPath,
    rules: null,
    section: 'Stories and Applications',
    placeholder: 'Folder of the story',
    filteringResource: 'Folder',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderAllowFieldPath),
};

const folderDenyFieldPath = 'source.config.folder_pattern.deny';
export const FOLDER_DENY: FilterRecipeField = {
    name: 'folder_pattern.deny',
    label: 'Folder Deny Patterns',
    helper: 'Exclude specific Stories and Apps',
    tooltip:
        'Exclude specific Stories and Applications by providing the folder containing the resources, or a Regular Expression (REGEX). If not provided, all Stories and Applications will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: folderDenyFieldPath,
    rules: null,
    section: 'Stories and Applications',
    filteringResource: 'Folder',
    placeholder: 'Folder of the story',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, folderDenyFieldPath),
};
