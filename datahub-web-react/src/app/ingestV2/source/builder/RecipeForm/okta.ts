import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const OKTA_DOMAIN_URL: RecipeField = {
    name: 'okta_domain',
    label: 'Okta Domain URL',
    helper: 'Location of your Okta Domain',
    tooltip: 'The location of your Okta Domain, without a protocol.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.okta_domain',
    placeholder: 'dev-35531955.okta.com',
    required: true,
    rules: null,
};

export const OKTA_API_TOKEN: RecipeField = {
    name: 'credential.project_id',
    label: 'Token',
    helper: 'API token for DataHub app',
    tooltip: 'An API token generated for the DataHub application inside your Okta Developer Console.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.okta_api_token',
    placeholder: 'd0121d0000882411234e11166c6aaa23ed5d74e0',
    rules: null,
    required: true,
};

export const PROFILE_TO_USER: RecipeField = {
    name: 'email',
    label: 'Okta Email',
    helper: 'Okta User Profile attribute',
    tooltip:
        'Which Okta User Profile attribute to use as input to DataHub username mapping. Common values used are - login, email.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.okta_profile_to_username_attr',
    placeholder: 'email',
    rules: null,
};

export const PROFILE_TO_GROUP: RecipeField = {
    name: 'okta_profile_to_group_name_attr',
    label: 'Okta Profile to group name attribute',
    helper: 'Okta Group Profile attribute',
    tooltip: 'Which Okta Group Profile attribute to use as input to DataHub group name mapping.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.okta_profile_to_group_name_attr',
    placeholder: 'Group name',
    rules: null,
};

const schemaAllowFieldPath = 'source.config.okta_profile_to_username_regex.allow';
export const PROFILE_TO_USER_REGX_ALLOW: FilterRecipeField = {
    name: 'user.allow',
    label: 'Allow Patterns',
    helper: 'Include specific schemas',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Okta Profile To User Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.okta_profile_to_username_regex.deny';
export const PROFILE_TO_USER_REGEX_DENY: FilterRecipeField = {
    name: 'user.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific schemas',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Okta Profile To User Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const schemaAllowFieldPathForGroup = 'source.config.okta_profile_to_group_name_regex.allow';
export const PROFILE_TO_GROUP_REGX_ALLOW: FilterRecipeField = {
    name: 'group.allow',
    label: 'Allow Patterns',
    helper: 'Include specific schemas',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'group_pattern',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPathForGroup,
    rules: null,
    section: 'Okta Profile To Group Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPathForGroup),
};

const schemaDenyFieldPathForGroup = 'source.config.okta_profile_to_group_name_regex.deny';
export const PROFILE_TO_GROUP_REGX_DENY: FilterRecipeField = {
    name: 'group.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific schemas',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'group_pattern',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPathForGroup,
    rules: null,
    section: 'Okta Profile To Group Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPathForGroup),
};
export const INGEST_USERS: RecipeField = {
    name: 'ingest_users',
    label: 'Ingest Users',
    helper: 'Ingest users into DataHub',
    tooltip: 'Whether users should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_users',
    rules: null,
};

export const INGEST_GROUPS: RecipeField = {
    name: 'ingest_groups',
    label: 'Ingest Groups',
    helper: 'Ingest groups into DataHub',
    tooltip: 'Whether groups should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_groups',
    rules: null,
};

export const INCLUDE_DEPROVISIONED_USERS: RecipeField = {
    name: 'include_deprovisioned_users',
    label: 'Include deprovisioned users',
    helper: 'Ingest DEPROVISIONED state users',
    tooltip: 'Whether to ingest users in the DEPROVISIONED state from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_deprovisioned_users',
    rules: null,
};
export const INCLUDE_SUSPENDED_USERS: RecipeField = {
    name: 'include_suspended_users',
    label: 'Include suspended users',
    helper: 'Ingest SUSPENDED state users',
    tooltip: 'Whether to ingest users in the SUSPENDED state from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_suspended_users',
    rules: null,
};

export const SKIP_USERS_WITHOUT_GROUP: RecipeField = {
    name: 'skip_users_without_a_group',
    label: 'Skip users without group',
    helper: 'Skip users without group',
    tooltip: 'Whether to skip users without group from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.skip_users_without_a_group',
    rules: null,
};
