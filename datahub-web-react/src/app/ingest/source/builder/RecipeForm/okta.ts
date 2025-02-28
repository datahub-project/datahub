import { validateURL } from '../../utils';
import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const OKTA_DOMAIN_URL: RecipeField = {
    name: 'okta_domain',
    label: 'Okta Domain URL',
    tooltip: 'The location of your Okta Domain, without a protocol.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.okta_domain',
    placeholder: 'dev-35531955.okta.com',
    required: true,
    rules: [() => validateURL('Okta Domain URL')],
};

export const OKTA_API_TOKEN: RecipeField = {
    name: 'credential.project_id',
    label: 'Token',
    tooltip: 'An API token generated for the DataHub application inside your Okta Developer Console.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.okta_api_token',
    placeholder: 'd0121d0000882411234e11166c6aaa23ed5d74e0',
    rules: null,
    required: true,
};

export const POFILE_TO_USER: RecipeField = {
    name: 'email',
    label: 'Okta Email',
    tooltip:
        'Which Okta User Profile attribute to use as input to DataHub username mapping. Common values used are - login, email.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.okta_profile_to_username_attr',
    placeholder: 'email',
    rules: null,
};

export const POFILE_TO_GROUP: RecipeField = {
    name: 'okta_profile_to_group_name_attr',
    label: 'Okta Profile to group name attribute',
    tooltip: 'Which Okta Group Profile attribute to use as input to DataHub group name mapping.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.okta_profile_to_group_name_attr',
    placeholder: 'Group name',
    rules: null,
};

const schemaAllowFieldPath = 'source.config.okta_profile_to_username_regex.allow';
export const POFILE_TO_USER_REGX_ALLOW: RecipeField = {
    name: 'user.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Okta Profile To User Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.okta_profile_to_username_regex.deny';
export const POFILE_TO_USER_REGX_DENY: RecipeField = {
    name: 'user.deny',
    label: 'Deny Patterns',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'user_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Okta Profile To User Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const schemaAllowFieldPathForGroup = 'source.config.okta_profile_to_group_name_regex.allow';
export const POFILE_TO_GROUP_REGX_ALLOW: RecipeField = {
    name: 'group.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'group_pattern',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPathForGroup,
    rules: null,
    section: 'Okta Profile To Group Attribute Regex',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPathForGroup),
};

const schemaDenyFieldPathForGroup = 'source.config.okta_profile_to_group_name_regex.deny';
export const POFILE_TO_GROUP_REGX_DENY: RecipeField = {
    name: 'group.deny',
    label: 'Deny Patterns',
    tooltip:
        'Only include specific schemas by providing the name of a schema, or a regular expression (regex) to include specific schemas. If not provided, all schemas inside allowed databases will be included.',
    placeholder: 'group_pattern',
    type: FieldType.LIST,
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
    tooltip: 'Whether users should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_users',
    rules: null,
};

export const INGEST_GROUPS: RecipeField = {
    name: 'ingest_groups',
    label: 'Ingest Groups',
    tooltip: 'Whether groups should be ingested into DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_groups',
    rules: null,
};

export const INCLUDE_DEPROVISIONED_USERS: RecipeField = {
    name: 'include_deprovisioned_users',
    label: 'Include deprovisioned users',
    tooltip: 'Whether to ingest users in the DEPROVISIONED state from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_deprovisioned_users',
    rules: null,
};
export const INCLUDE_SUSPENDED_USERS: RecipeField = {
    name: 'include_suspended_users',
    label: 'Include suspended users',
    tooltip: 'Whether to ingest users in the SUSPENDED state from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_suspended_users',
    rules: null,
};

export const SKIP_USERS_WITHOUT_GROUP: RecipeField = {
    name: 'skip_users_without_a_group',
    label: 'Skip users without group',
    tooltip: 'Whether to skip users without group from Okta.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.skip_users_without_a_group',
    rules: null,
};
