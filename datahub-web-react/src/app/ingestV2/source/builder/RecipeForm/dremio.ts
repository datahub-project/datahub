import { get } from 'lodash';

import {
    FieldType,
    FieldsValues,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const DREMIO = 'dremio';

// Core section

const isDremioCloudFieldPath = 'source.config.is_dremio_cloud';
const isDremioCloudFieldName = 'is_dremio_cloud';
export const DREMIO_IS_DREMIO_CLOUD: RecipeField = {
    name: isDremioCloudFieldName,
    label: 'Deployment Type',
    tooltip: 'Deployed with Dremio Cloud. Deselect if  self-hosted.',
    type: FieldType.BOOLEAN,
    fieldPath: isDremioCloudFieldPath,
    required: false,
    rules: null,
};

const dremioCloudRegionFieldPath = 'source.config.dremio_cloud_region';
export const DREMIO_DREMIO_CLOUD_REGION: RecipeField = {
    name: 'dremio_cloud_region',
    label: 'Dremio Cloud Region',
    tooltip: 'Dremio Cloud region: US or EU. ',
    type: FieldType.SELECT,
    options: [
        { label: 'US', value: 'US' },
        { label: 'EU', value: 'EU' },
    ],
    fieldPath: dremioCloudRegionFieldPath,
    dynamicRequired: (values: FieldsValues) => !!get(values, isDremioCloudFieldName) === true,
    dynamicHidden: (values: FieldsValues) => !!get(values, isDremioCloudFieldName) === false,
    rules: null,
};

const dremioCloudProjectIdFieldPath = 'source.config.dremio_cloud_project_id';
export const DREMIO_DREMIO_CLOUD_PROJECT_ID: RecipeField = {
    name: 'dremio_cloud_project_id',
    label: 'Cloud Project ID',
    tooltip: 'Dremio Cloud project ID. Find in Project Settings in Dremio Cloud UI.',
    type: FieldType.TEXT,
    fieldPath: dremioCloudProjectIdFieldPath,
    placeholder: 'dremio-cloud-project-id',
    dynamicRequired: (values: FieldsValues) => !!get(values, isDremioCloudFieldName) === true,
    dynamicHidden: (values: FieldsValues) => !!get(values, isDremioCloudFieldName) === false,
    rules: null,
};

const hostnameFieldPath = 'source.config.hostname';
export const DREMIO_HOSTNAME: RecipeField = {
    name: 'hostname',
    label: 'Hostname',
    tooltip: 'Hostname or IP address of Dremio server (e.g. dremio.company.com). ',
    type: FieldType.TEXT,
    fieldPath: hostnameFieldPath,
    placeholder: 'dremio.company.com',
    dynamicRequired: (values: FieldsValues) => !!get(values, isDremioCloudFieldName) === false,
    dynamicHidden: (values: FieldsValues) => !!get(values, isDremioCloudFieldName) === true,
    rules: null,
};

const portFieldPath = 'source.config.port';
export const DREMIO_PORT: RecipeField = {
    name: 'port',
    label: 'Port',
    tooltip: 'Port for Dremio REST API. Default is 9047 for self-hosted. ',
    type: FieldType.TEXT,
    fieldPath: portFieldPath,
    placeholder: '9047',
    required: false,
    rules: null,
};

const tlsFieldPath = 'source.config.tls';
export const DREMIO_TLS: RecipeField = {
    name: 'tls',
    label: 'TLS/SSL',
    tooltip: 'Whether Dremio REST API uses TLS/SSL encryption. Default true.',
    type: FieldType.BOOLEAN,
    fieldPath: tlsFieldPath,
    required: false,
    rules: null,
};

const authenticationMethodFieldPath = 'source.config.authentication_method';
const authenticationMethodFieldName = 'authentication_method';
const authenticationMethodPAT = 'PAT';
const authenticationMethodPassword = 'password';
export const DREMIO_AUTHENTICATION_METHOD: RecipeField = {
    name: authenticationMethodFieldName,
    label: 'Authentication Method',
    tooltip: 'Authentication method: PAT (Personal Access Token) or password. ',
    type: FieldType.SELECT,
    options: [
        { label: 'PAT', value: authenticationMethodPAT },
        { label: 'Password', value: authenticationMethodPassword },
    ],
    fieldPath: authenticationMethodFieldPath,
    required: true,
    rules: null,
};

const usernameFieldPath = 'source.config.username';
export const DREMIO_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Dremio username for authentication.',
    type: FieldType.TEXT,
    fieldPath: usernameFieldPath,
    placeholder: 'dremio_user',
    dynamicRequired: (values) => get(values, authenticationMethodFieldName) === authenticationMethodPassword,
    rules: null,
};

const passwordFieldPath = 'source.config.password';
export const DREMIO_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    dynamicLabel: (values: FieldsValues) => {
        const authMethod = get(values, authenticationMethodFieldName);
        return authMethod === authenticationMethodPAT ? 'Personal Access Token' : 'Password';
    },
    tooltip: 'Password or Personal Access Token. For PAT: generate in User Settings > Personal Access Tokens.',
    type: FieldType.SECRET,
    fieldPath: passwordFieldPath,
    placeholder: 'dremio_password',
    required: true,
    rules: null,
};

// Filters section

const dremioProfileAllowFieldPath = 'source.config.profile_pattern.allow';
export const DREMIO_PROFILE_ALLOW: FilterRecipeField = {
    name: 'profile_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Allow specific profiles',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dremioProfileAllowFieldPath,
    rules: null,
    section: 'Profiles',
    filteringResource: 'Profile',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dremioProfileAllowFieldPath),
};

const dremioProfileDenyFieldPath = 'source.config.profile_pattern.deny';
export const DREMIO_PROFILE_DENY: FilterRecipeField = {
    name: 'profile_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Deny specific profiles',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dremioProfileDenyFieldPath,
    rules: null,
    section: 'Profiles',
    filteringResource: 'Profile',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dremioProfileDenyFieldPath),
};

// Settings section

const platformInstanceFieldPath = 'source.config.platform_instance';
export const DREMIO_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'Optional instance identifier (e.g. prod_dremio). Critical for multi-database deployments and lineage.',
    type: FieldType.TEXT,
    fieldPath: platformInstanceFieldPath,
    required: false,
    rules: null,
};

const ingestOwnerFieldPath = 'source.config.ingest_owner';
export const DREMIO_INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Extract Owners',
    tooltip:
        'Import owners from Dremio. Will overwrite ownership manually set in DataHub. Disable if managing ownership in DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: ingestOwnerFieldPath,
    required: false,
    rules: null,
};

const includeQueryLineageFieldPath = 'source.config.include_query_lineage';
export const DREMIO_INCLUDE_QUERY_LINEAGE: RecipeField = {
    name: 'include_query_lineage',
    label: 'Extract Query Lineage',
    tooltip: 'Extract lineage from Dremio query history. Shows table dependencies from queries. Experimental feature.',
    type: FieldType.BOOLEAN,
    fieldPath: includeQueryLineageFieldPath,
    required: false,
    rules: null,
};
