import { get } from 'lodash';

import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

const deploymentFieldName = 'deployment';
const deploymentOnPrem = 'on_prem';
const deploymentCloud = 'cloud';

const isOnPrem = (values: Record<string, any>) => get(values, deploymentFieldName) === deploymentOnPrem;
const isCloud = (values: Record<string, any>) => get(values, deploymentFieldName) === deploymentCloud;

const deploymentFieldPath = 'source.config.deployment';
const baseUrlFieldPath = 'source.config.base_url';
const usernameFieldPath = 'source.config.username';
const passwordFieldPath = 'source.config.password';
const tokenFieldPath = 'source.config.token';
const verifySslFieldPath = 'source.config.verify_ssl';
const includeAppnodesFieldPath = 'source.config.include_appnodes';
const applicationAllowFieldPath = 'source.config.application_pattern.allow';
const applicationDenyFieldPath = 'source.config.application_pattern.deny';

export const TIBCO_BW_DEPLOYMENT: RecipeField = {
    name: deploymentFieldName,
    label: 'Deployment',
    tooltip:
        'Select the TIBCO runtime: On-Prem for ActiveMatrix BusinessWorks (bwagent) or Cloud for TIBCO Cloud Integration.',
    type: FieldType.SELECT,
    options: [
        { label: 'On-Prem (ActiveMatrix BusinessWorks)', value: deploymentOnPrem },
        { label: 'Cloud (TIBCO Cloud Integration)', value: deploymentCloud },
    ],
    fieldPath: deploymentFieldPath,
    required: true,
    rules: null,
};

export const TIBCO_BW_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Base URL',
    dynamicLabel: (values) => (isCloud(values) ? 'TIBCO Cloud API URL' : 'bwagent URL'),
    tooltip:
        'For On-Prem, the bwagent REST API endpoint (e.g. http://bw-host:8079). For Cloud, defaults to https://api.cloud.tibco.com.',
    type: FieldType.TEXT,
    fieldPath: baseUrlFieldPath,
    placeholder: 'http://bw-host.example.com:8079',
    dynamicRequired: (values) => isOnPrem(values),
    rules: null,
};

export const TIBCO_BW_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Username for bwagent HTTP basic authentication (On-Prem only).',
    type: FieldType.TEXT,
    fieldPath: usernameFieldPath,
    dynamicRequired: (values) => isOnPrem(values),
    dynamicHidden: (values) => !isOnPrem(values),
    rules: null,
};

export const TIBCO_BW_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Password for bwagent HTTP basic authentication (On-Prem only).',
    type: FieldType.SECRET,
    fieldPath: passwordFieldPath,
    dynamicRequired: (values) => isOnPrem(values),
    dynamicHidden: (values) => !isOnPrem(values),
    rules: null,
};

export const TIBCO_BW_TOKEN: RecipeField = {
    name: 'token',
    label: 'OAuth Token',
    tooltip: 'OAuth access token for the TIBCO Cloud Integration API (Cloud only).',
    type: FieldType.SECRET,
    fieldPath: tokenFieldPath,
    dynamicRequired: (values) => isCloud(values),
    dynamicHidden: (values) => !isCloud(values),
    rules: null,
};

export const TIBCO_BW_VERIFY_SSL: RecipeField = {
    name: 'verify_ssl',
    label: 'Verify SSL',
    tooltip: "Whether to verify the server's TLS certificate.",
    type: FieldType.BOOLEAN,
    fieldPath: verifySslFieldPath,
    rules: null,
};

export const TIBCO_BW_INCLUDE_APPNODES: RecipeField = {
    name: 'include_appnodes',
    label: 'Include Appnodes',
    tooltip: 'Attach appnode names and run states to the appspace as custom properties (On-Prem only).',
    type: FieldType.BOOLEAN,
    fieldPath: includeAppnodesFieldPath,
    dynamicHidden: (values) => !isOnPrem(values),
    rules: null,
};

export const TIBCO_BW_APPLICATION_ALLOW: FilterRecipeField = {
    name: 'application_allow',
    label: 'Include',
    tooltip: 'Regex patterns for application names to include. Leave empty to include all applications.',
    type: FieldType.LIST,
    fieldPath: applicationAllowFieldPath,
    placeholder: 'my_application',
    buttonLabel: 'Add pattern',
    rules: null,
    section: 'Applications',
    filteringResource: 'Application',
    rule: FilterRule.INCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, applicationAllowFieldPath),
};

export const TIBCO_BW_APPLICATION_DENY: FilterRecipeField = {
    name: 'application_deny',
    label: 'Exclude',
    tooltip: 'Regex patterns for application names to exclude. Exclude always takes precedence over Include.',
    type: FieldType.LIST,
    fieldPath: applicationDenyFieldPath,
    placeholder: 'test_.*',
    buttonLabel: 'Add pattern',
    rules: null,
    section: 'Applications',
    filteringResource: 'Application',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, applicationDenyFieldPath),
};
