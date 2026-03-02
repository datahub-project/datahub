import { get, omit } from 'lodash';
import React from 'react';

import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

// Deployment type constants
const deploymentTypeFieldName = 'deployment_type';
const deploymentTypeFieldPath = 'source.config.__deployment_type';
const deploymentTypeCloud = 'cloud';
const deploymentTypeDataCenter = 'datacenter';

// Field paths
const cloudFieldPath = 'source.config.cloud';
const urlFieldPath = 'source.config.url';
const usernameFieldPath = 'source.config.username';
const apiTokenFieldPath = 'source.config.api_token';
const personalAccessTokenFieldPath = 'source.config.personal_access_token';
const spaceAllowFieldPath = 'source.config.spaces.allow';
const spaceDenyFieldPath = 'source.config.spaces.deny';
const pageAllowFieldPath = 'source.config.pages.allow';
const pageDenyFieldPath = 'source.config.pages.deny';

// Helper functions for deployment type
function setDeploymentTypeOnRecipe(recipe: any, value: string): any {
    let updatedRecipe = { ...recipe };

    // Set the cloud boolean based on deployment type
    if (value === deploymentTypeCloud) {
        updatedRecipe = { ...updatedRecipe };
        updatedRecipe = omit(updatedRecipe, [personalAccessTokenFieldPath]);
        updatedRecipe.source = updatedRecipe.source || {};
        updatedRecipe.source.config = updatedRecipe.source.config || {};
        updatedRecipe.source.config.cloud = true;
    } else if (value === deploymentTypeDataCenter) {
        updatedRecipe = { ...updatedRecipe };
        updatedRecipe = omit(updatedRecipe, [usernameFieldPath, apiTokenFieldPath]);
        updatedRecipe.source = updatedRecipe.source || {};
        updatedRecipe.source.config = updatedRecipe.source.config || {};
        updatedRecipe.source.config.cloud = false;
    }

    return updatedRecipe;
}

function getDeploymentTypeFromRecipe(recipe: any): string {
    const cloudValue = get(recipe, cloudFieldPath);
    const hasUsername = !!get(recipe, usernameFieldPath);
    const hasApiToken = !!get(recipe, apiTokenFieldPath);
    const hasPAT = !!get(recipe, personalAccessTokenFieldPath);

    // Determine deployment type from existing fields
    if (cloudValue === true || hasUsername || hasApiToken) {
        return deploymentTypeCloud;
    }
    if (cloudValue === false || hasPAT) {
        return deploymentTypeDataCenter;
    }

    // Default to Cloud
    return deploymentTypeCloud;
}

export const CONFLUENCE_DEPLOYMENT_TYPE: RecipeField = {
    name: deploymentTypeFieldName,
    label: 'Deployment Type',
    tooltip:
        'Select whether you are using Confluence Cloud (hosted by Atlassian) or Confluence Data Center (self-hosted).',
    helper: React.createElement(
        React.Fragment,
        null,
        'Choose ',
        React.createElement('strong', null, 'Cloud'),
        ' if your URL is ',
        React.createElement('code', null, '*.atlassian.net'),
        '. Choose ',
        React.createElement('strong', null, 'Data Center'),
        ' for self-hosted installations.',
    ),
    type: FieldType.SELECT,
    options: [
        { label: 'Confluence Cloud (Atlassian-hosted)', value: deploymentTypeCloud },
        { label: 'Confluence Data Center (Self-hosted)', value: deploymentTypeDataCenter },
    ],
    fieldPath: deploymentTypeFieldPath,
    required: true,
    rules: null,
    setValueOnRecipeOverride: setDeploymentTypeOnRecipe,
    getValueFromRecipeOverride: getDeploymentTypeFromRecipe,
};

export const CONFLUENCE_URL: RecipeField = {
    name: 'url',
    label: 'Confluence URL',
    dynamicLabel: (values) => {
        const deploymentType = get(values, deploymentTypeFieldName);
        return deploymentType === deploymentTypeCloud ? 'Confluence Cloud URL' : 'Confluence Data Center URL';
    },
    helper: React.createElement(
        React.Fragment,
        null,
        'Your Confluence instance URL. Example: ',
        React.createElement('code', null, 'https://your-domain.atlassian.net/wiki'),
    ),
    tooltip: 'The base URL for your Confluence instance.',
    type: FieldType.TEXT,
    fieldPath: urlFieldPath,
    placeholder: 'https://your-domain.atlassian.net/wiki',
    rules: [{ required: true, message: 'Confluence URL is required' }],
    required: true,
};

export const CONFLUENCE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Email Address',
    helper: 'Your Confluence Cloud email address used for authentication.',
    tooltip: 'Email address associated with your Atlassian Cloud account.',
    type: FieldType.TEXT,
    fieldPath: usernameFieldPath,
    placeholder: 'user@example.com',
    dynamicRequired: (values) => get(values, deploymentTypeFieldName) === deploymentTypeCloud,
    dynamicHidden: (values) => get(values, deploymentTypeFieldName) !== deploymentTypeCloud,
    rules: null,
};

export const CONFLUENCE_API_TOKEN: RecipeField = {
    name: 'api_token',
    label: 'API Token',
    helper: React.createElement(
        React.Fragment,
        null,
        'Create an API token at ',
        React.createElement(
            'a',
            {
                href: 'https://id.atlassian.com/manage-profile/security/api-tokens',
                target: '_blank',
                rel: 'noreferrer',
            },
            'Atlassian Account Settings',
        ),
        '.',
    ),
    tooltip: 'API token for Confluence Cloud authentication. Create one in your Atlassian account security settings.',
    type: FieldType.SECRET,
    fieldPath: apiTokenFieldPath,
    placeholder: 'ATATT3xFfGF0...',
    dynamicRequired: (values) => get(values, deploymentTypeFieldName) === deploymentTypeCloud,
    dynamicHidden: (values) => get(values, deploymentTypeFieldName) !== deploymentTypeCloud,
    rules: null,
};

export const CONFLUENCE_PERSONAL_ACCESS_TOKEN: RecipeField = {
    name: 'personal_access_token',
    label: 'Personal Access Token',
    helper: 'Personal Access Token for Confluence Data Center. Generate one in Confluence Settings > Personal Access Tokens.',
    tooltip: 'Confluence Data Center Personal Access Token for authentication.',
    type: FieldType.SECRET,
    fieldPath: personalAccessTokenFieldPath,
    placeholder: 'your-pat-token',
    dynamicRequired: (values) => get(values, deploymentTypeFieldName) === deploymentTypeDataCenter,
    dynamicHidden: (values) => get(values, deploymentTypeFieldName) !== deploymentTypeDataCenter,
    rules: null,
};

export const CONFLUENCE_SPACE_ALLOW: FilterRecipeField = {
    name: 'space_allow',
    label: 'Include',
    tooltip: 'Space keys or URLs to include. Leave empty to auto-discover all accessible spaces.',
    helper: React.createElement(
        React.Fragment,
        null,
        React.createElement('strong', null, 'Spaces'),
        ' are top-level containers for pages in Confluence (similar to folders or projects). ',
        'Use this section to control which spaces are ingested. ',
        React.createElement('br'),
        React.createElement('br'),
        'Specify space keys (e.g., ',
        React.createElement('code', null, 'ENGINEERING, PRODUCT'),
        ') or full URLs. Leave empty to ingest all accessible spaces.',
    ),
    type: FieldType.LIST,
    fieldPath: spaceAllowFieldPath,
    placeholder: 'ENGINEERING or https://your-domain.atlassian.net/wiki/spaces/TEAM',
    buttonLabel: 'Add space',
    rules: null,
    section: 'Spaces',
    filteringResource: 'Space',
    rule: FilterRule.INCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, spaceAllowFieldPath),
};

export const CONFLUENCE_SPACE_DENY: FilterRecipeField = {
    name: 'space_deny',
    label: 'Exclude',
    tooltip: 'Space keys or URLs to exclude. Useful for excluding personal spaces or archived content.',
    helper: React.createElement(
        React.Fragment,
        null,
        'Exclude specific spaces by space key (e.g., ',
        React.createElement('code', null, '~johndoe, ARCHIVE'),
        ') or URL. Useful for excluding personal spaces or archived content. Exclude always takes precedence over Include.',
    ),
    type: FieldType.LIST,
    fieldPath: spaceDenyFieldPath,
    placeholder: '~johndoe or ARCHIVE',
    buttonLabel: 'Add space',
    rules: null,
    section: 'Spaces',
    filteringResource: 'Space',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, spaceDenyFieldPath),
};

export const CONFLUENCE_PAGE_ALLOW: FilterRecipeField = {
    name: 'page_allow',
    label: 'Include',
    tooltip: 'Page IDs or URLs to include. When specified, only these pages and their children will be ingested.',
    helper: React.createElement(
        React.Fragment,
        null,
        React.createElement('strong', null, 'Pages'),
        ' are individual documents within spaces. ',
        'Use this section when you want to ingest specific page trees instead of entire spaces. ',
        React.createElement('br'),
        React.createElement('br'),
        'Specify page IDs (e.g., ',
        React.createElement('code', null, '123456'),
        ') or full URLs. Child pages will be included automatically if recursive ingestion is enabled.',
    ),
    type: FieldType.LIST,
    fieldPath: pageAllowFieldPath,
    placeholder: '123456 or https://your-domain.atlassian.net/wiki/spaces/ENG/pages/123456/API-Docs',
    buttonLabel: 'Add page',
    rules: null,
    section: 'Pages',
    filteringResource: 'Page',
    rule: FilterRule.INCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, pageAllowFieldPath),
};

export const CONFLUENCE_PAGE_DENY: FilterRecipeField = {
    name: 'page_deny',
    label: 'Exclude',
    tooltip: 'Page IDs or URLs to exclude from ingestion.',
    helper: React.createElement(
        React.Fragment,
        null,
        'Exclude specific pages by page ID (e.g., ',
        React.createElement('code', null, '999999'),
        ') or URL. Useful for excluding draft pages, templates, or archived content. Exclude always takes precedence over Include.',
    ),
    type: FieldType.LIST,
    fieldPath: pageDenyFieldPath,
    placeholder: '999999',
    buttonLabel: 'Add page',
    rules: null,
    section: 'Pages',
    filteringResource: 'Page',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, pageDenyFieldPath),
};
