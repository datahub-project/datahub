import React from 'react';
import styled from 'styled-components';
import { get } from 'lodash';
import { RecipeField, FieldType, setFieldValueOnRecipe } from './common';

const TipSection = styled.div`
    margin-bottom: 12px;
`;

export const DBT_CLOUD = 'dbt-cloud';

export const DBT_CLOUD_TOKEN: RecipeField = {
    name: 'token',
    label: 'API Token',
    tooltip: (
        <span>
            <TipSection>
                A service account API token for extracting metadata from dbt Cloud APIs. This token must have the
                privileges required to read metadata (e.g. <b>Metadata Only</b> permissions).
            </TipSection>
            <TipSection>
                For more information about dbt service account tokens, check out the docs
                <a href="missions-for-service-account-tokens">here</a>
            </TipSection>
        </span>
    ),
    type: FieldType.SECRET,
    fieldPath: 'source.config.token',
    placeholder: 'dbts_ndg_m5oCuSRRC80tpx4ysYfN2tOreiHuATAu5VFcdrkIznQgl4VCOs6w==',
    required: true,
    rules: null,
};

export const DBT_CLOUD_ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: (
        <span>
            <TipSection>The ID of the dbt Cloud account to extract metadata for.</TipSection>
            <TipSection>
                This can be found in the URL of your dbt instance: https://cloud.getdbt.com/#/accounts/ACCOUNT_ID/.
            </TipSection>
        </span>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.account_id',
    placeholder: '123',
    required: true,
    rules: null,
};

export const DBT_CLOUD_PROJECT_ID: RecipeField = {
    name: 'project_id',
    label: 'Project ID',
    tooltip: (
        <span>
            <TipSection>The ID of the dbt Cloud project to extract metadata for.</TipSection>
            <TipSection>
                This can be found in the URL of your dbt instance:
                https://cloud.getdbt.com/#/accounts/123/projects/PROJECT_ID.
            </TipSection>
        </span>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_id',
    placeholder: '456',
    required: true,
    rules: null,
};

export const DBT_CLOUD_JOB_ID: RecipeField = {
    name: 'job_id',
    label: 'Job ID',
    tooltip: (
        <span>
            <TipSection>
                The ID of the dbt Cloud job to extract metadata for. Choose the job that serves as the primary mechanism
                for updating your production data.
            </TipSection>
            <TipSection>The Job ID can be found in the URL on the Jobs tab of dbt Cloud.</TipSection>
            <TipSection>
                Ensure that your job enables documentation generation on each run by enabling &apos;Generate Docs&apos;
                on dbt Cloud.
            </TipSection>
        </span>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.job_id',
    placeholder: '789',
    required: true,
    rules: null,
};

const includeModelsPath = 'source.config.entities_enabled.models';
export const INCLUDE_MODELS: RecipeField = {
    name: 'entities_enabled.models',
    label: 'Include Models',
    tooltip: 'Whether to include extraction of Models or not.',
    type: FieldType.BOOLEAN,
    fieldPath: includeModelsPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const includeModels = get(recipe, includeModelsPath);
        if (!includeModels || includeModels === 'YES') {
            return true;
        }
        return false;
    },
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        const includeModels = value === true ? 'YES' : 'NO';
        return setFieldValueOnRecipe(recipe, includeModels, includeModelsPath);
    },
};

const includeSourcesPath = 'source.config.entities_enabled.sources';
export const INCLUDE_SOURCES: RecipeField = {
    name: 'entities_enabled.sources',
    label: 'Include Sources',
    tooltip: 'Whether to include extraction of Sources or not.',
    type: FieldType.BOOLEAN,
    fieldPath: includeSourcesPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const includeSources = get(recipe, includeSourcesPath);
        if (includeSources === 'YES' || includeSources === undefined || includeSources === null) {
            return true;
        }
        return false;
    },
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        const includeSources = value === true ? 'YES' : 'NO';
        return setFieldValueOnRecipe(recipe, includeSources, includeSourcesPath);
    },
};

const includeSeedsPath = 'source.config.entities_enabled.seeds';
export const INCLUDE_SEEDS: RecipeField = {
    name: 'entities_enabled.seeds',
    label: 'Include Seeds',
    tooltip: 'Whether to include extraction of Seeds or not.',
    type: FieldType.BOOLEAN,
    fieldPath: includeSeedsPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const includeSeeds = get(recipe, includeSeedsPath);
        if (includeSeeds === 'YES' || includeSeeds === undefined || includeSeeds === null) {
            return true;
        }
        return false;
    },
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        const includeSeeds = value === true ? 'YES' : 'NO';
        return setFieldValueOnRecipe(recipe, includeSeeds, includeSourcesPath);
    },
};

const includeTestDefinitionsPath = 'source.config.entities_enabled.test_definitions';
export const INCLUDE_TEST_DEFINITIONS: RecipeField = {
    name: 'entities_enabled.test_definitions',
    label: 'Include Test Definitions',
    tooltip: 'Whether to include extraction of Test Definitions or not.',
    type: FieldType.BOOLEAN,
    fieldPath: includeTestDefinitionsPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const includeTestDefinitions = get(recipe, includeTestDefinitionsPath);
        if (
            includeTestDefinitions === 'YES' ||
            includeTestDefinitions === undefined ||
            includeTestDefinitions === null
        ) {
            return true;
        }
        return false;
    },
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        const includeTestDefinitions = value === true ? 'YES' : 'NO';
        return setFieldValueOnRecipe(recipe, includeTestDefinitions, includeTestDefinitionsPath);
    },
};

const includeTestResultsPath = 'source.config.entities_enabled.test_results';
export const INCLUDE_TEST_RESULTS: RecipeField = {
    name: 'entities_enabled.test_results',
    label: 'Include Test Results',
    tooltip: 'Whether to include extraction of Test Results or not.',
    type: FieldType.BOOLEAN,
    fieldPath: includeTestResultsPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const includeTestResults = get(recipe, includeTestResultsPath);
        if (includeTestResults === 'YES' || includeTestResults === undefined || includeTestResults === null) {
            return true;
        }
        return false;
    },
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        const includeTestResults = value === true ? 'YES' : 'NO';
        return setFieldValueOnRecipe(recipe, includeTestResults, includeTestResultsPath);
    },
};

const nodeAllowFieldPath = 'source.config.node_name_pattern.allow';
export const NODE_ALLOW: RecipeField = {
    name: 'node_name_pattern.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific dbt Nodes (resources) by providing their name, or a Regular Expression (REGEX). If not provided, all Nodes will be included.',
    placeholder: 'model_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: nodeAllowFieldPath,
    rules: null,
    section: 'Nodes',
};

const nodeDenyFieldPath = 'source.config.node_name_pattern.deny';
export const NODE_DENY: RecipeField = {
    name: 'node_name_pattern.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific dbt Nodes (Resources) by providing their name, or a Regular Expression (REGEX). If not provided, all Nodes will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'node_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: nodeDenyFieldPath,
    rules: null,
    section: 'Nodes',
};

export const METADATA_ENDPOINT: RecipeField = {
    name: 'metadata_endpoint',
    label: 'Custom Metadata Endpoint URL',
    tooltip:
        'A custom URL used for extracting Metadata. By default, this metadata is extracted from https://metadata.cloud.getdbt.com/graphql. In most cases, users should NOT need to provide this value.',
    placeholder: 'https://metadata.cloud.getdbt.com/graphql',
    type: FieldType.TEXT,
    fieldPath: 'source.config.metadata_endpoint',
    rules: null,
};

const extractOwnersPath = 'source.config.enable_owner_extraction';
export const EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    label: 'Extract Owners',
    tooltip:
        'Try to extract owners from dbt meta properties. Be careful: This can override Owners added by users of DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.enable_owner_extraction',
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const extractOwners = get(recipe, extractOwnersPath);
        if (extractOwners !== undefined && extractOwners !== null) {
            return extractOwners;
        }
        return true;
    },
};

export const TARGET_PLATFORM: RecipeField = {
    name: 'target_platform',
    label: 'Data Platform (Connection Type)',
    tooltip: 'The type of Data Platform that dbt is connected to.',
    placeholder: 'Select a Data Platform Type...',
    type: FieldType.SELECT,
    options: [
        { label: 'Snowflake', value: 'snowflake' },
        { label: 'BigQuery', value: 'bigquery' },
        { label: 'Redshift', value: 'redshift' },
        { label: 'Postgres', value: 'postgres' },
        { label: 'Trino (Starburst)', value: 'trino' },
        { label: 'Databricks', value: 'databricks' },
    ],
    fieldPath: 'source.config.target_platform',
    required: true,
    rules: null,
};

export const TARGET_PLATFORM_INSTANCE: RecipeField = {
    name: 'target_platform_instance',
    label: 'Data Platform Instance',
    tooltip: (
        <span>
            <TipSection>
                The DataHub Platform Instance identifier that should be used for the assets extracted from dbt.
            </TipSection>
            <TipSection>
                This is used to correctly connect the metadata extracted from the Data Platform with that extracted from
                dbt Cloud.
            </TipSection>
            <TipSection>
                Leave this blank if you have not configured a Data Platform Instance when ingesting from the associated
                Data Platform.
            </TipSection>
        </span>
    ),
    placeholder: 'redshift_instance_2',
    type: FieldType.TEXT,
    fieldPath: 'source.config.target_platform_instance',
    rules: null,
};
