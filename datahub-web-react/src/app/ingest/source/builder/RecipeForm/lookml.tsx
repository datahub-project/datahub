import { get } from 'lodash';
import React from 'react';

import { FieldType, RecipeField, setFieldValueOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

export const LOOKML = 'lookml';

export const LOOKML_GITHUB_INFO_REPO: RecipeField = {
    name: 'github_info.repo',
    label: 'GitHub Repo',
    tooltip: 'The name of the GitHub repository where your LookML is defined.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.github_info.repo',
    placeholder: 'datahub-project/datahub',
    rules: [{ required: true, message: 'Github Repo is required' }],
    required: true,
};

const deployKeyFieldPath = 'source.config.github_info.deploy_key';
export const DEPLOY_KEY: RecipeField = {
    name: 'github_info.deploy_key',
    label: 'GitHub Deploy Key',
    tooltip: (
        <>
            An SSH private key that has been provisioned for read access on the GitHub repository where the LookML is
            defined.
            <div style={{ marginTop: 8 }}>
                Learn how to generate an SSH for your GitHub repository{' '}
                <a
                    href="https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    here
                </a>
                .
            </div>
        </>
    ),
    type: FieldType.TEXTAREA,
    fieldPath: 'source.config.github_info.deploy_key',
    placeholder: '-----BEGIN OPENSSH PRIVATE KEY-----\n...',
    rules: [{ required: true, message: 'Github Deploy Key is required' }],
    setValueOnRecipeOverride: (recipe: any, value: string) => {
        const valueWithNewLine = `${value}\n`;
        return setFieldValueOnRecipe(recipe, valueWithNewLine, deployKeyFieldPath);
    },
    required: true,
};

function validateApiSection(getFieldValue, fieldName) {
    return {
        validator(_, value) {
            if (
                !!value ||
                (getFieldValue('connection_to_platform_map') &&
                    getFieldValue('connection_to_platform_map').length &&
                    getFieldValue('project_name'))
            ) {
                return Promise.resolve();
            }
            return Promise.reject(
                new Error(`${fieldName} is required if Connection to Platform Map and Project Name are not filled out`),
            );
        },
    };
}

export const LOOKML_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'Looker Base URL',
    tooltip: 'Optional URL to your Looker instance. This is used to generate external URLs for models in your project.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.api.base_url',
    placeholder: 'https://looker.company.com',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Base URL')],
};

export const LOOKML_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Looker Client ID',
    tooltip: 'The Looker API Client ID. Required if Looker Base URL is present.',
    type: FieldType.SECRET,
    placeholder: 'client_id',
    fieldPath: 'source.config.api.client_id',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Client ID')],
};

export const LOOKML_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Looker Client Secret',
    tooltip: 'A Looker API Client Secret. Required if Looker Base URL is present.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api.client_secret',
    placeholder: 'client_secret',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Client Secret')],
};

export const PROJECT_NAME: RecipeField = {
    name: 'project_name',
    label: 'LookML Project Name',
    tooltip: (
        <>
            The project name within which the LookML files live. See
            <a
                href="https://docs.looker.com/data-modeling/getting-started/how-project-works"
                target="_blank"
                rel="noreferrer"
            >
                this document
            </a>
            for more details. Required if Looker Base URL is not present.
        </>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_name',
    placeholder: 'My Project',
    rules: [
        ({ getFieldValue }) => ({
            validator(_, value) {
                if (
                    !!value ||
                    (getFieldValue('base_url') && getFieldValue('client_id') && getFieldValue('client_secret'))
                ) {
                    return Promise.resolve();
                }
                return Promise.reject(
                    new Error('Project Name is required if Base URL, Client ID, and Client Secret are not filled out'),
                );
            },
        }),
    ],
};

export const PARSE_TABLE_NAMES_FROM_SQL: RecipeField = {
    name: 'parse_table_names_from_sql',
    label: 'Extract External Lineage',
    tooltip:
        'Extract lineage between Looker and the external upstream Data Sources (e.g. Data Warehouses or Databases).',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.parse_table_names_from_sql',
    rules: null,
};

export const CONNECTION_TO_PLATFORM_MAP_NAME: RecipeField = {
    name: 'name',
    label: 'Name',
    tooltip: (
        <div>
            Looker connection name. See{' '}
            <a
                href="https://cloud.google.com/looker/docs/reference/param-model-connection"
                target="_blank"
                rel="noreferrer"
            >
                documentation
            </a>{' '}
            for more details.
        </div>
    ),

    type: FieldType.TEXT,
    fieldPath: 'name',
    placeholder: 'my_mysql_connection',
    rules: [{ required: true, message: 'Name is required' }],
};

export const PLATFORM: RecipeField = {
    name: 'platform',
    label: 'Platform',
    tooltip: 'The Data Platform ID in DataHub (e.g. snowflake, bigquery, redshift, mysql, postgres)',
    type: FieldType.TEXT,
    fieldPath: 'platform',
    placeholder: 'snowflake',
    rules: [{ required: true, message: 'Platform is required' }],
};

export const DEFAULT_DB: RecipeField = {
    name: 'default_db',
    label: 'Default Database',
    tooltip: 'The Database associated with assets from the Looker connection.',
    type: FieldType.TEXT,
    fieldPath: 'default_db',
    placeholder: 'default_db',
    rules: [{ required: true, message: 'Default Database is required' }],
};

const dictFields = [PLATFORM, DEFAULT_DB];
const connectionToPlatformMapFieldPath = 'source.config.connection_to_platform_map';
export const CONNECTION_TO_PLATFORM_MAP: RecipeField = {
    name: 'connection_to_platform_map',
    label: 'Connection To Platform Map',
    tooltip:
        'A mapping of Looker connection names to DataHub Data Platform and Database names. This is used to create an accurate picture of the Lineage between LookML models and upstream Data Sources.',
    type: FieldType.DICT,
    buttonLabel: 'Add mapping',
    fieldPath: connectionToPlatformMapFieldPath,
    keyField: CONNECTION_TO_PLATFORM_MAP_NAME,
    fields: dictFields,
    rules: [
        ({ getFieldValue }) => ({
            validator(_, value) {
                if (
                    (!!value && !!value.length) ||
                    (getFieldValue('base_url') && getFieldValue('client_id') && getFieldValue('client_secret'))
                ) {
                    return Promise.resolve();
                }
                return Promise.reject(
                    new Error(
                        'Connection Map is required if Base URL, Client ID, and Client Secret are not filled out',
                    ),
                );
            },
        }),
    ],
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, connectionToPlatformMapFieldPath);
        if (!value) return undefined;
        const keys = Object.keys(value);
        return keys.map((key) => ({
            [CONNECTION_TO_PLATFORM_MAP_NAME.name]: key,
            [PLATFORM.name]: value[key][PLATFORM.name],
            [DEFAULT_DB.name]: value[key][DEFAULT_DB.name],
        }));
    },
    setValueOnRecipeOverride: (recipe: any, values: string[]) => {
        const filteredValues = values.filter((v) => !!v);
        if (!filteredValues.length) return setFieldValueOnRecipe(recipe, null, connectionToPlatformMapFieldPath);

        const result = {};
        filteredValues.forEach((value) => {
            result[value[CONNECTION_TO_PLATFORM_MAP_NAME.name]] = {
                [PLATFORM.name]: value[PLATFORM.name],
                [DEFAULT_DB.name]: value[DEFAULT_DB.name],
            };
        });
        return setFieldValueOnRecipe(recipe, result, connectionToPlatformMapFieldPath);
    },
};
