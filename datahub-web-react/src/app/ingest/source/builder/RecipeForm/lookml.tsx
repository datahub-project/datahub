import React from 'react';
import { get } from 'lodash';
import { FieldType, RecipeField, setFieldValueOnRecipe } from './common';

export const LOOKML = 'lookml';

export const LOOKML_GITHUB_INFO_REPO: RecipeField = {
    name: 'github_info.repo',
    label: 'GitHub Repo',
    tooltip: (
        <div>
            <p>
                Name of your github repo. e.g. repo for{' '}
                <a href="https://github.com/datahub-project/datahub" target="_blank" rel="noreferrer">
                    https://github.com/datahub-project/datahub
                </a>{' '}
                is datahub-project/datahub.
            </p>
        </div>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.github_info.repo',
    rules: [{ required: true, message: 'Github Repo is required' }],
};

export const DEPLOY_KEY: RecipeField = {
    name: 'github_info.deploy_key',
    label: 'GitHub Deploy Key',
    tooltip: 'The SSH private key that has been provisioned for read access on the GitHub repository.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.github_info.deploy_key',
    rules: [{ required: true, message: 'Github Deploy Key is required' }],
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
    label: 'Base URL',
    tooltip:
        'Url to your Looker instance: https://company.looker.com:19999 or https://looker.company.com, or similar. Used for making API calls to Looker and constructing clickable dashboard and chart urls.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Base URL')],
};

export const LOOKML_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Looker API client id.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Client ID')],
};

export const LOOKML_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip: 'Looker API client secret.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Client Secret')],
};

export const PROJECT_NAME: RecipeField = {
    name: 'project_name',
    label: 'Project Name',
    tooltip: (
        <div>
            Required if you don&apos;t specify the api section. The project name within which all the model files live.
            See{' '}
            <a
                href="https://docs.looker.com/data-modeling/getting-started/how-project-works"
                target="_blank"
                rel="noreferrer"
            >
                https://docs.looker.com/data-modeling/getting-started/how-project-works
            </a>{' '}
            to understand what the Looker project name should be. The simplest way to see your projects is to click on
            Develop followed by Manage LookML Projects in the Looker application.
        </div>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_name',
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
    label: 'Parse Table Names from SQL',
    tooltip: 'Use an SQL parser to try to parse the tables the views depends on.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.parse_table_names_from_sql',
    rules: null,
};

export const CONNECTION_TO_PLATFORM_MAP_NAME: RecipeField = {
    name: 'name',
    label: 'Name',
    tooltip: 'Looker connection name',
    type: FieldType.TEXT,
    fieldPath: 'name',
    rules: null,
};

export const PLATFORM: RecipeField = {
    name: 'platform',
    label: 'Platform',
    tooltip: 'Associated platform in DataHub',
    type: FieldType.TEXT,
    fieldPath: 'platform',
    rules: null,
};

export const DEFAULT_DB: RecipeField = {
    name: 'default_db',
    label: 'Default Database',
    tooltip: 'Associated database in DataHub',
    type: FieldType.TEXT,
    fieldPath: 'default_db',
    rules: null,
};

const dictFields = [PLATFORM, DEFAULT_DB];
const connectionToPlatformMapFieldPath = 'source.config.connection_to_platform_map';
export const CONNECTION_TO_PLATFORM_MAP: RecipeField = {
    name: 'connection_to_platform_map',
    label: 'Connection To Platform Map',
    tooltip: 'A mapping of Looker connection names to DataHub platform and database values',
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
