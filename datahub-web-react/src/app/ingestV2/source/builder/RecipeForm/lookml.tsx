import { get } from 'lodash';
import React from 'react';

import { FieldType, RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

export const LOOKML = 'lookml';

export const LOOKML_GIT_INFO_REPO: RecipeField = {
    name: 'git_info.repo',
    label: 'Git Repository',
    helper: 'Git repository name or URL',
    tooltip: (
        <>
            <p>
                Name of your GitHub repository or the URL of your Git repository. Supports GitHub, GitLab, and other Git
                platforms. Examples:
                <ul>
                    <li>GitHub: datahub-project/datahub or https://github.com/datahub-project/datahub</li>
                    <li>GitLab: https://gitlab.com/gitlab-org/gitlab</li>
                    <li>Other platforms: https://your-git-server.com/org/repo (Repository SSH Locator is required)</li>
                </ul>
            </p>
        </>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.git_info.repo',
    placeholder: 'datahub-project/datahub or https://github.com/datahub-project/datahub',
    rules: [{ required: true, message: 'Git Repository is required' }],
    required: true,
};

const deployKeyFieldPath = 'source.config.git_info.deploy_key';
export const LOOKML_GIT_INFO_DEPLOY_KEY: RecipeField = {
    name: 'git_info.deploy_key',
    label: 'Git Deploy Key',
    helper: 'SSH private key for repo access',
    tooltip: (
        <>
            An SSH private key that has been provisioned for read access on the Git repository where the LookML is
            defined.
            <div style={{ marginTop: 8 }}>
                Learn how to generate SSH keys for your Git platform:
                <ul style={{ marginTop: 4, marginBottom: 0 }}>
                    <li>
                        <a
                            href="https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account"
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            GitHub
                        </a>
                    </li>
                    <li>
                        <a href="https://docs.gitlab.com/ee/user/ssh.html" target="_blank" rel="noopener noreferrer">
                            GitLab
                        </a>
                    </li>
                    <li>Other Git platforms: Check your platform&apos;s documentation for SSH key setup</li>
                </ul>
            </div>
        </>
    ),
    type: FieldType.SECRET,
    fieldPath: 'source.config.git_info.deploy_key',
    placeholder: '-----BEGIN OPENSSH PRIVATE KEY-----\n...',
    rules: [{ required: true, message: 'Git Deploy Key is required' }],
    setValueOnRecipeOverride: (recipe: any, value: string) => {
        const valueWithNewLine = `${value}\n`;
        return setFieldValueOnRecipe(recipe, valueWithNewLine, deployKeyFieldPath);
    },
    required: true,
};

export const LOOKML_GIT_INFO_REPO_SSH_LOCATOR: RecipeField = {
    name: 'git_info.repo_ssh_locator',
    label: 'Repository SSH Locator',
    helper: 'SSH URL for non-GitHub/GitLab',
    tooltip: (
        <>
            The SSH URL to clone the repository. Required for Git platforms other than GitHub and GitLab (which are
            auto-inferred). Example: git@your-git-server.com:org/repo.git
        </>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.git_info.repo_ssh_locator',
    placeholder: 'git@your-git-server.com:org/repo.git',
    rules: [
        ({ getFieldValue }) => ({
            validator(_, value) {
                const repo = getFieldValue('git_info.repo');
                if (!repo) return Promise.resolve();

                // Check if it's GitHub or GitLab (these are auto-inferred)
                // If it starts with git@, it must be explicitly github.com or gitlab.com
                const isGitHubSSH = repo.startsWith('git@github.com:');
                const isGitLabSSH = repo.startsWith('git@gitlab.com:');

                // Check for other SSH formats (git@custom-server.com:...) - these are NOT GitHub/GitLab
                const isOtherSSH = repo.startsWith('git@') && !isGitHubSSH && !isGitLabSSH;

                const isGitHub =
                    repo.toLowerCase().includes('github.com') ||
                    (!repo.includes('://') && !repo.startsWith('git@') && repo.split('/').length === 2) ||
                    isGitHubSSH;
                const isGitLab = (repo.toLowerCase().includes('gitlab.com') && !isOtherSSH) || isGitLabSSH;

                if (!isGitHub && !isGitLab && !value) {
                    return Promise.reject(
                        new Error('Repository SSH Locator is required for Git platforms other than GitHub and GitLab'),
                    );
                }
                return Promise.resolve();
            },
        }),
    ],
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
    helper: 'URL to Looker instance',
    tooltip: 'Optional URL to your Looker instance. This is used to generate external URLs for models in your project.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.api.base_url',
    placeholder: 'https://looker.company.com',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Base URL')],
};

export const LOOKML_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Looker Client ID',
    helper: 'Looker API Client ID Required',
    tooltip: 'The Looker API Client ID. Required if Looker Base URL is present.',
    type: FieldType.SECRET,
    placeholder: 'client_id',
    fieldPath: 'source.config.api.client_id',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Client ID')],
};

export const LOOKML_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Looker Client Secret',
    helper: 'Looker API Client Secret Required',
    tooltip: 'A Looker API Client Secret. Required if Looker Base URL is present.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api.client_secret',
    placeholder: 'client_secret',
    rules: [({ getFieldValue }) => validateApiSection(getFieldValue, 'Client Secret')],
};

export const PROJECT_NAME: RecipeField = {
    name: 'project_name',
    label: 'LookML Project Name',
    helper: 'LookML project name in Looker',
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
    helper: 'Extract lineage from external sources',
    tooltip:
        'Extract lineage between Looker and the external upstream Data Sources (e.g. Data Warehouses or Databases).',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.parse_table_names_from_sql',
    rules: null,
};

export const CONNECTION_TO_PLATFORM_MAP_NAME: RecipeField = {
    name: 'name',
    label: 'Name',
    helper: 'Looker connection name',
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
    helper: 'Data Platform ID in DataHub',
    tooltip: 'The Data Platform ID in DataHub (e.g. snowflake, bigquery, redshift, mysql, postgres)',
    type: FieldType.TEXT,
    fieldPath: 'platform',
    placeholder: 'snowflake',
    rules: [{ required: true, message: 'Platform is required' }],
};

export const DEFAULT_DB: RecipeField = {
    name: 'default_db',
    label: 'Default Database',
    helper: 'Database for assets from connection',
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
    helper: 'Map Looker connections to platforms',
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
