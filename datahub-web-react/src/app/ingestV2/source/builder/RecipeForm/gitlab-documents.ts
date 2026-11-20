import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import { GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT } from '@app/ingestV2/source/builder/RecipeForm/github-documents';

export const GITLAB_DOCUMENTS_TOKEN: RecipeField = {
    name: 'gitlab_token',
    label: 'GitLab token',
    tooltip:
        'Personal, project, or group access token with the read_api scope. Project and group tokens need at least the Reporter role.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.gitlab_token',
    placeholder: 'glpat-xxxxxxxxxxxx',
    rules: null,
    required: true,
};

export const GITLAB_DOCUMENTS_BASE_URL: RecipeField = {
    name: 'base_url',
    label: 'GitLab URL',
    tooltip:
        'URL of your GitLab instance. Leave as https://gitlab.com for GitLab.com, or set your self-managed URL. Inferred from the project when that is a full URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.base_url',
    placeholder: 'https://gitlab.com',
    rules: null,
};

export const GITLAB_DOCUMENTS_REPOSITORY: RecipeField = {
    name: 'repository',
    label: 'Project',
    tooltip: "Project to ingest, as its full path ('group/subgroup/project') or a full GitLab project URL.",
    type: FieldType.TEXT,
    fieldPath: 'source.config.repository',
    placeholder: 'group/project',
    rules: null,
    required: true,
};

export const GITLAB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT: RecipeField = {
    ...GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT,
    label: 'Project folder document',
    tooltip:
        'When enabled, a folder document named after the project is created and imported files are nested beneath it.',
};
