import get from 'lodash/get';
import omit from 'lodash/omit';

import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';
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

const repositoriesFieldPath = 'source.config.repositories';
// Deprecated single-project field. Sources created before repositories existed
// still carry it, so the form reads it and moves it to repositories on save.
const legacyRepositoryFieldPath = 'source.config.repository';

export const GITLAB_DOCUMENTS_REPOSITORIES: RecipeField = {
    name: 'repositories',
    label: 'Projects',
    tooltip:
        "Projects to ingest, each as its full path ('group/subgroup/project') or a full GitLab project URL. Each project gets its own folder document.",
    type: FieldType.LIST,
    buttonLabel: 'Add project',
    fieldPath: repositoriesFieldPath,
    placeholder: 'group/project',
    // Required rules on a list item do not run when the list is empty, so the list validates itself.
    rules: [
        {
            validator(_: unknown, value: string[] | undefined) {
                if (value?.some((project) => project?.trim())) return Promise.resolve();
                return Promise.reject(new Error('Projects is required'));
            },
        },
    ],
    required: true,
    migrateOnEveryChange: true,
    getValueFromRecipeOverride: (recipe: any) => {
        const repositories: string[] = get(recipe, repositoriesFieldPath) || [];
        const legacyRepository: string | undefined = get(recipe, legacyRepositoryFieldPath);
        return legacyRepository ? [legacyRepository, ...repositories] : repositories;
    },
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(omit(recipe, legacyRepositoryFieldPath), values, repositoriesFieldPath),
};

export const GITLAB_DOCUMENTS_BRANCH: RecipeField = {
    name: 'branch',
    label: 'Branch',
    tooltip: 'Branch to read from in every project. Leave blank to read each project from its own default branch.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.branch',
    placeholder: 'Default branch',
    rules: null,
};

export const GITLAB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT: RecipeField = {
    ...GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT,
    label: 'Project folder document',
    tooltip:
        'When enabled, a folder document named after the project is created and imported files are nested beneath it.',
};
