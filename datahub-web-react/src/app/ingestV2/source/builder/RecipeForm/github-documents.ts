import { get, omit, set } from 'lodash';

import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const GITHUB_DOCUMENTS_TOKEN: RecipeField = {
    name: 'github_token',
    label: 'GitHub token',
    tooltip: 'Personal access token or GitHub App installation token with repository read access.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.github_token',
    placeholder: 'ghp_xxxxxxxxxxxx',
    rules: null,
    required: true,
};

export const GITHUB_DOCUMENTS_REPOSITORY: RecipeField = {
    name: 'repository',
    label: 'Repository',
    tooltip: "Repository to ingest, as 'owner/repo' or a full GitHub URL.",
    type: FieldType.TEXT,
    fieldPath: 'source.config.repository',
    placeholder: 'org/repo',
    rules: null,
    required: true,
};

export const GITHUB_DOCUMENTS_BRANCH: RecipeField = {
    name: 'branch',
    label: 'Branch',
    tooltip: 'Git branch to read files from (defaults to main).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.branch',
    placeholder: 'main',
    rules: null,
    required: true,
};

export const GITHUB_DOCUMENTS_PATH_PREFIX: RecipeField = {
    name: 'path_prefix',
    label: 'Path prefix',
    tooltip: 'Only ingest files under this path (for example, docs).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.path_prefix',
    placeholder: 'docs',
    rules: null,
};

export const GITHUB_DOCUMENTS_FILE_EXTENSIONS: RecipeField = {
    name: 'file_extensions',
    label: 'File extensions',
    tooltip: 'Include the leading dot (for example, .md).',
    type: FieldType.LIST,
    fieldPath: 'source.config.file_extensions',
    placeholder: '.md',
    buttonLabel: 'Add extension',
    rules: null,
};

const createRepoRootDocumentFieldPath = 'source.config.create_repo_root_document';
const parentDocumentUrnFieldPath = 'source.config.parent_document_urn';

export const GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT: RecipeField = {
    name: 'create_repo_root_document',
    label: 'Create parent document with repository name',
    tooltip:
        'When enabled, a folder document named after the repository is created and imported files are nested beneath it.',
    type: FieldType.BOOLEAN,
    fieldPath: createRepoRootDocumentFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe) => {
        const createRepoRoot = get(recipe, createRepoRootDocumentFieldPath);
        if (createRepoRoot !== undefined) {
            return createRepoRoot;
        }

        const parentDocumentUrn = get(recipe, parentDocumentUrnFieldPath);
        if (parentDocumentUrn) {
            return false;
        }

        return true;
    },
    setValueOnRecipeOverride: (recipe, value) => {
        let updatedRecipe = set(recipe, createRepoRootDocumentFieldPath, value);
        if (value) {
            updatedRecipe = omit(updatedRecipe, parentDocumentUrnFieldPath);
        }
        return updatedRecipe;
    },
};
