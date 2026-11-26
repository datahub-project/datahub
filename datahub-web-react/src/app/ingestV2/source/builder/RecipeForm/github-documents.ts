import get from 'lodash/get';
import omit from 'lodash/omit';
import set from 'lodash/set';

import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import {
    REPOSITORY_DOCUMENTS_BRANCH,
    REPOSITORY_DOCUMENTS_FILE_EXTENSIONS,
    REPOSITORY_DOCUMENTS_PATH_PREFIX,
} from '@app/ingestV2/source/builder/RecipeForm/repository-documents';

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

/** @deprecated Use REPOSITORY_DOCUMENTS_BRANCH. Kept for one release. */
export const GITHUB_DOCUMENTS_BRANCH = REPOSITORY_DOCUMENTS_BRANCH;
/** @deprecated Use REPOSITORY_DOCUMENTS_PATH_PREFIX. Kept for one release. */
export const GITHUB_DOCUMENTS_PATH_PREFIX = REPOSITORY_DOCUMENTS_PATH_PREFIX;
/** @deprecated Use REPOSITORY_DOCUMENTS_FILE_EXTENSIONS. Kept for one release. */
export const GITHUB_DOCUMENTS_FILE_EXTENSIONS = REPOSITORY_DOCUMENTS_FILE_EXTENSIONS;

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

const createRepoRootDocumentFieldPath = 'source.config.create_repo_root_document';
const parentDocumentUrnFieldPath = 'source.config.parent_document_urn';

export const GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT: RecipeField = {
    name: 'create_repo_root_document',
    label: 'Repository folder document',
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
