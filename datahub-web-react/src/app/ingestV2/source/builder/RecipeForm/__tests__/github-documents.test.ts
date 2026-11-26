import { describe, expect, it } from 'vitest';

import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import {
    GITHUB_DOCUMENTS_BRANCH,
    GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT,
    GITHUB_DOCUMENTS_FILE_EXTENSIONS,
    GITHUB_DOCUMENTS_PATH_PREFIX,
    GITHUB_DOCUMENTS_REPOSITORY,
    GITHUB_DOCUMENTS_TOKEN,
} from '@app/ingestV2/source/builder/RecipeForm/github-documents';
import {
    REPOSITORY_DOCUMENTS_BRANCH,
    REPOSITORY_DOCUMENTS_FILE_EXTENSIONS,
    REPOSITORY_DOCUMENTS_PATH_PREFIX,
} from '@app/ingestV2/source/builder/RecipeForm/repository-documents';

describe('github-documents recipe fields', () => {
    it('keeps the deprecated field names as aliases of the shared fields', () => {
        expect(GITHUB_DOCUMENTS_BRANCH).toBe(REPOSITORY_DOCUMENTS_BRANCH);
        expect(GITHUB_DOCUMENTS_PATH_PREFIX).toBe(REPOSITORY_DOCUMENTS_PATH_PREFIX);
        expect(GITHUB_DOCUMENTS_FILE_EXTENSIONS).toBe(REPOSITORY_DOCUMENTS_FILE_EXTENSIONS);
    });

    it('exports recipe field metadata for github-documents source', () => {
        expect(GITHUB_DOCUMENTS_TOKEN.fieldPath).toBe('source.config.github_token');
        expect(GITHUB_DOCUMENTS_REPOSITORY.required).toBe(true);
        expect(REPOSITORY_DOCUMENTS_BRANCH.placeholder).toBe('main');
        expect(REPOSITORY_DOCUMENTS_PATH_PREFIX.name).toBe('path_prefix');
        expect(REPOSITORY_DOCUMENTS_FILE_EXTENSIONS.type).toBe(FieldType.LIST);
        expect(GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT.type).toBe(FieldType.BOOLEAN);
        expect(GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT.tooltip).toContain('folder document');
    });

    it('defaults create repo root checkbox to true for new recipes', () => {
        const recipe = {
            source: {
                config: {},
            },
        };

        expect(GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT.getValueFromRecipeOverride?.(recipe)).toBe(true);
    });

    it('maps legacy parent_document_urn recipes to unchecked repo root checkbox', () => {
        const recipe = {
            source: {
                config: {
                    parent_document_urn: 'urn:li:document:parent',
                },
            },
        };

        expect(GITHUB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT.getValueFromRecipeOverride?.(recipe)).toBe(false);
    });
});
