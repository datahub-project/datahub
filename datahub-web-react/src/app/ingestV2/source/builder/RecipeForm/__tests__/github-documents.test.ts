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

describe('github-documents recipe fields', () => {
    it('exports recipe field metadata for github-documents source', () => {
        expect(GITHUB_DOCUMENTS_TOKEN.fieldPath).toBe('source.config.github_token');
        expect(GITHUB_DOCUMENTS_REPOSITORY.required).toBe(true);
        expect(GITHUB_DOCUMENTS_BRANCH.placeholder).toBe('main');
        expect(GITHUB_DOCUMENTS_PATH_PREFIX.name).toBe('path_prefix');
        expect(GITHUB_DOCUMENTS_FILE_EXTENSIONS.type).toBe(FieldType.LIST);
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
