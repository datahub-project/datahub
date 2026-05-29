import { describe, expect, it } from 'vitest';

import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import {
    GITHUB_DOCUMENTS_BRANCH,
    GITHUB_DOCUMENTS_FILE_EXTENSIONS,
    GITHUB_DOCUMENTS_PARENT_DOCUMENT_URN,
    GITHUB_DOCUMENTS_PATH_PREFIX,
    GITHUB_DOCUMENTS_REPOSITORY,
    GITHUB_DOCUMENTS_SHOW_IN_GLOBAL_CONTEXT,
    GITHUB_DOCUMENTS_TOKEN,
} from '@app/ingestV2/source/builder/RecipeForm/github-documents';

describe('github-documents recipe fields', () => {
    it('exports recipe field metadata for github-documents source', () => {
        expect(GITHUB_DOCUMENTS_TOKEN.fieldPath).toBe('source.config.github_token');
        expect(GITHUB_DOCUMENTS_REPOSITORY.required).toBe(true);
        expect(GITHUB_DOCUMENTS_BRANCH.placeholder).toBe('main');
        expect(GITHUB_DOCUMENTS_PATH_PREFIX.name).toBe('path_prefix');
        expect(GITHUB_DOCUMENTS_FILE_EXTENSIONS.type).toBe(FieldType.LIST);
        expect(GITHUB_DOCUMENTS_PARENT_DOCUMENT_URN.tooltip).toContain('repository root');
        expect(GITHUB_DOCUMENTS_SHOW_IN_GLOBAL_CONTEXT.type).toBe(FieldType.BOOLEAN);
    });
});
