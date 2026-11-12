import { describe, expect, it } from 'vitest';

import {
    GITLAB_DOCUMENTS_BASE_URL,
    GITLAB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT,
    GITLAB_DOCUMENTS_REPOSITORY,
    GITLAB_DOCUMENTS_TOKEN,
} from '@app/ingestV2/source/builder/RecipeForm/gitlab-documents';

describe('gitlab-documents recipe fields', () => {
    it('maps GitLab-specific fields to the gitlab-documents config', () => {
        expect(GITLAB_DOCUMENTS_TOKEN.fieldPath).toBe('source.config.gitlab_token');
        expect(GITLAB_DOCUMENTS_TOKEN.required).toBe(true);
        expect(GITLAB_DOCUMENTS_BASE_URL.fieldPath).toBe('source.config.base_url');
        expect(GITLAB_DOCUMENTS_REPOSITORY.fieldPath).toBe('source.config.repository');
        expect(GITLAB_DOCUMENTS_REPOSITORY.required).toBe(true);
    });

    it('keeps the repo root default and legacy parent_document_urn handling', () => {
        const rootField = GITLAB_DOCUMENTS_CREATE_REPO_ROOT_DOCUMENT;
        const legacyRecipe = { source: { config: { parent_document_urn: 'urn:li:document:parent' } } };

        expect(rootField.fieldPath).toBe('source.config.create_repo_root_document');
        expect(rootField.getValueFromRecipeOverride?.({ source: { config: {} } })).toBe(true);
        expect(rootField.getValueFromRecipeOverride?.(legacyRecipe)).toBe(false);
    });
});
