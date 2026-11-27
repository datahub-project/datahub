import { describe, expect, it } from 'vitest';

import { buildGitLabDocumentsIngestionState } from '@app/context/import/buildGitLabDocumentsIngestionState';

describe('buildGitLabDocumentsIngestionState', () => {
    it('builds a gitlab-documents recipe as JSON with native import mode', () => {
        const state = buildGitLabDocumentsIngestionState();

        expect(state.type).toBe('gitlab-documents');
        expect(state.name).toBe('GitLab');

        const recipe = JSON.parse(state.config?.recipe ?? '{}');
        expect(recipe.source.type).toBe('gitlab-documents');
        expect(recipe.source.config.base_url).toBe('https://gitlab.com');
        // repositories, not the deprecated repository; no branch, so each project
        // is read from its own default branch.
        expect(recipe.source.config.repositories).toEqual([]);
        expect(recipe.source.config.repository).toBeUndefined();
        expect(recipe.source.config.branch).toBeUndefined();
        expect(recipe.source.config.document_import_mode).toBe('NATIVE');
        expect(recipe.source.config.create_repo_root_document).toBe(true);
        expect(recipe.source.config.stateful_ingestion).toEqual({ enabled: true });
        expect(recipe.sink).toBeUndefined();
    });
});
