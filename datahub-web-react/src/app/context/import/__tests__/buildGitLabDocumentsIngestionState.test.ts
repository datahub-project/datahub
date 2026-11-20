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
        expect(recipe.source.config.document_import_mode).toBe('NATIVE');
        expect(recipe.source.config.create_repo_root_document).toBe(true);
        expect(recipe.source.config.stateful_ingestion).toEqual({ enabled: true });
        expect(recipe.sink).toBeUndefined();
    });
});
