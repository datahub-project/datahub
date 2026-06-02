import { describe, expect, it } from 'vitest';

import { buildGitHubDocumentsIngestionState } from '@app/context/import/buildGitHubDocumentsIngestionState';

describe('buildGitHubDocumentsIngestionState', () => {
    it('builds a github-documents recipe as JSON with native import mode', () => {
        const state = buildGitHubDocumentsIngestionState();

        expect(state.type).toBe('github-documents');
        expect(state.name).toBe('GitHub');
        expect(state.config?.recipe).toBeDefined();

        const recipe = JSON.parse(state.config!.recipe!);
        expect(recipe.source.type).toBe('github-documents');
        expect(recipe.source.config.document_import_mode).toBe('NATIVE');
        expect(recipe.source.config.parent_document_urn).toBeUndefined();
    });
});
