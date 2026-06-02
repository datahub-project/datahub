import { describe, expect, it } from 'vitest';

import { buildConfluenceDocumentsIngestionState } from '@app/context/import/buildConfluenceDocumentsIngestionState';

describe('buildConfluenceDocumentsIngestionState', () => {
    it('builds a confluence recipe as JSON with native import mode for context docs', () => {
        const state = buildConfluenceDocumentsIngestionState();

        expect(state.type).toBe('confluence');
        expect(state.name).toBe('Confluence');

        const recipe = JSON.parse(state.config!.recipe!);
        expect(recipe.source.type).toBe('confluence');
        expect(recipe.source.config.document_import_mode).toBe('NATIVE');
        expect(recipe.source.config.parent_document_urn).toBeUndefined();
    });
});
