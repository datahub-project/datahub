import { describe, expect, it } from 'vitest';

import { buildNotionDocumentsIngestionState } from '@app/context/import/buildNotionDocumentsIngestionState';

describe('buildNotionDocumentsIngestionState', () => {
    it('builds a notion recipe as JSON with native import mode for context docs', () => {
        const state = buildNotionDocumentsIngestionState();

        expect(state.type).toBe('notion');
        expect(state.name).toBe('Notion');

        const recipe = JSON.parse(state.config!.recipe!);
        expect(recipe.source.type).toBe('notion');
        expect(recipe.source.config.document_import_mode).toBe('NATIVE');
        expect(recipe.source.config.parent_document_urn).toBeUndefined();
    });
});
