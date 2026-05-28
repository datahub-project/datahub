import { describe, expect, it } from 'vitest';

import { buildConfluenceDocumentsIngestionState } from '@app/context/import/buildConfluenceDocumentsIngestionState';

describe('buildConfluenceDocumentsIngestionState', () => {
    it('defaults document import mode to EXTERNAL', () => {
        const state = buildConfluenceDocumentsIngestionState({});
        expect(state.config?.recipe).toContain('document_import_mode: EXTERNAL');
    });
});
