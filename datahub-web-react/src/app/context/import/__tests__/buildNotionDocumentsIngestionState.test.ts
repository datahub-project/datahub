import { describe, expect, it } from 'vitest';

import { buildNotionDocumentsIngestionState } from '@app/context/import/buildNotionDocumentsIngestionState';

describe('buildNotionDocumentsIngestionState', () => {
    it('defaults document import mode to EXTERNAL', () => {
        const state = buildNotionDocumentsIngestionState({});
        expect(state.config?.recipe).toContain('document_import_mode: EXTERNAL');
    });
});
