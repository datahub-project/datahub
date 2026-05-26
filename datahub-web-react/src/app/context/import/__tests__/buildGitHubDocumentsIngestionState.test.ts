import { describe, expect, it } from 'vitest';

import { buildGitHubDocumentsIngestionState } from '@app/context/import/buildGitHubDocumentsIngestionState';

describe('buildGitHubDocumentsIngestionState', () => {
    it('builds a github-documents recipe with parent urn', () => {
        const state = buildGitHubDocumentsIngestionState({
            parentDocumentUrn: 'urn:li:document:parent',
        });

        expect(state.type).toBe('github-documents');
        expect(state.config?.recipe).toContain('parent_document_urn: "urn:li:document:parent"');
        expect(state.config?.recipe).toContain('document_import_mode: NATIVE');
    });

    it('uses null parent when not provided', () => {
        const state = buildGitHubDocumentsIngestionState({});
        expect(state.config?.recipe).toContain('parent_document_urn: null');
    });
});
