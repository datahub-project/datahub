import { describe, expect, it } from 'vitest';

import { shouldShowDocumentSearchResults } from '@app/homeV2/layout/sidebar/documents/shared/DocumentPopoverBase.utils';

describe('DocumentPopoverBase utilities', () => {
    it('shows flat results when searching or when the shared tree is empty', () => {
        expect(shouldShowDocumentSearchResults(true, 2)).toBe(true);
        expect(shouldShowDocumentSearchResults(false, 0)).toBe(true);
        expect(shouldShowDocumentSearchResults(false, 2)).toBe(false);
    });
});
