import {
    DEFAULT_DOCUMENT_SIDEBAR_GROUP_BY,
    DOCUMENT_SIDEBAR_GROUP_BY_STORAGE_KEY,
    readStoredDocumentGroupBy,
    writeStoredDocumentGroupBy,
} from '@app/document/utils/documentSidebarGroupByStorage';
import { DOCUMENT_GROUP_BY } from '@app/document/utils/documentSidebarGrouping';

describe('documentSidebarGroupByStorage', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    it('defaults when nothing is stored', () => {
        expect(readStoredDocumentGroupBy()).toBe(DEFAULT_DOCUMENT_SIDEBAR_GROUP_BY);
    });

    it('reads and writes a sticky grouping choice', () => {
        writeStoredDocumentGroupBy(DOCUMENT_GROUP_BY.DOMAIN);
        expect(localStorage.getItem(DOCUMENT_SIDEBAR_GROUP_BY_STORAGE_KEY)).toBe(DOCUMENT_GROUP_BY.DOMAIN);
        expect(readStoredDocumentGroupBy()).toBe(DOCUMENT_GROUP_BY.DOMAIN);
    });

    it('ignores invalid stored values', () => {
        localStorage.setItem(DOCUMENT_SIDEBAR_GROUP_BY_STORAGE_KEY, 'not-a-group');
        expect(readStoredDocumentGroupBy()).toBe(DEFAULT_DOCUMENT_SIDEBAR_GROUP_BY);
    });
});
