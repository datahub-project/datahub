import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DOCUMENT_SIDEBAR_SORT,
    documentSidebarSortToCriterion,
} from '@app/document/utils/documentSidebarSort';

import { SortOrder } from '@types';

describe('documentSidebarSort', () => {
    it('maps name and lastModified selections to search sortCriterion', () => {
        expect(documentSidebarSortToCriterion(DOCUMENT_SIDEBAR_SORT.NAME_ASC)).toEqual({
            field: '_entityName',
            sortOrder: SortOrder.Ascending,
        });
        expect(documentSidebarSortToCriterion(DOCUMENT_SIDEBAR_SORT.NAME_DESC)).toEqual({
            field: '_entityName',
            sortOrder: SortOrder.Descending,
        });
        expect(documentSidebarSortToCriterion(DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC)).toEqual({
            field: 'lastModifiedAt',
            sortOrder: SortOrder.Descending,
        });
    });

    it('defaults to last modified (newest first)', () => {
        expect(DEFAULT_DOCUMENT_SIDEBAR_SORT).toBe(DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC);
    });
});
