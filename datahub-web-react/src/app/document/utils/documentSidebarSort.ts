import { SortCriterion, SortOrder } from '@types';

/** Document sidebar sort option values (also used as menu keys). */
export const DOCUMENT_SIDEBAR_SORT = {
    NAME_ASC: 'name_asc',
    NAME_DESC: 'name_desc',
    LAST_MODIFIED_DESC: 'last_modified_desc',
} as const;

export type DocumentSidebarSortValue = (typeof DOCUMENT_SIDEBAR_SORT)[keyof typeof DOCUMENT_SIDEBAR_SORT];

export const DEFAULT_DOCUMENT_SIDEBAR_SORT: DocumentSidebarSortValue = DOCUMENT_SIDEBAR_SORT.NAME_ASC;

/**
 * Maps sidebar sort selection to searchAcrossEntities sortCriterion.
 * Tree browse sorts name + lastModified client-side on DocumentTreeNode;
 * search mode uses this criterion against the index.
 */
export function documentSidebarSortToCriterion(sort: DocumentSidebarSortValue): SortCriterion {
    switch (sort) {
        case DOCUMENT_SIDEBAR_SORT.NAME_ASC:
            return { field: '_entityName', sortOrder: SortOrder.Ascending };
        case DOCUMENT_SIDEBAR_SORT.NAME_DESC:
            return { field: '_entityName', sortOrder: SortOrder.Descending };
        case DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC:
            return { field: 'lastModifiedAt', sortOrder: SortOrder.Descending };
        default: {
            const exhaustiveCheck: never = sort;
            throw new Error(`Unhandled document sidebar sort: ${exhaustiveCheck}`);
        }
    }
}
