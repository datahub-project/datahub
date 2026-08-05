import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { SortCriterion, SortOrder } from '@types';

/** Document sidebar sort option values (also used as menu keys). */
export const DOCUMENT_SIDEBAR_SORT = {
    NAME_ASC: 'name_asc',
    NAME_DESC: 'name_desc',
    LAST_MODIFIED_DESC: 'last_modified_desc',
} as const;

export type DocumentSidebarSortValue = (typeof DOCUMENT_SIDEBAR_SORT)[keyof typeof DOCUMENT_SIDEBAR_SORT];

export const DEFAULT_DOCUMENT_SIDEBAR_SORT: DocumentSidebarSortValue = DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC;

/**
 * Same fields as global search Name A–Z / Z–A / Last modified ({@link ENTITY_NAME_FIELD}).
 * Pass via searchDocuments sortInput — never reorder client-side.
 */
export function documentSidebarSortToCriterion(sort: DocumentSidebarSortValue): SortCriterion {
    switch (sort) {
        case DOCUMENT_SIDEBAR_SORT.NAME_ASC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending };
        case DOCUMENT_SIDEBAR_SORT.NAME_DESC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Descending };
        case DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC:
            return { field: 'lastModifiedAt', sortOrder: SortOrder.Descending };
        default: {
            const exhaustiveCheck: never = sort;
            throw new Error(`Unhandled document sidebar sort: ${exhaustiveCheck}`);
        }
    }
}
