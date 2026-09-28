import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { CREATED_TIME_FIELD_NAME } from '@app/searchV2/utils/constants';

import { SortCriterion, SortOrder } from '@types';

/** Domain sidebar sort option values (also used as menu keys). */
export const DOMAIN_SIDEBAR_SORT = {
    NAME_ASC: 'name_asc',
    NAME_DESC: 'name_desc',
    CREATED_DESC: 'created_desc',
} as const;

export type DomainSidebarSortValue = (typeof DOMAIN_SIDEBAR_SORT)[keyof typeof DOMAIN_SIDEBAR_SORT];

/** Preserve historical tree order (name A–Z) as the default. */
export const DEFAULT_DOMAIN_SIDEBAR_SORT: DomainSidebarSortValue = DOMAIN_SIDEBAR_SORT.NAME_ASC;

/**
 * Domains index `_entityName` and `createdTime` (searchLabel createdAt) on
 * DomainProperties — there is no searchable lastModifiedAt.
 * Pass via scrollAcrossEntities sortInput — never reorder client-side.
 */
export function domainSidebarSortToCriterion(sort: DomainSidebarSortValue): SortCriterion {
    switch (sort) {
        case DOMAIN_SIDEBAR_SORT.NAME_ASC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending };
        case DOMAIN_SIDEBAR_SORT.NAME_DESC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Descending };
        case DOMAIN_SIDEBAR_SORT.CREATED_DESC:
            return { field: CREATED_TIME_FIELD_NAME, sortOrder: SortOrder.Descending };
        default: {
            const exhaustiveCheck: never = sort;
            throw new Error(`Unhandled domain sidebar sort: ${exhaustiveCheck}`);
        }
    }
}
