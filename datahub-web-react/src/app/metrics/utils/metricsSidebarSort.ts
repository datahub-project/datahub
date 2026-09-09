import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { SortCriterion, SortOrder } from '@types';

/** Metrics sidebar sort option values (also used as menu keys). */
export const METRICS_SIDEBAR_SORT = {
    NAME_ASC: 'name_asc',
    NAME_DESC: 'name_desc',
    LAST_MODIFIED_DESC: 'last_modified_desc',
} as const;

export type MetricsSidebarSortValue = (typeof METRICS_SIDEBAR_SORT)[keyof typeof METRICS_SIDEBAR_SORT];

/** Default keeps today's Metrics tree alphabetical browse behavior. */
export const DEFAULT_METRICS_SIDEBAR_SORT: MetricsSidebarSortValue = METRICS_SIDEBAR_SORT.NAME_ASC;

/**
 * Same fields as Documents / global search Name A–Z / Z–A / Last modified.
 * Pass via scrollAcrossEntities sortInput — never reorder client-side.
 */
export function metricsSidebarSortToCriterion(sort: MetricsSidebarSortValue): SortCriterion {
    switch (sort) {
        case METRICS_SIDEBAR_SORT.NAME_ASC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending };
        case METRICS_SIDEBAR_SORT.NAME_DESC:
            return { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Descending };
        case METRICS_SIDEBAR_SORT.LAST_MODIFIED_DESC:
            return { field: 'lastModifiedAt', sortOrder: SortOrder.Descending };
        default: {
            const exhaustiveCheck: never = sort;
            throw new Error(`Unhandled metrics sidebar sort: ${exhaustiveCheck}`);
        }
    }
}
