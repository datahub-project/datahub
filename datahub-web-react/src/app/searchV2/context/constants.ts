import i18next from 'i18next';

import { SortOrder } from '@types';

export const RELEVANCE = 'relevance';
export const ENTITY_NAME_FIELD = '_entityName';
const LAST_MODIFIED_TIME_FIELD = 'lastModifiedAt';

export const DEFAULT_SORT_OPTION = RELEVANCE;

export function getSortOptions() {
    return {
        [RELEVANCE]: {
            label: i18next.t('search:sort.relevance'),
            field: RELEVANCE,
            sortOrder: SortOrder.Descending,
        },
        [`${ENTITY_NAME_FIELD}_${SortOrder.Ascending}`]: {
            label: i18next.t('search:sort.nameAtoZ'),
            field: ENTITY_NAME_FIELD,
            sortOrder: SortOrder.Ascending,
        },
        [`${ENTITY_NAME_FIELD}_${SortOrder.Descending}`]: {
            label: i18next.t('search:sort.nameZtoA'),
            field: ENTITY_NAME_FIELD,
            sortOrder: SortOrder.Descending,
        },
        [`${LAST_MODIFIED_TIME_FIELD}_${SortOrder.Descending}`]: {
            label: i18next.t('search:sort.lastModifiedInSource'),
            field: LAST_MODIFIED_TIME_FIELD,
            sortOrder: SortOrder.Descending,
        },
    };
}

/** @deprecated Use getSortOptions() instead so labels are translated at call time. */
export const SORT_OPTIONS = getSortOptions();
