import i18next from 'i18next';

import { SortOrder } from '@types';

export const RELEVANCE = 'relevance';
const ENTITY_NAME_FIELD = '_entityName';
const LAST_OPERATION_TIME_FIELD = 'lastOperationTime';

export function getSortOptions() {
    return {
        [RELEVANCE]: { label: i18next.t('search:sort.relevance'), field: RELEVANCE, sortOrder: SortOrder.Descending },
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
        [`${LAST_OPERATION_TIME_FIELD}_${SortOrder.Descending}`]: {
            label: i18next.t('search:sort.lastModifiedInSource'),
            field: LAST_OPERATION_TIME_FIELD,
            sortOrder: SortOrder.Descending,
        },
    };
}

/** @deprecated use getSortOptions() instead */
export const SORT_OPTIONS = {
    [RELEVANCE]: { label: 'Relevance (Default)', field: RELEVANCE, sortOrder: SortOrder.Descending },
    [`${ENTITY_NAME_FIELD}_${SortOrder.Ascending}`]: {
        label: 'Name A to Z',
        field: ENTITY_NAME_FIELD,
        sortOrder: SortOrder.Ascending,
    },
    [`${ENTITY_NAME_FIELD}_${SortOrder.Descending}`]: {
        label: 'Name Z to A',
        field: ENTITY_NAME_FIELD,
        sortOrder: SortOrder.Descending,
    },
    [`${LAST_OPERATION_TIME_FIELD}_${SortOrder.Descending}`]: {
        label: 'Last Modified In Source',
        field: LAST_OPERATION_TIME_FIELD,
        sortOrder: SortOrder.Descending,
    },
};
