import { SortOrder } from '@types';

export const RELEVANCE = 'relevance';
export const ENTITY_NAME_FIELD = '_entityName';
export const LAST_MODIFIED_TIME_FIELD = 'lastModifiedAt';

export const DEFAULT_SORT_OPTION = RELEVANCE;

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
    [`${LAST_MODIFIED_TIME_FIELD}_${SortOrder.Descending}`]: {
        label: 'Last Modified Time (In Source)',
        field: LAST_MODIFIED_TIME_FIELD,
        sortOrder: SortOrder.Descending,
    },
};
