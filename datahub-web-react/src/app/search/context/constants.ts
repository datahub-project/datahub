import { SortOrder } from '../../../types.generated';

export const RELEVANCE = 'relevance';
export const NAME_FIELD = 'name';
export const LAST_OPERATION_TIME_FIELD = 'lastOperationTime';

export const DEFAULT_SORT_OPTION = RELEVANCE;

export const SORT_OPTIONS = {
    [RELEVANCE]: { label: 'Relevance', field: RELEVANCE, sortOrder: SortOrder.Descending },
    [`${NAME_FIELD}_${SortOrder.Ascending}`]: { label: 'A to Z', field: NAME_FIELD, sortOrder: SortOrder.Ascending },
    [`${NAME_FIELD}_${SortOrder.Descending}`]: { label: 'Z to A', field: NAME_FIELD, sortOrder: SortOrder.Descending },
    [`${LAST_OPERATION_TIME_FIELD}_${SortOrder.Descending}`]: {
        label: 'Last Modified in Platform',
        field: LAST_OPERATION_TIME_FIELD,
        sortOrder: SortOrder.Descending,
    },
};
