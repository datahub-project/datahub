import { SortOrder } from '../../../types.generated';

export const RECOMMENDED = 'recommended';
export const NAME_FIELD = 'name';

export const DEFAULT_SORT_OPTION = RECOMMENDED;

export const SORT_OPTIONS = {
    [RECOMMENDED]: { label: 'Recommended', field: RECOMMENDED, sortOrder: SortOrder.Descending },
    [`${NAME_FIELD}_${SortOrder.Ascending}`]: { label: 'A to Z', field: NAME_FIELD, sortOrder: SortOrder.Ascending },
    [`${NAME_FIELD}_${SortOrder.Descending}`]: { label: 'Z to A', field: NAME_FIELD, sortOrder: SortOrder.Descending },
};
