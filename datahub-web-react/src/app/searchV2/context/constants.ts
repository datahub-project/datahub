import { SortOrder } from '../../../types.generated';

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

// Extensions
export const ROW_COUNT_FIELD = 'rowCountFeature';
export const SIZE_IN_BYTES_FIELD = 'sizeInBytesFeature';
export const QUERY_COUNT = 'queryCountLast30DaysFeature';
export const UPDATE_COUNT = 'writeCountLast30DaysFeature';

export const DATASET_FEATURES_SORT_OPTIONS = {
    [`${ROW_COUNT_FIELD}_${SortOrder.Descending}`]: {
        label: 'Table Row Count',
        field: ROW_COUNT_FIELD,
        sortOrder: SortOrder.Descending,
    },
    [`${SIZE_IN_BYTES_FIELD}_${SortOrder.Descending}`]: {
        label: 'Table Size (Bytes)',
        field: SIZE_IN_BYTES_FIELD,
        sortOrder: SortOrder.Descending,
    },
    [`${QUERY_COUNT}_${SortOrder.Descending}`]: {
        label: 'Table Query Count (Last 30 Days)',
        field: QUERY_COUNT,
        sortOrder: SortOrder.Descending,
    },
    [`${UPDATE_COUNT}_${SortOrder.Descending}`]: {
        label: 'Table Update Count (Last 30 Days)',
        field: UPDATE_COUNT,
        sortOrder: SortOrder.Descending,
    },
};
