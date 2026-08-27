import { AssertionSourceType } from '@src/types.generated';

export const ASSERTION_DEFAULT_FILTERS = {
    sortBy: '',
    groupBy: 'type',
    filterCriteria: {
        searchText: '',
        status: [],
        type: [],
        tags: [],
        column: [],
        source: [],
        owners: [],
    },
};

export const ASSERTION_DEFAULT_RAW_DATA = {
    assertions: [],
    groupBy: {
        type: [],
        status: [],
        column: [],
    },
};

export const ASSERTION_SUMMARY_CARD_STATUSES = ['failing', 'passing', 'erroring', 'initializing', 'notRunning'];

export const NO_RUNNING_STATE = 'notRunning';

// In OSS every assertion is external (produced via API / ingestion sources). Native and inferred
// ("smart") assertions are DataHub Cloud only, so External is the only source shown.
export const ASSERTION_SOURCES = [AssertionSourceType.External];

export const ASSERTION_FILTER_TYPES = {
    TAG: 'tags',
};
