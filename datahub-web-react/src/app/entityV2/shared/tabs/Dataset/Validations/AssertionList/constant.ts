import { AssertionSourceType } from '@src/types.generated';
import { AssertionTable } from './types';

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
    },
};

export const ASSERTION_GROUP_BY_FILTER_OPTIONS = [
    { label: 'Type', value: 'type' },
    { label: 'Status', value: 'status' },
];

export const ASSERTION_SUMMARY_CARD_STATUSES = ['failing', 'passing', 'erroring'];

export const NO_RUNNING_STATE = 'notRunning';

export const ASSERTION_DEFAULT_RAW_DATA: AssertionTable = {
    assertions: [],
    groupBy: { type: [], status: [], column: [] },
    filterOptions: {},
    totalCount: 0,
    filteredCount: 0,
    searchMatchesCount: 0,
};

export const ASSERTION_SOURCES = [
    AssertionSourceType.Native,
    AssertionSourceType.Inferred,
    AssertionSourceType.External,
];

export const ASSERTION_FILTER_TYPES = {
    TAG: 'tags',
};
