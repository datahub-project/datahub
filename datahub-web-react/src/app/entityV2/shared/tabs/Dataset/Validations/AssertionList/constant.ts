export const DEFAULT_FILTERS = {
    sortBy: '',
    groupBy: '',
    filterCriteria: {
        searchText: '',
        status: [],
        type: [],
        tags: [],
        columns: [],
        others: [],
    },
};

export const ASSERTION_GROUP_BY_FILTER_OPTIONS = [
    { label: 'Type', value: 'type' },
    { label: 'Status', value: 'status' },
];

export const ASSERTION_SUMMARY_CARD_STATUSES = ['failing', 'passing', 'erroring'];
