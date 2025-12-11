/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { AssertionTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
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
