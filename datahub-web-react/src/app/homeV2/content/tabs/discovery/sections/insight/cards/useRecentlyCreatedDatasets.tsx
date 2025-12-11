/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import moment from 'moment/moment';

import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { UnionType } from '@app/searchV2/utils/constants';

import { FilterOperator, SortCriterion, SortOrder } from '@types';

export const buildRecentlyCreatedDatasetsFilters = (sinceDays: number): FilterSet => {
    const startDate = moment().utcOffset(0).subtract(sinceDays, 'days').set({
        hour: 0,
        minute: 0,
        second: 0,
        millisecond: 0,
    });
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'createdAt',
                values: [startDate.valueOf().toString()],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildRecentlyCreatedDatasetsSort = (): SortCriterion => {
    return {
        field: 'createdAt',
        sortOrder: SortOrder.Descending,
    };
};
