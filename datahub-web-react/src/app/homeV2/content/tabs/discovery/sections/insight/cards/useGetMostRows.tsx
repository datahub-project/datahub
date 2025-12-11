/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { UnionType } from '@app/searchV2/utils/constants';

import { FilterOperator, SortCriterion, SortOrder } from '@types';

const MIN_ROWS = '100';

export const buildMostRowsFilters = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'rowCount',
                values: [MIN_ROWS],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildMostRowsSort = (): SortCriterion => {
    return {
        field: 'rowCount',
        sortOrder: SortOrder.Descending,
    };
};
