/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { LAST_MODIFIED_TIME_FIELD } from '@app/searchV2/context/constants';
import { UnionType } from '@app/searchV2/utils/constants';

import { FilterOperator, SortCriterion, SortOrder } from '@types';

const FOUNDATIONAL_ASSET_TAGS = ['urn:li:tag:__default_large_table', 'urn:li:tag:__default_high_queries'];

const FOUNDATIONAL_ASSET_EXCLUDE_TAGS = ['urn:li:tag:__default_no_queries', 'urn:li:tag:__default_low_changes'];

export const buildFoundationalAssetsFilters = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'tags',
                values: FOUNDATIONAL_ASSET_TAGS,
                condition: FilterOperator.Equal,
                negated: false,
            },
            {
                field: 'tags',
                values: FOUNDATIONAL_ASSET_EXCLUDE_TAGS,
                condition: FilterOperator.Equal,
                negated: true,
            },
        ],
    };
};

export const buildFoundationalAssetsSort = (): SortCriterion => {
    return {
        field: LAST_MODIFIED_TIME_FIELD,
        sortOrder: SortOrder.Descending,
    };
};
