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
