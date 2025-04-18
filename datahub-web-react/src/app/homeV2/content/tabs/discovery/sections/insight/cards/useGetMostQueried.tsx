import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

const MIN_QUERIES = '10';

export const buildMostQueriedFilters = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'usageCountLast30DaysFeature',
                values: [MIN_QUERIES],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildMostQueriedSort = (): SortCriterion => {
    return {
        field: 'usageCountLast30DaysFeature',
        sortOrder: SortOrder.Descending,
    };
};
