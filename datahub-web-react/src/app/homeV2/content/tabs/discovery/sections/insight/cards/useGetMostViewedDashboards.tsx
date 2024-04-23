import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

const MIN_QUERIES = '1';

export const buildMostViewedDashboardsFilter = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'viewCountLast30DaysFeature',
                values: [MIN_QUERIES],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildMostViewedDashboardsSort = (): SortCriterion => {
    return {
        field: 'viewCountLast30DaysFeature',
        sortOrder: SortOrder.Descending,
    };
};
