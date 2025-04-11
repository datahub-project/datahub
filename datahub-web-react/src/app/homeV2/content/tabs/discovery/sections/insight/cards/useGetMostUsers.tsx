import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

const MIN_USERS = '2';

export const buildMostUsersFilters = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'uniqueUserCountLast30DaysFeature',
                values: [MIN_USERS],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildMostUsersSort = (): SortCriterion => {
    return {
        field: 'uniqueUserCountLast30DaysFeature',
        sortOrder: SortOrder.Descending,
    };
};
