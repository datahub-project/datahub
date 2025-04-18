import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

const MIN_UPDATES = '10';

export const buildMostUpdatedFilters = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'writeCountLast30DaysFeature',
                values: [MIN_UPDATES],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildMostUpdatedSort = (): SortCriterion => {
    return {
        field: 'writeCountLast30DaysFeature',
        sortOrder: SortOrder.Descending,
    };
};
