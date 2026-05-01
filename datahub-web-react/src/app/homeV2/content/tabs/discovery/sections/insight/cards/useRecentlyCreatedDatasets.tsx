import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { UnionType } from '@app/searchV2/utils/constants';
import dayjs from '@utils/dayjs';

import { FilterOperator, SortCriterion, SortOrder } from '@types';

export const buildRecentlyCreatedDatasetsFilters = (sinceDays: number): FilterSet => {
    const startDate = dayjs().utcOffset(0).subtract(sinceDays, 'days').startOf('day');
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
