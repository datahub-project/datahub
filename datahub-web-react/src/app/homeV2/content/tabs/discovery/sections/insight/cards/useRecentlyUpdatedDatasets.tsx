import dayjs from 'dayjs';

import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { UnionType } from '@app/searchV2/utils/constants';

import { FilterOperator, SortCriterion, SortOrder } from '@types';

export const buildRecentlyUpdatedDatasetsFilters = (sinceDays: number): FilterSet => {
    const startDate = dayjs().utcOffset(0).subtract(sinceDays, 'days').startOf('day');
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'lastModifiedAt',
                values: [startDate.valueOf().toString()],
                condition: FilterOperator.GreaterThanOrEqualTo,
                negated: false,
            },
        ],
    };
};

export const buildRecentlyUpdatedDatasetsSort = (): SortCriterion => {
    return {
        field: 'lastModifiedAt',
        sortOrder: SortOrder.Descending,
    };
};
