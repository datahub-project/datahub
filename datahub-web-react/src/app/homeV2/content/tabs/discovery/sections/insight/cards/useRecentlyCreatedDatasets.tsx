import moment from 'moment/moment';
import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

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
