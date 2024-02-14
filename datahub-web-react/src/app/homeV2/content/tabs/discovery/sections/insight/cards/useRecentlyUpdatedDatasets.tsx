import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

export const buildRecentlyUpdatedDatasetsFilters = (maxAgeMs: number): FilterSet => {
    const filterTimeMs = Date.now() - maxAgeMs;
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'lastModifiedAt',
                values: [filterTimeMs.toString()],
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
