import { EntityType, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { ENTITY_FILTER_NAME, UnionType } from '../../../../../../../searchV2/utils/constants';

export const buildMostViewedDashboardsFilter = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [{ field: ENTITY_FILTER_NAME, values: [EntityType.Dashboard] }],
    };
};

export const buildMostViewedDashboardsSort = (): SortCriterion => {
    return {
        field: 'viewCountLast30DaysFeature',
        sortOrder: SortOrder.Descending,
    };
};
