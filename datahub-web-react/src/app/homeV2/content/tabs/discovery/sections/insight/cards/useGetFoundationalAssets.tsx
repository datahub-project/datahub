import { FilterOperator, SortCriterion, SortOrder } from '../../../../../../../../types.generated';
import { FilterSet } from '../../../../../../../entityV2/shared/components/styled/search/types';
import { LAST_MODIFIED_TIME_FIELD } from '../../../../../../../searchV2/context/constants';
import { UnionType } from '../../../../../../../searchV2/utils/constants';

const FOUNDATIONAL_ASSET_TAGS = ['urn:li:tag:__default_large_table', 'urn:li:tag:__default_high_queries'];

const FOUNDATIONAL_ASSET_EXCLUDE_TAGS = ['urn:li:tag:__default_no_queries', 'urn:li:tag:__default_low_changes'];

export const buildFoundationalAssetsFilters = (): FilterSet => {
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: 'tags',
                values: FOUNDATIONAL_ASSET_TAGS,
                condition: FilterOperator.Equal,
                negated: false,
            },
            {
                field: 'tags',
                values: FOUNDATIONAL_ASSET_EXCLUDE_TAGS,
                condition: FilterOperator.Equal,
                negated: true,
            },
        ],
    };
};

export const buildFoundationalAssetsSort = (): SortCriterion => {
    return {
        field: LAST_MODIFIED_TIME_FIELD,
        sortOrder: SortOrder.Descending,
    };
};
