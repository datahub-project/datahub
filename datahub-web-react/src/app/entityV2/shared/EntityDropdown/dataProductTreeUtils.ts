import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { EntityType, FilterOperator, SortOrder } from '@types';

export const DATA_PRODUCT_TREE_PAGE_SIZE = 50;

export function getDataProductRootsScrollInput(scrollId: string | null, count: number = DATA_PRODUCT_TREE_PAGE_SIZE) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.DataProduct],
            count,
            orFilters: [
                {
                    and: [
                        {
                            field: 'hasParentDataProduct',
                            condition: FilterOperator.Equal,
                            values: ['false'],
                        },
                    ],
                },
            ],
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

export function getDataProductChildrenScrollInput(
    parentUrn: string,
    scrollId: string | null,
    count: number = DATA_PRODUCT_TREE_PAGE_SIZE,
) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.DataProduct],
            count,
            orFilters: [{ and: [{ field: 'parentDataProduct', values: [parentUrn] }] }],
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}
