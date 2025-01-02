import { FilterOperator } from '../../types.generated';

export function getHomePagePostsFilters() {
    return [
        {
            and: [
                {
                    field: 'type',
                    condition: FilterOperator.Equal,
                    values: ['HOME_PAGE_ANNOUNCEMENT'],
                },
            ],
        },
    ];
}
