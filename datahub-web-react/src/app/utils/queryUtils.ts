import { FilterOperator, PostContentType } from '@types';

export function getHomePagePostsFilters(contentType?: PostContentType) {
    return [
        {
            and: [
                {
                    field: 'type',
                    condition: FilterOperator.Equal,
                    values: ['HOME_PAGE_ANNOUNCEMENT'],
                },
                ...(contentType
                    ? [
                          {
                              field: 'contentType',
                              condition: FilterOperator.Equal,
                              values: [contentType],
                          },
                      ]
                    : []),
            ],
        },
    ];
}
