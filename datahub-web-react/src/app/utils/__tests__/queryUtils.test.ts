import { getHomePagePostsFilters } from '@app/utils/queryUtils';

import { PostContentType } from '@types';

describe('getHomePagePostsFilters', () => {
    it('filters to home page posts only by default', () => {
        expect(getHomePagePostsFilters()[0].and.map((filter) => filter.field)).toEqual(['type']);
    });

    it('adds a content type filter when one is given', () => {
        const contentFilter = getHomePagePostsFilters(PostContentType.Text)[0].and.find(
            (filter) => filter.field === 'contentType',
        );
        expect(contentFilter?.values).toEqual([PostContentType.Text]);
    });
});
