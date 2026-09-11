import {
    clearBrowseNavigationFilters,
    getEntitySubtypeFiltersForEntity,
    hasBrowseNavigationFilter,
} from '@app/searchV2/sidebar/browseContextUtils';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';

describe('browseContextUtils', () => {
    it('should remove any different entity types from the filter and keep anything related to our given entityType', () => {
        const existingFilters = [
            {
                field: ENTITY_SUB_TYPE_FILTER_NAME,
                values: ['DATASET␞table', 'CONTAINER', 'DATASET', 'CHART', 'DATASET␞view'],
            },
            {
                field: TAGS_FILTER_NAME,
                values: ['urn:li:tag:test'],
            },
        ];
        const entitySubtypeFilters = getEntitySubtypeFiltersForEntity('DATASET', existingFilters);

        expect(entitySubtypeFilters).toMatchObject(['DATASET␞table', 'DATASET', 'DATASET␞view']);
    });

    it('treats platform, path, and environment as browse navigation filters', () => {
        expect(hasBrowseNavigationFilter([{ field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] }])).toBe(false);
        expect(
            hasBrowseNavigationFilter([{ field: PLATFORM_FILTER_NAME, values: ['urn:li:dataPlatform:snowflake'] }]),
        ).toBe(true);
    });

    it('clears browse navigation filters and keeps unrelated facets', () => {
        expect(
            clearBrowseNavigationFilters([
                { field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] },
                { field: PLATFORM_FILTER_NAME, values: ['urn:li:dataPlatform:snowflake'] },
                { field: BROWSE_PATH_V2_FILTER_NAME, values: ['␟analytics'] },
                { field: ORIGIN_FILTER_NAME, values: ['PROD'] },
            ]),
        ).toEqual([{ field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] }]);
    });
});
