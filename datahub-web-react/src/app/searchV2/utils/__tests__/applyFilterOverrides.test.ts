import { applyFacetFilterOverrides, applyOrFilterOverrides } from '@app/searchV2/utils/applyFilterOverrides';
import {
    CONTAINER_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    UnionType,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

describe('applyFacetFilterOverrides', () => {
    it('should concatenate overrides to an empty list', () => {
        const filterOverrides = [
            { field: PLATFORM_FILTER_NAME, values: ['looker'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
        ];
        const baseFilters = [];
        const orFiltersWithOverrides = applyFacetFilterOverrides(baseFilters, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            { field: PLATFORM_FILTER_NAME, values: ['looker'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
        ]);
    });

    it('should apply new overrides to the end of the list', () => {
        const filterOverrides = [{ field: ORIGIN_FILTER_NAME, values: ['PROD'] }];
        const baseFilters = [
            { field: PLATFORM_FILTER_NAME, values: ['looker'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
        ];
        const orFiltersWithOverrides = applyFacetFilterOverrides(baseFilters, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            { field: PLATFORM_FILTER_NAME, values: ['looker'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
            { field: ORIGIN_FILTER_NAME, values: ['PROD'] },
        ]);
    });

    it('should replace existing filters and concatenate new ones', () => {
        const filterOverrides = [
            { field: PLATFORM_FILTER_NAME, values: ['hive'] },
            { field: ORIGIN_FILTER_NAME, values: ['PROD'] },
        ];
        const baseFilters = [
            { field: PLATFORM_FILTER_NAME, values: ['looker'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
        ];
        const orFiltersWithOverrides = applyFacetFilterOverrides(baseFilters, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            { field: PLATFORM_FILTER_NAME, values: ['hive'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
            { field: ORIGIN_FILTER_NAME, values: ['PROD'] },
        ]);
    });
});

describe('applyOrFilterOverrides', () => {
    it('should apply overrides to each OR block of orFilters', () => {
        const filterOverrides = [{ field: ORIGIN_FILTER_NAME, values: ['PROD'] }];
        const baseFilters = [
            { field: PLATFORM_FILTER_NAME, values: ['looker'] },
            { field: CONTAINER_FILTER_NAME, values: ['parent'] },
        ];
        const orFiltersWithoutOverrides = generateOrFilters(UnionType.OR, baseFilters);
        const orFiltersWithOverrides = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            {
                and: [
                    { field: PLATFORM_FILTER_NAME, values: ['looker'] },
                    { field: ORIGIN_FILTER_NAME, values: ['PROD'] },
                ],
            },
            {
                and: [
                    { field: CONTAINER_FILTER_NAME, values: ['parent'] },
                    { field: ORIGIN_FILTER_NAME, values: ['PROD'] },
                ],
            },
        ]);
    });

    it('should apply overrides to empty orFilters', () => {
        const filterOverrides = [{ field: ORIGIN_FILTER_NAME, values: ['PROD'] }];
        const baseFilters = [];
        const orFiltersWithoutOverrides = generateOrFilters(UnionType.OR, baseFilters);
        const orFiltersWithOverrides = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            {
                and: [{ field: ORIGIN_FILTER_NAME, values: ['PROD'] }],
            },
        ]);
    });

    it('should not modify orFilters on empty overrides', () => {
        const filterOverrides = [];
        const baseFilters = [{ field: PLATFORM_FILTER_NAME, values: ['looker'] }];
        const orFiltersWithoutOverrides = generateOrFilters(UnionType.OR, baseFilters);
        const orFiltersWithOverrides = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            {
                and: [{ field: PLATFORM_FILTER_NAME, values: ['looker'] }],
            },
        ]);
    });
});
