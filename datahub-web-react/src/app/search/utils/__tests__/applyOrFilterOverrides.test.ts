import applyOrFilterOverrides from '../applyOrFilterOverrides';
import { CONTAINER_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME, UnionType } from '../constants';
import { generateOrFilters } from '../generateOrFilters';

describe('applyOrFilterOverrides', () => {
    it('should apply overrides to each OR block of orFilters', () => {
        const filterOverrides = [{ field: ORIGIN_FILTER_NAME, value: 'PROD' }];
        const baseFilters = [
            { field: PLATFORM_FILTER_NAME, value: 'looker' },
            { field: CONTAINER_FILTER_NAME, value: 'parent' },
        ];
        const orFiltersWithoutOverrides = generateOrFilters(UnionType.OR, baseFilters);
        const orFiltersWithOverrides = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            {
                and: [
                    { field: PLATFORM_FILTER_NAME, value: 'looker' },
                    { field: ORIGIN_FILTER_NAME, value: 'PROD' },
                ],
            },
            {
                and: [
                    { field: CONTAINER_FILTER_NAME, value: 'parent' },
                    { field: ORIGIN_FILTER_NAME, value: 'PROD' },
                ],
            },
        ]);
    });

    it('should apply overrides to empty orFilters', () => {
        const filterOverrides = [{ field: ORIGIN_FILTER_NAME, value: 'PROD' }];
        const baseFilters = [];
        const orFiltersWithoutOverrides = generateOrFilters(UnionType.OR, baseFilters);
        const orFiltersWithOverrides = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            {
                and: [{ field: ORIGIN_FILTER_NAME, value: 'PROD' }],
            },
        ]);
    });

    it('should not modify orFilters on empty overrides', () => {
        const filterOverrides = [];
        const baseFilters = [{ field: PLATFORM_FILTER_NAME, value: 'looker' }];
        const orFiltersWithoutOverrides = generateOrFilters(UnionType.OR, baseFilters);
        const orFiltersWithOverrides = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

        expect(orFiltersWithOverrides).toMatchObject([
            {
                and: [{ field: PLATFORM_FILTER_NAME, value: 'looker' }],
            },
        ]);
    });
});
