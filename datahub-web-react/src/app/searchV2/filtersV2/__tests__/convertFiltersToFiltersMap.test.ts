import { convertFiltersToFiltersMap } from '@app/searchV2/filtersV2/utils';

import { FacetFilterInput } from '@types';

describe('convertFiltersToFiltersMap', () => {
    it('should return undefined when input is undefined', () => {
        const result = convertFiltersToFiltersMap(undefined);
        expect(result).toBeUndefined();
    });

    it('should return an empty Map when input is an empty array', () => {
        const result = convertFiltersToFiltersMap([]);
        expect(result).toBeInstanceOf(Map);
        expect(result?.size).toBe(0);
    });

    it('should correctly map filters to a Map when input contains valid filters', () => {
        const filters: FacetFilterInput[] = [
            { field: 'category', values: ['electronics'] },
            { field: 'price', values: ['100-200'] },
        ];

        const result = convertFiltersToFiltersMap(filters);

        expect(result).toBeInstanceOf(Map);
        expect(result?.size).toBe(2);

        // Check the first entry
        expect(result?.get('category')).toEqual({ filters: [{ field: 'category', values: ['electronics'] }] });

        // Check the second entry
        expect(result?.get('price')).toEqual({ filters: [{ field: 'price', values: ['100-200'] }] });
    });

    it('should overwrite earlier entries when input contains duplicate fields', () => {
        const filters: FacetFilterInput[] = [
            { field: 'category', values: ['electronics'] },
            { field: 'category', values: ['books'] }, // Duplicate field
        ];

        const result = convertFiltersToFiltersMap(filters);

        expect(result).toBeInstanceOf(Map);
        expect(result?.size).toBe(1); // Only one unique key remains

        // Check the final entry for 'category'
        expect(result?.get('category')).toEqual({ filters: [{ field: 'category', values: ['books'] }] });
    });
});
