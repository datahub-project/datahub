import { convertFiltersMapToFilters } from '../utils';

describe('convertFiltersMapToFilters', () => {
    const mockFiltersMap = new Map<string, { filters: { field: string; values?: string[] }[] }>([
        ['field1', { filters: [{ field: 'field1', values: ['value1', 'value2'] }] }],
        ['field2', { filters: [{ field: 'field2', values: ['value3'] }] }],
        ['field3', { filters: [{ field: 'field3', values: [] }] }], // Empty values array
        ['field4', { filters: [{ field: 'field4' }] }], // No values array
    ]);

    it('should return all filters when no options are provided', () => {
        const result = convertFiltersMapToFilters(mockFiltersMap);
        expect(result).toEqual([
            { field: 'field1', values: ['value1', 'value2'] },
            { field: 'field2', values: ['value3'] },
        ]);
    });

    it('should filter by includedFields', () => {
        const result = convertFiltersMapToFilters(mockFiltersMap, { includedFields: ['field1'] });
        expect(result).toEqual([{ field: 'field1', values: ['value1', 'value2'] }]);
    });

    it('should exclude fields specified in excludedFields', () => {
        const result = convertFiltersMapToFilters(mockFiltersMap, { excludedFields: ['field2'] });
        expect(result).toEqual([{ field: 'field1', values: ['value1', 'value2'] }]);
    });

    it('should handle both includedFields and excludedFields', () => {
        const result = convertFiltersMapToFilters(mockFiltersMap, {
            includedFields: ['field1', 'field2'],
            excludedFields: ['field2'],
        });
        expect(result).toEqual([{ field: 'field1', values: ['value1', 'value2'] }]);
    });

    it('should return an empty array when filtersMap is undefined', () => {
        const result = convertFiltersMapToFilters(undefined);
        expect(result).toEqual([]);
    });

    it('should return an empty array when filtersMap is empty', () => {
        const result = convertFiltersMapToFilters(new Map());
        expect(result).toEqual([]);
    });

    it('should exclude filters with empty values', () => {
        const result = convertFiltersMapToFilters(mockFiltersMap);
        expect(result).not.toContainEqual({ field: 'field3', values: [] });
        expect(result).not.toContainEqual({ field: 'field4' });
    });
});
