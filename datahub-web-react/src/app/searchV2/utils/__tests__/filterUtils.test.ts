import { QuickFilterField } from '@app/searchV2/autoComplete/quickFilters/utils';
import {
    excludeEmptyAndFilters,
    getAutoCompleteInputFromQuickFilter,
    getFiltersWithQuickFilter,
    mergeEnvIntoOriginFacets,
} from '@app/searchV2/utils/filterUtils';

describe('getAutoCompleteInputFromQuickFilter', () => {
    it('should create a platform filter if the selected quick filter is a platform', () => {
        const selectedQuickFilter = { field: QuickFilterField.Platform, value: 'urn:li:dataPlatform:dbt' };
        const filterResult = getAutoCompleteInputFromQuickFilter(selectedQuickFilter);

        expect(filterResult).toMatchObject({
            filters: [{ field: selectedQuickFilter.field, values: [selectedQuickFilter.value] }],
            types: [],
        });
    });

    it('should create an entity type filter if the selected quick filter is an entity', () => {
        const selectedQuickFilter = { field: QuickFilterField.Entity, value: 'DASHBOARD' };
        const filterResult = getAutoCompleteInputFromQuickFilter(selectedQuickFilter);

        expect(filterResult).toMatchObject({
            filters: [],
            types: [selectedQuickFilter.value],
        });
    });

    it('should return empty filters if there is no selected quick filter', () => {
        const filterResult = getAutoCompleteInputFromQuickFilter(null);

        expect(filterResult).toMatchObject({ filters: [], types: [] });
    });
});

describe('getFiltersWithQuickFilter', () => {
    it('should create a filters list with a quick filter as a FacetFilter when there is a quick filter', () => {
        const selectedQuickFilter = { field: QuickFilterField.Platform, value: 'urn:li:dataPlatform:dbt' };
        const filterResult = getFiltersWithQuickFilter(selectedQuickFilter);

        expect(filterResult).toMatchObject([{ field: selectedQuickFilter.field, values: [selectedQuickFilter.value] }]);
    });

    it('should return empty filters if there is no selected quick filter', () => {
        const filterResult = getFiltersWithQuickFilter(null);

        expect(filterResult).toMatchObject([]);
    });
});

describe('excludeEmptyAndFilters', () => {
    it('should handle filter out empty filters', () => {
        const result = excludeEmptyAndFilters(undefined);

        expect(result).toBeUndefined();
    });

    it('should handle empty array', () => {
        const result = excludeEmptyAndFilters([]);

        expect(result).toMatchObject([]);
    });

    it('should handle array of filled filters', () => {
        const result = excludeEmptyAndFilters([
            { and: [{ field: 'test', values: ['test'] }] },
            { and: [{ field: 'test2', values: ['test2'] }] },
        ]);

        expect(result).toMatchObject([
            { and: [{ field: 'test', values: ['test'] }] },
            { and: [{ field: 'test2', values: ['test2'] }] },
        ]);
    });

    it('should handle mixed empty and filled filters', () => {
        const result = excludeEmptyAndFilters([{ and: [] }, { and: [{ field: 'test', values: ['test'] }] }]);

        expect(result).toMatchObject([{ and: [{ field: 'test', values: ['test'] }] }]);
    });

    it('should handle array of empty filters', () => {
        const result = excludeEmptyAndFilters([{ and: [] }, { and: [] }]);

        expect(result).toMatchObject([]);
    });
});

describe('mergeEnvIntoOriginFacets', () => {
    const makeFacet = (field: string, aggregations: Record<string, number> = {}) =>
        ({
            field,
            displayName: field === 'origin' ? 'Environment' : field,
            aggregations: Object.entries(aggregations).map(([value, count]) => ({ value, count })),
        }) as any;

    it('should return facets unchanged when origin is present', () => {
        const facets = [makeFacet('origin', { PROD: 10 }), makeFacet('env', { PROD: 5 }), makeFacet('platform')];
        const result = mergeEnvIntoOriginFacets(facets);

        expect(result).toEqual(facets);
    });

    it('should rename env to origin when origin is not present', () => {
        const facets = [makeFacet('env', { PROD: 5 }), makeFacet('platform')];
        const result = mergeEnvIntoOriginFacets(facets);

        expect(result).toHaveLength(2);
        expect(result[0].field).toBe('origin');
        expect(result[1].field).toBe('platform');
    });

    it('should return facets unchanged when neither env nor origin is present', () => {
        const facets = [makeFacet('platform'), makeFacet('tags')];
        const result = mergeEnvIntoOriginFacets(facets);

        expect(result).toEqual(facets);
    });

    it('should handle empty facets array', () => {
        const result = mergeEnvIntoOriginFacets([]);

        expect(result).toEqual([]);
    });
});
