import { QuickFilterField } from '@app/searchV2/autoComplete/quickFilters/utils';
import {
    excludeEmptyAndFilters,
    getAutoCompleteInputFromQuickFilter,
    getFiltersWithQuickFilter,
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
