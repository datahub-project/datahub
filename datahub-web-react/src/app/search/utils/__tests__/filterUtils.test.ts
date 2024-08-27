import { QuickFilterField } from '../../autoComplete/quickFilters/utils';
import { getAutoCompleteInputFromQuickFilter, getFiltersWithQuickFilter } from '../filterUtils';

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
