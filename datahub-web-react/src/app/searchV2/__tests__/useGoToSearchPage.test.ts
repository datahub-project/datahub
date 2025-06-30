import { renderHook } from '@testing-library/react-hooks';

import { useSelectedSortOption } from '@app/search/context/SearchContext';
import useGoToSearchPage from '@app/searchV2/useGoToSearchPage';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

vi.mock('@app/search/context/SearchContext', () => ({
    useSelectedSortOption: vi.fn(),
}));

vi.mock('@app/searchV2/useQueryAndFiltersFromLocation', () => ({
    default: vi.fn(() => ({ filters: [] })),
}));

vi.mock('@app/searchV2/utils/navigateToSearchUrl', () => ({
    navigateToSearchUrl: vi.fn(),
}));

describe('useGoToSearchPage Hook', () => {
    const mockQuickFilter = {
        field: 'type',
        value: 'dataset',
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return a function that navigates to search url', () => {
        const mockSortOption = 'relevance';
        vi.mocked(useSelectedSortOption).mockReturnValue(mockSortOption);

        const { result } = renderHook(() => useGoToSearchPage(mockQuickFilter));

        const query = 'testQuery';
        const filters = [{ field: 'origin', values: ['urn:li:dataPlatform:bigquery'] }];

        result.current(query, filters);

        expect(vi.mocked(navigateToSearchUrl)).toHaveBeenCalledWith(
            expect.objectContaining({
                query,
                filters,
                selectedSortOption: mockSortOption,
            }),
        );
    });

    it('should use existing filters if no newFilters provided', () => {
        const existingFilters = [{ field: 'platform', values: ['urn:li:dataPlatform:mysql'] }];
        vi.mocked(useQueryAndFiltersFromLocation).mockReturnValue({ filters: existingFilters, query: '' });

        vi.mocked(useSelectedSortOption).mockReturnValue(undefined);

        const { result } = renderHook(() => useGoToSearchPage(null));

        const query = 'hello';

        result.current(query);

        expect(vi.mocked(navigateToSearchUrl)).toHaveBeenCalledWith(
            expect.objectContaining({
                query,
                filters: existingFilters,
                selectedSortOption: undefined,
            }),
        );
    });

    it('should always pass newFilters if newFilters is provided', () => {
        vi.mocked(useQueryAndFiltersFromLocation).mockReturnValue({ filters: [], query: '' });
        vi.mocked(useSelectedSortOption).mockReturnValue(undefined);
        const newFilters = [{ field: 'platform', values: ['urn:li:dataPlatform:hive'] }];

        const { result } = renderHook(() => useGoToSearchPage(null));

        const query = 'featureFlagTest';

        result.current(query, newFilters);

        expect(vi.mocked(navigateToSearchUrl)).toHaveBeenCalledWith(
            expect.objectContaining({
                query,
                filters: newFilters,
                selectedSortOption: undefined,
            }),
        );
    });

    it('should not override filters if newFilters is not provided', () => {
        const existingFilters = [{ field: 'platform', values: ['urn:li:dataPlatform:hive'] }];
        vi.mocked(useQueryAndFiltersFromLocation).mockReturnValue({ filters: existingFilters, query: '' });

        const { result } = renderHook(() => useGoToSearchPage(null));

        const query = 'noOverride';

        result.current(query);

        expect(vi.mocked(navigateToSearchUrl)).toHaveBeenCalledWith(
            expect.objectContaining({
                filters: existingFilters,
            }),
        );
    });
});
