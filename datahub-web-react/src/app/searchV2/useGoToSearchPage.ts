import { useCallback } from 'react';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

import { FacetFilterInput, QuickFilter } from '@types';

export default function useGoToSearchPage(quickFilter: QuickFilter | null, showSearchBarAutocompleteRedesign: boolean) {
    const history = useHistory();
    const selectedSortOption = useSelectedSortOption();

    const { filters } = useQueryAndFiltersFromLocation();

    return useCallback(
        (query: string, newFilters?: FacetFilterInput[]) => {
            analytics.event({
                type: EventType.SearchEvent,
                query,
                pageNumber: 1,
                originPath: window.location.pathname,
                selectedQuickFilterTypes: quickFilter ? [quickFilter.field] : undefined,
                selectedQuickFilterValues: quickFilter ? [quickFilter.value] : undefined,
            });

            let newAppliedFilters: FacetFilterInput[] | undefined = filters;

            // For the redesigned search bar we should always pass new filters even though they are empty
            if (showSearchBarAutocompleteRedesign || (newFilters && newFilters?.length > 0)) {
                newAppliedFilters = newFilters;
            }

            navigateToSearchUrl({
                query,
                filters: newAppliedFilters,
                history,
                selectedSortOption,
            });
        },
        [filters, history, quickFilter, selectedSortOption, showSearchBarAutocompleteRedesign],
    );
}
