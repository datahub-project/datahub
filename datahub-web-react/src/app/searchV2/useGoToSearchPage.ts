/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import useQueryAndFiltersFromLocation from '@app/searchV2/useQueryAndFiltersFromLocation';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

import { FacetFilterInput, QuickFilter } from '@types';

export default function useGoToSearchPage(quickFilter: QuickFilter | null) {
    const location = useLocation();
    const history = useHistory();
    const selectedSortOption = useSelectedSortOption();

    const { filters } = useQueryAndFiltersFromLocation();

    return useCallback(
        (query: string, newFilters?: FacetFilterInput[]) => {
            analytics.event({
                type: EventType.SearchEvent,
                query,
                pageNumber: 1,
                originPath: location.pathname,
                selectedQuickFilterTypes: quickFilter ? [quickFilter.field] : undefined,
                selectedQuickFilterValues: quickFilter ? [quickFilter.value] : undefined,
            });

            let newAppliedFilters: FacetFilterInput[] | undefined = filters;

            if (newFilters && newFilters?.length > 0) {
                newAppliedFilters = newFilters;
            }

            navigateToSearchUrl({
                query,
                filters: newAppliedFilters,
                history,
                selectedSortOption,
            });
        },
        [filters, history, quickFilter, selectedSortOption, location.pathname],
    );
}
