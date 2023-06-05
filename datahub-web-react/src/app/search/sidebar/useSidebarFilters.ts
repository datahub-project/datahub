import { useMemo } from 'react';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { BROWSE_PATH_V2_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { useMaybeEnvironmentAggregation, useMaybePlatformAggregation } from './BrowseContext';
import { SidebarFilters } from './types';
import { applyOrFilterOverrides } from '../utils/applyFilterOverrides';

export const useSidebarFilters = (): SidebarFilters => {
    const environment = useMaybeEnvironmentAggregation()?.value;
    const platform = useMaybePlatformAggregation()?.value;

    const filterOverrides = useMemo(
        () => [
            ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
            ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
        ],
        [environment, platform],
    );

    const excludedFilterFields = useMemo(
        () => filterOverrides.map((filter) => filter.field).concat(BROWSE_PATH_V2_FILTER_NAME),
        [filterOverrides],
    );

    const { query, orFilters: orFiltersWithoutOverrides, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const orFilters = useMemo(
        () => applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides),
        [filterOverrides, orFiltersWithoutOverrides],
    );

    return useMemo(() => ({ query, orFilters, viewUrn } as const), [orFilters, query, viewUrn]);
};
