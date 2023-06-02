import isEqual from 'lodash/isEqual';
import { useCallback, useEffect, useMemo, useState } from 'react';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { applyOrFilterOverrides } from '../utils/applyFilterOverrides';
import { BROWSE_PATH_V2_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { useMaybeEnvironmentAggregation, useMaybePlatformAggregation } from './BrowseContext';

export const useSidebarFilters = () => {
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

    const {
        query: latestQuery,
        orFilters: latestOrFilters,
        viewUrn: latestViewUrn,
    } = useGetSearchQueryInputs(excludedFilterFields);

    const createSidebarFilters = useCallback(
        () => ({
            query: latestQuery,
            orFilters: applyOrFilterOverrides(latestOrFilters, filterOverrides),
            viewUrn: latestViewUrn,
        }),
        [filterOverrides, latestOrFilters, latestQuery, latestViewUrn],
    );

    const [sidebarFilters, setSidebarFilters] = useState(createSidebarFilters);

    // Ensures we only trigger filter updates in the sidebar if they truly changed (clicking browse could trigger this when we don't want)
    useEffect(() => {
        setSidebarFilters((sf) => {
            const latestSidebarFilters = createSidebarFilters();
            // todo - pull this out and write a more explicit comparison function/test
            return isEqual(sf, latestSidebarFilters) ? sf : latestSidebarFilters;
        });
    }, [createSidebarFilters]);

    return sidebarFilters;
};

export type SidebarFilters = ReturnType<typeof useSidebarFilters>;
