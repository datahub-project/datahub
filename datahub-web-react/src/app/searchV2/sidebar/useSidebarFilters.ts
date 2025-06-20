import isEqual from 'lodash/isEqual';
import { useEffect, useMemo, useState } from 'react';

import {
    useMaybeEntityType,
    useMaybeEnvironmentAggregation,
    useMaybePlatformAggregation,
} from '@app/searchV2/sidebar/BrowseContext';
import { SidebarFilters } from '@app/searchV2/sidebar/types';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import { applyOrFilterOverrides } from '@app/searchV2/utils/applyFilterOverrides';
import { ENTITY_FILTER_NAME, ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

export const useSidebarFilters = (): SidebarFilters => {
    const entityType = useMaybeEntityType();
    const environment = useMaybeEnvironmentAggregation()?.value;
    const platform = useMaybePlatformAggregation()?.value;

    const filterOverrides = useMemo(
        () => [
            ...(entityType ? [{ field: ENTITY_FILTER_NAME, value: entityType }] : []),
            ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
            ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
        ],
        [entityType, environment, platform],
    );

    const excludedFilterFields = useMemo(() => filterOverrides.map((filter) => filter.field), [filterOverrides]);

    const {
        entityFilters: latestEntityFilters,
        query: latestQuery,
        orFilters: latestOrFilters,
        viewUrn: latestViewUrn,
    } = useGetSearchQueryInputs(excludedFilterFields);

    const latestSidebarFilters = useMemo(
        () => ({
            // todo(josh): remove this and move it to a normal filterOverride when _entityType is fully wired up on the backend
            entityFilters: entityType ? [entityType] : latestEntityFilters,
            query: latestQuery,
            orFilters: applyOrFilterOverrides(latestOrFilters, filterOverrides),
            viewUrn: latestViewUrn,
        }),
        [entityType, filterOverrides, latestEntityFilters, latestOrFilters, latestQuery, latestViewUrn],
    );

    const [sidebarFilters, setSidebarFilters] = useState(latestSidebarFilters);

    // Ensures we only trigger filter updates in the sidebar if they truly changed (clicking browse could trigger this when we don't want)
    useEffect(() => {
        if (!isEqual(latestSidebarFilters, sidebarFilters)) setSidebarFilters(latestSidebarFilters);
    }, [latestSidebarFilters, sidebarFilters]);

    return sidebarFilters;
};
