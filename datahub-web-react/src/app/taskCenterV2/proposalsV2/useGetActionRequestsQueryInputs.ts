import * as QueryString from 'query-string';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useHistory, useLocation } from 'react-router';

import { mergeFilters, replaceFilterValues } from '@app/taskCenterV2/proposalsV2/utils';
import { UnionType } from '@src/app/searchV2/utils/constants';
import { generateOrFilters } from '@src/app/searchV2/utils/generateOrFilters';
import useFilters from '@src/app/searchV2/utils/useFilters';
import { navigateWithFilters } from '@src/app/sharedV2/filters/navigateWithFilters';
import { FacetFilterInput } from '@src/types.generated';

type Props = {
    useUrlParams: boolean;
    defaultFilters?: FacetFilterInput[];
    initialFilters?: FacetFilterInput[];
};

export default function useGetActionRequestsQueryInputs({
    useUrlParams,
    defaultFilters = [],
    initialFilters = [],
}: Props) {
    const history = useHistory();
    const location = useLocation();
    const hasInjectedInitialFiltersRef = useRef(false);

    // TODO: Separate other page params from proposal params when needed (eg ProposalsTab)
    const params = useMemo(() => {
        return QueryString.parse(location.search, { arrayFormat: 'comma' });
    }, [location.search]);
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;

    const activeFilters = useFilters(params);

    // Only inject initialFilters into the url if there are no active filters
    const shouldInjectInitialFilters = useMemo(() => {
        if (!useUrlParams || !initialFilters?.length || hasInjectedInitialFiltersRef.current) return false;
        const shouldInject = !activeFilters?.length;
        if (shouldInject) hasInjectedInitialFiltersRef.current = true;
        return shouldInject;
    }, [useUrlParams, initialFilters, activeFilters]);

    useEffect(() => {
        if (shouldInjectInitialFilters) {
            const merged = mergeFilters(activeFilters, initialFilters || []);
            navigateWithFilters({ filters: merged, history, location });
        }
    }, [shouldInjectInitialFilters, history, location, initialFilters, activeFilters]);

    // Filters extracted from query params.
    const filtersFromUrl: FacetFilterInput[] = mergeFilters(defaultFilters, useFilters(params));
    const orFiltersFromUrl = useMemo(() => generateOrFilters(unionType, filtersFromUrl), [filtersFromUrl, unionType]);

    // Filters for local state
    const [filters, setFilters] = useState<FacetFilterInput[]>(() => initialFilters || []);
    const orFilters = useMemo(() => generateOrFilters(UnionType.AND, filters), [filters]);

    const onChangeFilters = (newFilters: FacetFilterInput[], replace?: boolean) => {
        const currentFilters = useUrlParams ? filtersFromUrl : filters;
        // Either replace specific filter values or merge them
        const updatedFilters = replace ? replaceFilterValues(currentFilters, newFilters) : newFilters;

        if (useUrlParams) {
            navigateWithFilters({
                filters: mergeFilters(defaultFilters, updatedFilters),
                history,
                location,
            });
        } else {
            setFilters(mergeFilters(defaultFilters, updatedFilters));
        }
    };

    // Use url query params if specified
    if (useUrlParams) {
        return { orFilters: orFiltersFromUrl, filters: filtersFromUrl, onChangeFilters };
    }

    // Otherwise default to local state. This will be useful in modals etc
    return {
        orFilters,
        filters,
        onChangeFilters,
    };
}
