import { UnionType } from '@src/app/searchV2/utils/constants';
import { generateOrFilters } from '@src/app/searchV2/utils/generateOrFilters';
import useFilters from '@src/app/searchV2/utils/useFilters';
import { navigateWithFilters } from '@src/app/sharedV2/filters/navigateWithFilters';
import { FacetFilterInput } from '@src/types.generated';
import * as QueryString from 'query-string';
import { useMemo, useState } from 'react';
import { useHistory, useLocation } from 'react-router';

export default function useGetActionRequestsQueryInputs({ useUrlParams }) {
    const history = useHistory();
    const location = useLocation();
    const [filters, setFilters] = useState<Array<FacetFilterInput>>([]);

    // TODO: Separate other page params from proposal params when needed (eg ProposalsTab)
    const params = useMemo(() => {
        return QueryString.parse(location.search, { arrayFormat: 'comma' });
    }, [location.search]);
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;

    // Filters extracted from query params
    const filtersFromUrl: Array<FacetFilterInput> = useFilters(params);
    const orFiltersFromUrl = useMemo(() => generateOrFilters(unionType, filtersFromUrl), [filtersFromUrl, unionType]);
    // Filters
    const orFilters = useMemo(() => generateOrFilters(UnionType.AND, filters), [filters]);

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        if (useUrlParams) {
            navigateWithFilters({
                filters: newFilters,
                history,
                location,
            });
        } else {
            setFilters(newFilters);
        }
    };

    // Default to use url query params
    if (useUrlParams) {
        return { orFilters: orFiltersFromUrl, filters: filtersFromUrl, onChangeFilters };
    }

    // Use local state otherwise. This will be useful in modals etc
    return {
        orFilters,
        filters,
        onChangeFilters,
    };
}
