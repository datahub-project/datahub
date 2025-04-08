import * as QueryString from 'query-string';
import { useMemo } from 'react';
import { useLocation } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import useSortInput from '@app/searchV2/sorting/useSortInput';
import { ENTITY_FILTER_NAME, UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import useFilters from '@app/searchV2/utils/useFilters';

import { EntityType, FacetFilterInput } from '@types';

export default function useGetSearchQueryInputs(excludedFilterFields?: Array<string>) {
    const userContext = useUserContext();
    const location = useLocation();

    const params = useMemo(() => QueryString.parse(location.search, { arrayFormat: 'comma' }), [location.search]);
    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;
    const viewUrn = userContext.localState?.selectedViewUrn;
    const selectedSortOption = useSelectedSortOption();
    const sortInput = useSortInput(selectedSortOption);

    const filters: Array<FacetFilterInput> = useFilters(params);
    const entityFilters: Array<EntityType> = useMemo(
        () =>
            filters
                .filter((filter) => filter.field === ENTITY_FILTER_NAME)
                .flatMap((filter) => (filter.values || []).map((value) => value?.toUpperCase() as EntityType))
                .sort((a, b) => a.localeCompare(b)),
        [filters],
    );

    const orFilters = useMemo(
        () => generateOrFilters(unionType, filters, excludedFilterFields),
        [filters, excludedFilterFields, unionType],
    );

    return { entityFilters, query, unionType, filters, orFilters, viewUrn, page, sortInput };
}
