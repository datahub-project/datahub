import * as QueryString from 'query-string';
import { useMemo } from 'react';
import { useLocation, useParams } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import useSortInput from '@app/search/sorting/useSortInput';
import { ENTITY_FILTER_NAME, UnionType } from '@app/search/utils/constants';
import { generateOrFilters } from '@app/search/utils/generateOrFilters';
import useFilters from '@app/search/utils/useFilters';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, FacetFilterInput } from '@types';

type SearchPageParams = {
    type?: string;
};

export default function useGetSearchQueryInputs(excludedFilterFields?: Array<string>) {
    const userContext = useUserContext();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const params = useMemo(() => QueryString.parse(location.search, { arrayFormat: 'comma' }), [location.search]);
    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;
    const viewUrn = userContext.localState?.selectedViewUrn;
    const sortInput = useSortInput();

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

    return { entityFilters, query, unionType, filters, orFilters, viewUrn, page, activeType, sortInput };
}
