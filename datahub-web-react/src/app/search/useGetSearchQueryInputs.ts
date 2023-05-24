import * as QueryString from 'query-string';
import { useLocation, useParams } from 'react-router';
import { useMemo } from 'react';
import { FacetFilterInput, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME, FILTER_DELIMITER, UnionType } from './utils/constants';
import { useUserContext } from '../context/useUserContext';
import useFilters from './utils/useFilters';
import { generateOrFilters } from './utils/generateOrFilters';

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

    const filters: Array<FacetFilterInput> = useFilters(params);
    const nonNestedFilters = filters.filter((f) => !f.field.includes(FILTER_DELIMITER));
    const nestedFilters = filters.filter(
        (f) => f.field.includes(FILTER_DELIMITER) && !excludedFilterFields?.includes(f.field),
    );
    const filtersWithoutEntities = nonNestedFilters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME && !excludedFilterFields?.includes(filter.field),
    );
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .flatMap((filter) => (filter.values || []).map((value) => value?.toUpperCase() as EntityType));

    const orFilters = generateOrFilters(unionType, filtersWithoutEntities, nestedFilters);

    return { entityFilters, query, unionType, filters, orFilters, filtersWithoutEntities, viewUrn, page, activeType };
}
