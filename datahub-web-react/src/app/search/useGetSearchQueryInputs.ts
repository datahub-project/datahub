import * as QueryString from 'query-string';
import { useLocation, useParams } from 'react-router';
import { useMemo } from 'react';
import { FacetFilterInput, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME, FILTER_DELIMITER, FILTER_URL_PREFIX, UnionType } from './utils/constants';
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

    // Some filters, like browsePathV2 cause the sidebar to re-load, and we want to ignore that in downstream useEffect's
    const queryStringWithExclusions = useMemo(
        () =>
            QueryString.exclude(
                location.search,
                (name) => !!excludedFilterFields?.some((field) => name.startsWith(`${FILTER_URL_PREFIX}${field}`)),
            ),
        [excludedFilterFields, location.search],
    );

    const params = useMemo(
        () => QueryString.parse(queryStringWithExclusions, { arrayFormat: 'comma' }),
        [queryStringWithExclusions],
    );

    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;
    const viewUrn = userContext.localState?.selectedViewUrn;

    const filters: Array<FacetFilterInput> = useFilters(params);
    const nonNestedFilters = useMemo(() => filters.filter((f) => !f.field.includes(FILTER_DELIMITER)), [filters]);
    const nestedFilters = useMemo(() => filters.filter((filter) => filter.field.includes(FILTER_DELIMITER)), [filters]);
    const filtersWithoutEntities = useMemo(
        () => nonNestedFilters.filter((filter) => filter.field !== ENTITY_FILTER_NAME),
        [nonNestedFilters],
    );
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .flatMap((filter) => (filter.values || []).map((value) => value?.toUpperCase() as EntityType));

    const orFilters = useMemo(
        () => generateOrFilters(unionType, filtersWithoutEntities, nestedFilters),
        [filtersWithoutEntities, nestedFilters, unionType],
    );

    return { entityFilters, query, unionType, filters, orFilters, filtersWithoutEntities, viewUrn, page, activeType };
}
