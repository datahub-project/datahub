import * as QueryString from 'query-string';
import { useLocation, useParams } from 'react-router';
import { FacetFilterInput, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { ENTITY_FILTER_NAME, UnionType } from './utils/constants';
import { useUserContext } from '../context/useUserContext';
import useFilters from './utils/useFilters';
import { generateOrFilters } from './utils/generateOrFilters';

type SearchPageParams = {
    type?: string;
};

export default function useGetSearchQueryInputs(filterFieldToExclude?: string) {
    const userContext = useUserContext();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const query: string = decodeURIComponent(params.query ? (params.query as string) : '');
    const activeType = entityRegistry.getTypeOrDefaultFromPathName(useParams<SearchPageParams>().type || '', undefined);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;
    const viewUrn = userContext.localState?.selectedViewUrn;

    const filters: Array<FacetFilterInput> = useFilters(params);
    let filtersWithoutEntities: Array<FacetFilterInput> = filters.filter(
        (filter) => filter.field !== ENTITY_FILTER_NAME,
    );
    if (filterFieldToExclude) {
        filtersWithoutEntities = filtersWithoutEntities.filter((filter) => filter.field !== filterFieldToExclude);
    }
    const entityFilters: Array<EntityType> = filters
        .filter((filter) => filter.field === ENTITY_FILTER_NAME)
        .flatMap((filter) => (filter.values || []).map((value) => value?.toUpperCase() as EntityType));

    const orFilters = generateOrFilters(unionType, filtersWithoutEntities);

    return { entityFilters, query, unionType, filters, orFilters, filtersWithoutEntities, viewUrn, page, activeType };
}
