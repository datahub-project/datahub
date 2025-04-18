import { useMemo } from 'react';

import { useUserContext } from '@src/app/context/useUserContext';
import { UnionType } from '@src/app/searchV2/utils/constants';
import { generateOrFilters } from '@src/app/searchV2/utils/generateOrFilters';
import { useAggregateAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { EntityType, FacetFilterInput, FacetMetadata } from '@src/types.generated';

type Props = {
    activeFilters: FacetFilterInput[];
    facets: FacetMetadata[];
};
export default function useGetEntitySuggestions({ activeFilters, facets }: Props) {
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;

    const entityFilter = facets?.find((f) => f.field === 'resource');

    const isEntityFilterApplied = !!activeFilters.find((f) => f.field === 'resource')?.values?.length;
    const orFilters = generateOrFilters(UnionType.AND, activeFilters, ['resource']);

    const { data, loading } = useAggregateAcrossEntitiesQuery({
        skip: !isEntityFilterApplied,
        variables: {
            input: {
                query: '',
                facets: ['resource'],
                types: [EntityType.ActionRequest],
                orFilters,
                viewUrn,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const aggregations = isEntityFilterApplied
        ? data?.aggregateAcrossEntities?.facets?.[0]?.aggregations
        : entityFilter?.aggregations;

    const suggestions = useMemo(() => aggregations?.map((e) => e.entity), [aggregations]);

    return { suggestions, loading };
}
