import { useMemo } from 'react';

import {
    DomainOwnerInfo,
    extractOwnerOptionsFromFacets,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FacetMetadata } from '@types';

const GLOSSARY_OWNER_FACET_MAX = 100;

/**
 * Corpus-wide owners facet for GlossaryNode + GlossaryTerm — no parent / owners
 * filters, so the dropdown stays complete while sidebar filters are applied.
 */
export default function useGlossaryOwnerAggregations({ skip = false }: { skip?: boolean } = {}): {
    owners: DomainOwnerInfo[];
    loading: boolean;
    error: unknown;
} {
    const {
        data: newData,
        previousData,
        loading,
        error,
    } = useAggregateAcrossEntitiesQuery({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                query: '*',
                facets: [OWNERS_FILTER_NAME],
                searchFlags: {
                    maxAggValues: GLOSSARY_OWNER_FACET_MAX,
                },
            },
        },
    });

    const data = error ? null : (newData ?? previousData);

    const owners = useMemo(() => {
        const facets = (data?.aggregateAcrossEntities?.facets ?? null) as FacetMetadata[] | null;
        return extractOwnerOptionsFromFacets(facets);
    }, [data]);

    return { owners, loading, error };
}
