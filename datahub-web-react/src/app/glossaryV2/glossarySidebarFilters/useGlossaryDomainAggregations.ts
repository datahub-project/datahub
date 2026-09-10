import { useMemo } from 'react';

import { FacetSelectOption, mapFacetToEntityOptions } from '@app/document/utils/documentSidebarFacets.utils';
import { DOMAINS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { Entity, EntityType, FacetMetadata } from '@types';

const GLOSSARY_DOMAIN_FACET_MAX = 100;

/**
 * Corpus-wide domains facet for GlossaryNode + GlossaryTerm.
 * Options keep the Domain entity so the sidebar can render Documents-style DomainLink pills.
 */
export default function useGlossaryDomainAggregations({ skip = false }: { skip?: boolean } = {}): {
    domains: FacetSelectOption[];
    loading: boolean;
    error: unknown;
} {
    const entityRegistry = useEntityRegistry();
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
                facets: [DOMAINS_FILTER_NAME],
                searchFlags: {
                    maxAggValues: GLOSSARY_DOMAIN_FACET_MAX,
                },
            },
        },
    });

    const data = error ? null : (newData ?? previousData);

    const domains = useMemo(() => {
        const facets = (data?.aggregateAcrossEntities?.facets ?? []) as FacetMetadata[];
        const facet = facets.find((f) => f.field === DOMAINS_FILTER_NAME);
        const displayName = (type: EntityType, entity: Entity) => entityRegistry.getDisplayName(type, entity);
        return mapFacetToEntityOptions(facet, displayName);
    }, [data, entityRegistry]);

    return { domains, loading, error };
}
