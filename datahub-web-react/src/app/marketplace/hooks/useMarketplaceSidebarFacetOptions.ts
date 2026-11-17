import { useMemo } from 'react';

import {
    AuthorFacetOption,
    FacetSelectOption,
    ensureSelectedAuthorOptions,
    ensureSelectedOptions,
    filterOutAiAgentAuthors,
    mapFacetToAuthorOptions,
    mapFacetToEntityOptions,
} from '@app/document/utils/documentSidebarFacets.utils';
import {
    APPLICATIONS_FILTER_NAME,
    buildMarketplaceSidebarFilters,
} from '@app/marketplace/utils/marketplaceSidebarFilters';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
    UnionType,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { Entity, EntityType, FacetFilterInput, FacetMetadata } from '@types';

export type { AuthorFacetOption, FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';

const FACET_MAX = 100;

type AppliedFilters = {
    searchQuery?: string;
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    ownerUrns?: string[];
    applicationUrns?: string[];
    viewUrn?: string | null;
    includeDomainFacets?: boolean;
    includeTagFacets?: boolean;
    includeTermFacets?: boolean;
    includeOwnerFacets?: boolean;
    includeApplicationFacets?: boolean;
};

function useMarketplaceFacetAggregations(
    field: string,
    appliedFilters: FacetFilterInput[],
    searchQuery: string,
    viewUrn?: string | null,
    skip?: boolean,
) {
    const orFilters = useMemo(() => generateOrFilters(UnionType.AND, appliedFilters, [field]), [appliedFilters, field]);
    const query = searchQuery.trim().length > 0 ? searchQuery.trim() : '*';

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
                types: [EntityType.DataProduct],
                query,
                orFilters,
                viewUrn,
                facets: [field],
                searchFlags: {
                    maxAggValues: FACET_MAX,
                },
            },
        },
    });

    const data = error ? null : (newData ?? previousData);
    const facet = (data?.aggregateAcrossEntities?.facets ?? []).find((f) => f.field === field) as
        | FacetMetadata
        | undefined;

    return { facet, loading: skip ? false : loading };
}

/**
 * Domain / tag / term / owner / application options for the marketplace sidebar.
 */
export default function useMarketplaceSidebarFacetOptions({
    searchQuery = '',
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    ownerUrns = [],
    applicationUrns = [],
    viewUrn,
    includeDomainFacets = true,
    includeTagFacets = true,
    includeTermFacets = true,
    includeOwnerFacets = true,
    includeApplicationFacets = true,
}: AppliedFilters = {}): {
    domainOptions: FacetSelectOption[];
    tagOptions: FacetSelectOption[];
    termOptions: FacetSelectOption[];
    ownerOptions: AuthorFacetOption[];
    applicationOptions: FacetSelectOption[];
    loading: boolean;
} {
    const entityRegistry = useEntityRegistry();
    const appliedFilters = useMemo(
        () =>
            buildMarketplaceSidebarFilters({
                domainUrns,
                tagUrns,
                termUrns,
                ownerUrns,
                applicationUrns,
            }),
        [domainUrns, tagUrns, termUrns, ownerUrns, applicationUrns],
    );

    const { facet: domainsFacet, loading: domainsLoading } = useMarketplaceFacetAggregations(
        DOMAINS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeDomainFacets,
    );
    const { facet: tagsFacet, loading: tagsLoading } = useMarketplaceFacetAggregations(
        TAGS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeTagFacets,
    );
    const { facet: termsFacet, loading: termsLoading } = useMarketplaceFacetAggregations(
        GLOSSARY_TERMS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeTermFacets,
    );
    const { facet: ownersFacet, loading: ownersLoading } = useMarketplaceFacetAggregations(
        OWNERS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeOwnerFacets,
    );
    const { facet: applicationsFacet, loading: applicationsLoading } = useMarketplaceFacetAggregations(
        APPLICATIONS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeApplicationFacets,
    );

    const { domainOptions, tagOptions, termOptions, ownerOptions, applicationOptions } = useMemo(() => {
        const displayName = (type: EntityType, entity: Entity) => entityRegistry.getDisplayName(type, entity);

        return {
            domainOptions: ensureSelectedOptions(mapFacetToEntityOptions(domainsFacet, displayName), domainUrns),
            tagOptions: ensureSelectedOptions(mapFacetToEntityOptions(tagsFacet, displayName), tagUrns),
            termOptions: ensureSelectedOptions(mapFacetToEntityOptions(termsFacet, displayName), termUrns),
            ownerOptions: filterOutAiAgentAuthors(
                ensureSelectedAuthorOptions(mapFacetToAuthorOptions(ownersFacet, displayName), ownerUrns),
            ),
            applicationOptions: ensureSelectedOptions(
                mapFacetToEntityOptions(applicationsFacet, displayName),
                applicationUrns,
            ),
        };
    }, [
        domainsFacet,
        tagsFacet,
        termsFacet,
        ownersFacet,
        applicationsFacet,
        entityRegistry,
        domainUrns,
        tagUrns,
        termUrns,
        ownerUrns,
        applicationUrns,
    ]);

    return {
        domainOptions,
        tagOptions,
        termOptions,
        ownerOptions,
        applicationOptions,
        loading: domainsLoading || tagsLoading || termsLoading || ownersLoading || applicationsLoading,
    };
}
