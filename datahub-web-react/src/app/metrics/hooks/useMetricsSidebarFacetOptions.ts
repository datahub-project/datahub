import { useMemo } from 'react';

import { useResolvedFacetOptions } from '@app/document/hooks/useResolvedFacetOptions';
import {
    AuthorFacetOption,
    FacetSelectOption,
    ensureSelectedAuthorOptions,
    ensureSelectedOptions,
    filterOutAiAgentAuthors,
    getMissingSelectedValues,
    mapFacetToAuthorOptions,
    mapFacetToEntityOptions,
} from '@app/document/utils/documentSidebarFacets.utils';
import { buildMetricsSidebarFilters } from '@app/metrics/utils/metricsSidebarFilters';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    UnionType,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { Entity, EntityType, FacetFilterInput, FacetMetadata } from '@types';

export type { AuthorFacetOption, FacetSelectOption } from '@app/document/utils/documentSidebarFacets.utils';
export { isDataPlatformEntity } from '@app/document/utils/documentSidebarFacets.utils';

const FACET_MAX = 100;

type AppliedFilters = {
    searchQuery?: string;
    platformUrns?: string[];
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    ownerUrns?: string[];
    viewUrn?: string | null;
    /** Skip Tag / Owners aggs until those filters are promoted (or selected). */
    includeTagFacets?: boolean;
    includeOwnerFacets?: boolean;
};

function useMetricsFacetAggregations(
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
                types: [EntityType.Metric],
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
 * Platform / domain / tag / term / owner options for the metrics sidebar.
 * Per-field aggregations so each dropdown can exclude its own filter.
 */
export default function useMetricsSidebarFacetOptions({
    searchQuery = '',
    platformUrns = [],
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    ownerUrns = [],
    viewUrn,
    includeTagFacets = true,
    includeOwnerFacets = true,
}: AppliedFilters = {}): {
    platformOptions: FacetSelectOption[];
    domainOptions: FacetSelectOption[];
    tagOptions: FacetSelectOption[];
    termOptions: FacetSelectOption[];
    ownerOptions: AuthorFacetOption[];
    loading: boolean;
} {
    const entityRegistry = useEntityRegistry();
    const appliedFilters = useMemo(
        () =>
            buildMetricsSidebarFilters({
                platformUrns,
                domainUrns,
                tagUrns,
                termUrns,
                ownerUrns,
            }),
        [platformUrns, domainUrns, tagUrns, termUrns, ownerUrns],
    );

    const { facet: platformsFacet, loading: platformsLoading } = useMetricsFacetAggregations(
        PLATFORM_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
    );
    const { facet: domainsFacet, loading: domainsLoading } = useMetricsFacetAggregations(
        DOMAINS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
    );
    const { facet: tagsFacet, loading: tagsLoading } = useMetricsFacetAggregations(
        TAGS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeTagFacets,
    );
    const { facet: termsFacet, loading: termsLoading } = useMetricsFacetAggregations(
        GLOSSARY_TERMS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
    );
    const { facet: ownersFacet, loading: ownersLoading } = useMetricsFacetAggregations(
        OWNERS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeOwnerFacets,
    );

    const facetOptions = useMemo(() => {
        const displayName = (type: EntityType, entity: Entity) => entityRegistry.getDisplayName(type, entity);
        return {
            platformOptions: mapFacetToEntityOptions(platformsFacet, displayName),
            domainOptions: mapFacetToEntityOptions(domainsFacet, displayName),
            tagOptions: mapFacetToEntityOptions(tagsFacet, displayName),
            termOptions: mapFacetToEntityOptions(termsFacet, displayName),
            ownerOptions: mapFacetToAuthorOptions(ownersFacet, displayName),
        };
    }, [platformsFacet, domainsFacet, tagsFacet, termsFacet, ownersFacet, entityRegistry]);

    const missingSelectedUrns = useMemo(
        () => [
            ...getMissingSelectedValues(facetOptions.platformOptions, platformUrns),
            ...getMissingSelectedValues(facetOptions.domainOptions, domainUrns),
            ...getMissingSelectedValues(facetOptions.tagOptions, tagUrns),
            ...getMissingSelectedValues(facetOptions.termOptions, termUrns),
            ...getMissingSelectedValues(facetOptions.ownerOptions, ownerUrns),
        ],
        [facetOptions, platformUrns, domainUrns, tagUrns, termUrns, ownerUrns],
    );
    const resolvedOptions = useResolvedFacetOptions(missingSelectedUrns, facetOptions);

    const { platformOptions, domainOptions, tagOptions, termOptions, ownerOptions } = useMemo(
        () => ({
            platformOptions: ensureSelectedOptions(facetOptions.platformOptions, platformUrns, resolvedOptions),
            domainOptions: ensureSelectedOptions(facetOptions.domainOptions, domainUrns, resolvedOptions),
            tagOptions: ensureSelectedOptions(facetOptions.tagOptions, tagUrns, resolvedOptions),
            termOptions: ensureSelectedOptions(facetOptions.termOptions, termUrns, resolvedOptions),
            ownerOptions: filterOutAiAgentAuthors(
                ensureSelectedAuthorOptions(facetOptions.ownerOptions, ownerUrns, resolvedOptions),
            ),
        }),
        [facetOptions, resolvedOptions, platformUrns, domainUrns, tagUrns, termUrns, ownerUrns],
    );

    return {
        platformOptions,
        domainOptions,
        tagOptions,
        termOptions,
        ownerOptions,
        loading: platformsLoading || domainsLoading || tagsLoading || termsLoading || ownersLoading,
    };
}
