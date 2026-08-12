import { useMemo } from 'react';

import {
    AuthorFacetOption,
    FacetSelectOption,
    ensureSelectedAuthorOptions,
    ensureSelectedOptions,
    filterOutAiAgentAuthors,
    mapFacetToAuthorOptions,
    mapFacetToEntityOptions,
    mapFacetToTypeOptions,
} from '@app/document/utils/documentSidebarFacets.utils';
import { DOCUMENT_CREATOR_FILTER_NAME, buildDocumentSidebarFilters } from '@app/document/utils/documentSidebarFilters';
import { DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
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
    typeNames?: string[];
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    authorUrns?: string[];
    platformUrns?: string[];
    status?: DocumentStatusFilter;
    viewUrn?: string | null;
    /** Skip Tag / Author / Source aggs until those filters are promoted (or selected). */
    includeTagFacets?: boolean;
    includeAuthorFacets?: boolean;
    includePlatformFacets?: boolean;
};

function useDocumentFacetAggregations(
    field: string,
    appliedFilters: FacetFilterInput[],
    searchQuery: string,
    viewUrn?: string | null,
    skip?: boolean,
) {
    // Same as global search: exclude the field being aggregated so the dropdown still
    // shows alternative values while other filters narrow the set.
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
                types: [EntityType.Document],
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

    // previousData avoids empty flicker between dependent-facet refetches (useAggregationsQuery).
    const data = error ? null : (newData ?? previousData);
    const facet = (data?.aggregateAcrossEntities?.facets ?? []).find((f) => f.field === field) as
        | FacetMetadata
        | undefined;

    return { facet, loading: skip ? false : loading };
}

/**
 * Type / domain / tag / term / author / source options for the documents sidebar.
 * Per-field aggregations (not one batched query) so each dropdown can exclude its
 * own filter — same dependent-facet pattern as useSearchFilterDropdown.
 */
export default function useDocumentSidebarFacetOptions({
    searchQuery = '',
    typeNames = [],
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    authorUrns = [],
    platformUrns = [],
    status = 'all',
    viewUrn,
    includeTagFacets = true,
    includeAuthorFacets = true,
    includePlatformFacets = true,
}: AppliedFilters = {}): {
    typeOptions: FacetSelectOption[];
    domainOptions: FacetSelectOption[];
    tagOptions: FacetSelectOption[];
    termOptions: FacetSelectOption[];
    authorOptions: AuthorFacetOption[];
    platformOptions: FacetSelectOption[];
    loading: boolean;
} {
    const entityRegistry = useEntityRegistry();
    const appliedFilters = useMemo(
        () =>
            buildDocumentSidebarFilters({
                typeNames,
                domainUrns,
                tagUrns,
                termUrns,
                authorUrns,
                platformUrns,
                status,
            }),
        [typeNames, domainUrns, tagUrns, termUrns, authorUrns, platformUrns, status],
    );

    const { facet: typesFacet, loading: typesLoading } = useDocumentFacetAggregations(
        TYPE_NAMES_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
    );
    const { facet: domainsFacet, loading: domainsLoading } = useDocumentFacetAggregations(
        DOMAINS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
    );
    const { facet: tagsFacet, loading: tagsLoading } = useDocumentFacetAggregations(
        TAGS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeTagFacets,
    );
    const { facet: termsFacet, loading: termsLoading } = useDocumentFacetAggregations(
        GLOSSARY_TERMS_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
    );
    const { facet: creatorsFacet, loading: creatorsLoading } = useDocumentFacetAggregations(
        DOCUMENT_CREATOR_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includeAuthorFacets,
    );
    const { facet: platformsFacet, loading: platformsLoading } = useDocumentFacetAggregations(
        PLATFORM_FILTER_NAME,
        appliedFilters,
        searchQuery,
        viewUrn,
        !includePlatformFacets,
    );

    const { typeOptions, domainOptions, tagOptions, termOptions, authorOptions, platformOptions } = useMemo(() => {
        const displayName = (type: EntityType, entity: Entity) => entityRegistry.getDisplayName(type, entity);

        return {
            typeOptions: ensureSelectedOptions(mapFacetToTypeOptions(typesFacet), typeNames),
            domainOptions: ensureSelectedOptions(mapFacetToEntityOptions(domainsFacet, displayName), domainUrns),
            tagOptions: ensureSelectedOptions(mapFacetToEntityOptions(tagsFacet, displayName), tagUrns),
            termOptions: ensureSelectedOptions(mapFacetToEntityOptions(termsFacet, displayName), termUrns),
            authorOptions: filterOutAiAgentAuthors(
                ensureSelectedAuthorOptions(mapFacetToAuthorOptions(creatorsFacet, displayName), authorUrns),
            ),
            platformOptions: ensureSelectedOptions(mapFacetToEntityOptions(platformsFacet, displayName), platformUrns),
        };
    }, [
        typesFacet,
        domainsFacet,
        tagsFacet,
        termsFacet,
        creatorsFacet,
        platformsFacet,
        entityRegistry,
        typeNames,
        domainUrns,
        tagUrns,
        termUrns,
        authorUrns,
        platformUrns,
    ]);

    return {
        typeOptions,
        domainOptions,
        tagOptions,
        termOptions,
        authorOptions,
        platformOptions,
        loading: typesLoading || domainsLoading || tagsLoading || termsLoading || creatorsLoading || platformsLoading,
    };
}
