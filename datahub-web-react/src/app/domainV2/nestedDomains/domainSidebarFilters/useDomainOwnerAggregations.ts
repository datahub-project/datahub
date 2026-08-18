import { useMemo } from 'react';

import {
    DomainOwnerInfo,
    extractOwnerOptionsFromFacets,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FacetMetadata } from '@types';

// Generous-but-bounded cap on distinct owners returned for the dropdown.
// Matches the cap used by the main search sidebar's aggregation query
// (`MAX_AGGREGATION_VALUES` in `searchV2/sidebar/constants.ts`).
const DOMAIN_OWNER_FACET_MAX = 100;

/**
 * Source the Owner multi-select options for the domain sidebar from a single
 * dedicated server-side aggregation, instead of stitching them together from
 * whatever owners happened to be attached to the lazily-paginated domain
 * rows currently in view.
 *
 * Two properties matter for correctness:
 *   1. No `parentDomain` / `orFilters` are sent, so the aggregation spans
 *      every domain in the index — including subdomains at arbitrary depth.
 *      Owners that only appear on a deeply nested subdomain still surface
 *      in the dropdown.
 *   2. No `owners` filter is sent, so selecting Alice doesn't shrink the
 *      dropdown to "owners who co-own things with Alice" — users can always
 *      add a second owner without first deselecting the first.
 *
 * Uses `cache-first` + `previousData` fallback (same pattern as
 * `useAggregationsQuery` in the main search sidebar) so the dropdown doesn't
 * flicker empty between renders.
 *
 * @param skip Skip the query entirely — pass true outside the sidebar
 *             variant so picker variants don't fire a useless network call.
 */
export default function useDomainOwnerAggregations({ skip = false }: { skip?: boolean } = {}): {
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
                types: [EntityType.Domain],
                query: '*',
                facets: [OWNERS_FILTER_NAME],
                searchFlags: {
                    maxAggValues: DOMAIN_OWNER_FACET_MAX,
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
