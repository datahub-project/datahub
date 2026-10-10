import { useMemo } from 'react';

import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useGetDomainsOwnedByQuery } from '@graphql/domain.generated';
import { useGetAutoCompleteMultipleResultsQuery, useGetAutoCompleteResultsQuery } from '@graphql/search.generated';
import { Domain, Entity, EntityType } from '@types';

const OWNER_ENTITY_TYPES = [EntityType.CorpUser, EntityType.CorpGroup];
// Applied per owner entity type by autoCompleteForMultiple, so up to this many users and this many
// groups can each contribute. No global cap is applied afterwards; otherwise five matching users
// would silently discard every matching group.
const MAX_MATCHED_OWNERS_PER_TYPE = 5;
const MAX_OWNED_DOMAINS = 10;

export interface DomainSearchResult {
    entity: Entity;
    /** Set when the domain was found through one of its owners rather than its own name. */
    matchedOwner?: Entity;
}

/**
 * Finds domains for the sidebar search box. Domains whose name matches the query come first,
 * followed by domains owned by any user or group whose name matches the query. Owner URNs are
 * not full-text searchable on the domain index, so owners are resolved through their own
 * autocomplete and then used as a filter.
 */
export default function useDomainSearchResults(query: string): { results: DomainSearchResult[]; loading: boolean } {
    const skip = !query;

    const { data: domainData, loading: domainsLoading } = useGetAutoCompleteResultsQuery({
        variables: { input: { type: EntityType.Domain, query } },
        skip,
    });

    const { data: ownerData, loading: ownersLoading } = useGetAutoCompleteMultipleResultsQuery({
        variables: { input: { types: OWNER_ENTITY_TYPES, query, limit: MAX_MATCHED_OWNERS_PER_TYPE } },
        skip,
    });

    const matchedOwners = useMemo(
        () => (ownerData?.autoCompleteForMultiple?.suggestions || []).flatMap((suggestion) => suggestion.entities),
        [ownerData],
    );
    const matchedOwnerUrns = useMemo(() => matchedOwners.map((owner) => owner.urn), [matchedOwners]);

    const { data: ownedData, loading: ownedLoading } = useGetDomainsOwnedByQuery({
        variables: {
            input: {
                types: [EntityType.Domain],
                query: '*',
                start: 0,
                count: MAX_OWNED_DOMAINS,
                orFilters: [{ and: [{ field: OWNERS_FILTER_NAME, values: matchedOwnerUrns }] }],
            },
        },
        skip: skip || matchedOwnerUrns.length === 0,
    });

    const results = useMemo(() => {
        const nameMatches: DomainSearchResult[] = (domainData?.autoComplete?.entities || []).map((entity) => ({
            entity,
        }));
        const seenUrns = new Set(nameMatches.map((result) => result.entity.urn));
        const ownersByUrn = new Map(matchedOwners.map((owner) => [owner.urn, owner]));

        // Apollo keeps the previous result when the owned-domains query flips back to `skip`, so a
        // query with no matching owners must not merge in stale domains from the last one that had.
        const ownedResults = matchedOwners.length > 0 ? ownedData?.searchAcrossEntities?.searchResults || [] : [];

        const ownerMatches: DomainSearchResult[] = [];
        ownedResults.forEach(({ entity }) => {
            if (seenUrns.has(entity.urn)) return;
            seenUrns.add(entity.urn);
            const matchedOwner = ((entity as Domain).ownership?.owners || [])
                .map((owner) => ownersByUrn.get(owner.owner.urn))
                .find((owner) => owner !== undefined);
            ownerMatches.push({ entity, matchedOwner });
        });

        return [...nameMatches, ...ownerMatches];
    }, [domainData, ownedData, matchedOwners]);

    // Name matches arrive before the owner-backed lookup, which depends on a second round trip.
    // Only report loading while there is nothing to show, so the dropdown is not blanked out while
    // owner matches are still being appended.
    const anyLoading = domainsLoading || ownersLoading || ownedLoading;
    return { results, loading: anyLoading && results.length === 0 };
}
