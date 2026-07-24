import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import useManageDomains from '@app/domainV2/useManageDomains';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { ListDomainFragment } from '@graphql/domain.generated';
import { useScrollAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FacetFilterInput, FilterOperator, SortOrder } from '@types';

export const DOMAIN_COUNT = 25;

export function getDomainsScrollInput(
    parentDomain: string | null,
    scrollId: string | null,
    selectedOwnerUrns?: ReadonlyArray<string> | null,
    ignoreParentScope?: boolean,
) {
    const filters: FacetFilterInput[] = [];
    // `ignoreParentScope` switches the query from "domains at this level of
    // the hierarchy" to "domains anywhere in the index" — used by the
    // sidebar's flat-list mode when an owner filter is active, so we don't
    // miss matching subdomains whose parents don't match the filter.
    if (!ignoreParentScope) {
        const parentFilter: FacetFilterInput = parentDomain
            ? { field: 'parentDomain', values: [parentDomain] }
            : { field: 'parentDomain', condition: FilterOperator.Exists, negated: true };
        filters.push(parentFilter);
    }

    if (selectedOwnerUrns && selectedOwnerUrns.length > 0) {
        filters.push({ field: OWNERS_FILTER_NAME, values: [...selectedOwnerUrns] });
    }

    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.Domain],
            // Always emit an `orFilters` array. When no clauses apply (flat
            // mode with no owner selection) the server sees `[{ and: [] }]`,
            // which is equivalent to "no filter", and consumers reading
            // `result.input.orFilters` always get a stable shape to assert
            // against.
            orFilters: [{ and: filters }],
            count: DOMAIN_COUNT,
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

interface Props {
    parentDomain?: string;
    skip?: boolean;
    /**
     * Optional server-side owner filter. When non-empty, the scroll query asks
     * the backend to return only domains owned by at least one of these URNs.
     * Passed only by the sidebar variant of `DomainNavigator` / `DomainNode`;
     * picker variants and the `/domains` index page omit it.
     */
    selectedOwnerUrns?: ReadonlyArray<string>;
    /**
     * When true, drop the `parentDomain` clause from the scroll query so the
     * result set spans every domain at every depth. Used by the sidebar's
     * flat-list "filter results" mode — without this, an owner filter would
     * be ANDed with `parentDomain NOT EXISTS` and silently exclude matching
     * subdomains whose parents don't match the filter (the bug John flagged
     * in PR #18088 review).
     */
    ignoreParentScope?: boolean;
}

export default function useScrollDomains({ parentDomain, skip, selectedOwnerUrns, ignoreParentScope }: Props) {
    const [hasInitialized, setHasInitialized] = useState(false);
    const [data, setData] = useState<ListDomainFragment[]>([]);
    const [dataUrnsSet, setDataUrnsSet] = useState<Set<string>>(new Set());
    const [scrollId, setScrollId] = useState<string | null>(null);

    // Reset the in-memory accumulator whenever the *filter scope* changes
    // (parent domain or owner selection). Without this, switching the owner
    // filter from Alice to Bob would leave Alice's already-fetched domains
    // stuck in `data` because the URN-dedup guard would silently drop the
    // newly-fetched Bob domains as "already seen" without ever flushing
    // Alice's. Memoize the owner-filter key on a sorted join so reference
    // identity (Apollo refetches) doesn't trip the reset.
    const ownerKey = useMemo(
        () => (selectedOwnerUrns ? [...selectedOwnerUrns].sort().join(',') : ''),
        [selectedOwnerUrns],
    );
    useEffect(() => {
        setData([]);
        setDataUrnsSet(new Set());
        setScrollId(null);
        setHasInitialized(false);
    }, [parentDomain, ownerKey, ignoreParentScope]);

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollAcrossEntitiesQuery({
        variables: {
            ...getDomainsScrollInput(parentDomain || null, scrollId, selectedOwnerUrns, ignoreParentScope),
        },
        skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'cache-and-network', // to do check this still good
    });

    // Manage CRUD of domains on domains page
    useManageDomains({ dataUrnsSet, setDataUrnsSet, setData, parentDomain });

    // Handle initial data and updates from scroll
    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const newResults = (scrollData.scrollAcrossEntities.searchResults
                .filter((r) => !dataUrnsSet.has(r.entity.urn))
                .map((r) => r.entity)
                .filter((e) => e.type === EntityType.Domain) || []) as ListDomainFragment[];

            if (newResults.length > 0) {
                setData((currData) => [...currData, ...newResults]);
                setDataUrnsSet((currSet) => {
                    const newSet = new Set(currSet);
                    newResults.forEach((r) => newSet.add(r.urn));
                    return newSet;
                });
            }
            setHasInitialized(true);
        }
    }, [scrollData, dataUrnsSet]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    const [scrollRef, inView] = useInView({ triggerOnce: false, rootMargin: '200px' });

    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading]);

    return {
        domains: data,
        hasInitialized,
        loading,
        error,
        refetch,
        scrollRef,
    };
}
