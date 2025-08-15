import { useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import useManageDomains from '@app/domainV2/useManageDomains';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { ListDomainFragment } from '@graphql/domain.generated';
import { useScrollAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator, SortOrder } from '@types';

export const DOMAIN_COUNT = 25;

export function getDomainsScrollInput(parentDomain: string | null, scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.Domain],
            orFilters: parentDomain
                ? [{ and: [{ field: 'parentDomain', values: [parentDomain] }] }]
                : [{ and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] }],
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
}

export default function useScrollDomains({ parentDomain, skip }: Props) {
    const [hasInitialized, setHasInitialized] = useState(false);
    const [data, setData] = useState<ListDomainFragment[]>([]);
    const [dataUrnsSet, setDataUrnsSet] = useState<Set<string>>(new Set());
    const [scrollId, setScrollId] = useState<string | null>(null);
    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollAcrossEntitiesQuery({
        variables: {
            ...getDomainsScrollInput(parentDomain || null, scrollId),
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

    const [scrollRef, inView] = useInView({ triggerOnce: false });

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
