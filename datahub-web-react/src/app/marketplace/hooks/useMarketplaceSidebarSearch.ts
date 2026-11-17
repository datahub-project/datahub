import { useEffect, useMemo, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import { buildMarketplaceSidebarFilters } from '@app/marketplace/utils/marketplaceSidebarFilters';
import { marketplaceSidebarSearchQuery } from '@app/marketplace/utils/marketplaceSidebarMode';
import { mergeScrollPageResults } from '@app/marketplace/utils/scrollMergeUtils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { UnionType } from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { useScrollDataProductsQuery } from '@graphql/marketplaceBrowse.generated';
import { EntityType, SortOrder } from '@types';

const MARKETPLACE_SIDEBAR_SEARCH_COUNT = 50;

type Props = {
    searchQuery: string;
    domainUrns?: string[];
    tagUrns?: string[];
    termUrns?: string[];
    ownerUrns?: string[];
    applicationUrns?: string[];
    viewUrn?: string | null;
    skip?: boolean;
};

/**
 * Flat marketplace sidebar search via scrollAcrossEntities.
 * Spans all data products at every depth — used when query and/or filters are active.
 */
export default function useMarketplaceSidebarSearch({
    searchQuery,
    domainUrns = [],
    tagUrns = [],
    termUrns = [],
    ownerUrns = [],
    applicationUrns = [],
    viewUrn,
    skip,
}: Props) {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [dataProducts, setDataProducts] = useState<DataProductEntity[]>([]);

    const query = marketplaceSidebarSearchQuery(searchQuery);
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
    const orFilters = useMemo(() => generateOrFilters(UnionType.AND, appliedFilters), [appliedFilters]);

    const criteriaKey = useMemo(
        () =>
            JSON.stringify({
                query,
                domainUrns,
                tagUrns,
                termUrns,
                ownerUrns,
                applicationUrns,
                viewUrn: viewUrn ?? null,
            }),
        [query, domainUrns, tagUrns, termUrns, ownerUrns, applicationUrns, viewUrn],
    );

    // Reset scroll cursor synchronously when criteria change so the next query never
    // pairs a new search with the previous search's scrollId.
    const [prevCriteriaKey, setPrevCriteriaKey] = useState(criteriaKey);
    if (criteriaKey !== prevCriteriaKey) {
        setPrevCriteriaKey(criteriaKey);
        setScrollId(null);
        setDataProducts([]);
    }

    const {
        data: scrollData,
        loading,
        previousData,
        error,
    } = useScrollDataProductsQuery({
        variables: {
            input: {
                scrollId,
                query,
                types: [EntityType.DataProduct],
                count: MARKETPLACE_SIDEBAR_SEARCH_COUNT,
                orFilters: orFilters.length > 0 ? orFilters : undefined,
                viewUrn: viewUrn ?? undefined,
                sortInput: {
                    sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
                },
                searchFlags: { skipCache: true },
            },
        },
        skip: !!skip,
        notifyOnNetworkStatusChange: true,
        fetchPolicy: 'network-only',
    });

    useEffect(() => {
        if (skip || error) return;
        if (loading && scrollId === null) return;
        if (!scrollData?.scrollAcrossEntities?.searchResults) return;

        const fresh = scrollData.scrollAcrossEntities.searchResults
            .map((r) => r.entity)
            .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct');

        setDataProducts((currData) =>
            mergeScrollPageResults({
                current: currData,
                fresh,
                scrollId,
            }),
        );
    }, [scrollData, skip, error, loading, scrollId]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;
    const total =
        skip || error ? 0 : (scrollData?.scrollAcrossEntities?.total ?? previousData?.scrollAcrossEntities?.total ?? 0);

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!skip && !loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading, skip]);

    const isRefreshing = !skip && loading && dataProducts.length > 0 && scrollId === null;

    return {
        dataProducts: skip || error ? [] : dataProducts,
        total,
        loading: skip ? false : loading,
        isRefreshing,
        scrollRef,
    };
}
