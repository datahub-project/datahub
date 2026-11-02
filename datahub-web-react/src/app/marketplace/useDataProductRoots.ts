import { useCallback, useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import { mergeScrollPageResults } from '@app/marketplace/utils/scrollMergeUtils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useScrollDataProductsQuery } from '@graphql/marketplaceBrowse.generated';
import { EntityType, FilterOperator, SortOrder } from '@types';

export const DATA_PRODUCT_ROOT_COUNT = 50;

function buildRootScrollInput(scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.DataProduct],
            count: DATA_PRODUCT_ROOT_COUNT,
            orFilters: [
                {
                    and: [
                        {
                            field: 'hasParentDataProduct',
                            condition: FilterOperator.Equal,
                            values: ['false'],
                        },
                    ],
                },
            ],
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

/**
 * Infinite scroll for root-level data products in browse mode.
 * Filtered flat search uses `useMarketplaceSidebarSearch` instead.
 */
export default function useDataProductRoots(skip = false) {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<DataProductEntity[]>([]);

    useEffect(() => {
        if (skip) {
            // Keep prior browse results so leaving search does not flash an empty tree.
            setScrollId(null);
        }
    }, [skip]);

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollDataProductsQuery({
        skip,
        variables: buildRootScrollInput(scrollId),
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (skip) return;
        if (loading && scrollId === null) return;
        if (!scrollData?.scrollAcrossEntities?.searchResults) return;

        const fresh = scrollData.scrollAcrossEntities.searchResults
            .map((r) => r.entity)
            .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct');

        setData((currData) =>
            mergeScrollPageResults({
                current: currData,
                fresh,
                scrollId,
            }),
        );
    }, [scrollData, skip, loading, scrollId]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (skip || loading || !nextScrollId || scrollId === nextScrollId || !inView) {
            return;
        }
        setScrollId(nextScrollId);
    }, [inView, nextScrollId, scrollId, loading, skip]);

    const refetchRoots = useCallback(() => {
        setScrollId(null);
        setData([]);
        return refetch(buildRootScrollInput(null));
    }, [refetch]);

    return {
        data,
        loading: skip ? false : loading,
        error,
        scrollRef,
        refetch: refetchRoots,
    };
}
