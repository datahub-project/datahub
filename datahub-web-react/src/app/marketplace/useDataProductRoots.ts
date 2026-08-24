import { useCallback, useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useScrollDataProductsQuery } from '@graphql/marketplaceBrowse.generated';
import { EntityType, SortOrder } from '@types';

export const DATA_PRODUCT_ROOT_COUNT = 50;

function buildScrollInput(scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.DataProduct],
            count: DATA_PRODUCT_ROOT_COUNT,
            orFilters: [{ and: [{ field: 'hasParentDataProduct', values: ['false'] }] }],
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

export default function useDataProductRoots() {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<DataProductEntity[]>([]);

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollDataProductsQuery({
        variables: buildScrollInput(scrollId),
        notifyOnNetworkStatusChange: true,
    });

    useEffect(() => {
        if (scrollData?.scrollAcrossEntities?.searchResults) {
            const fresh = scrollData.scrollAcrossEntities.searchResults
                .map((r) => r.entity)
                .filter((e): e is DataProductEntity => e?.__typename === 'DataProduct');
            const freshByUrn = new Map(fresh.map((e) => [e.urn, e]));

            setData((currData) => {
                const updated = currData.map((e) => freshByUrn.get(e.urn) || e);
                const seenUrns = new Set(updated.map((e) => e.urn));
                const additions = fresh.filter((e) => !seenUrns.has(e.urn));
                if (additions.length === 0 && updated.every((e, i) => e === currData[i])) {
                    return currData;
                }
                return [...updated, ...additions];
            });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [scrollData]);

    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading]);

    const refetchRoots = useCallback(() => {
        setScrollId(null);
        setData([]);
        return refetch(buildScrollInput(null));
    }, [refetch]);

    return {
        data,
        loading,
        error,
        scrollRef,
        refetch: refetchRoots,
    };
}
