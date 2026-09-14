import { useCallback, useEffect, useState } from 'react';
import { useInView } from 'react-intersection-observer';

import { DataProductEntity } from '@app/marketplace/marketplaceTypes';
import { mergeScrollPageResults } from '@app/marketplace/utils/scrollMergeUtils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useScrollDataProductsQuery } from '@graphql/marketplaceBrowse.generated';
import { EntityType, SortOrder } from '@types';

export const DATA_PRODUCT_CHILDREN_COUNT = 50;

type Props = {
    parentUrn: string;
    /** Pass true when the parent row is collapsed — skips the query entirely. */
    skip?: boolean;
};

function buildScrollInput(parentUrn: string, scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.DataProduct],
            count: DATA_PRODUCT_CHILDREN_COUNT,
            orFilters: [{ and: [{ field: 'parentDataProduct', values: [parentUrn] }] }],
            sortInput: {
                sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
            },
            searchFlags: { skipCache: true },
        },
    };
}

export default function useDataProductChildren({ parentUrn, skip }: Props) {
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [data, setData] = useState<DataProductEntity[]>([]);

    const [prevParentUrn, setPrevParentUrn] = useState(parentUrn);
    if (parentUrn !== prevParentUrn) {
        setPrevParentUrn(parentUrn);
        setScrollId(null);
        setData([]);
    }

    const {
        data: scrollData,
        loading,
        error,
        refetch,
    } = useScrollDataProductsQuery({
        variables: buildScrollInput(parentUrn, scrollId),
        skip: !!skip,
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
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading]);

    const refetchChildren = useCallback(() => {
        setScrollId(null);
        setData([]);
        return refetch(buildScrollInput(parentUrn, null));
    }, [parentUrn, refetch]);

    return {
        data,
        loading,
        error,
        refetch: refetchChildren,
        scrollRef,
    };
}
