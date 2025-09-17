import React from 'react';

import { ObserverContainer } from '@components/components/InfiniteScrollList/components';
import { InfiniteScrollListProps } from '@components/components/InfiniteScrollList/types';
import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';
import { Loader } from '@components/components/Loader';

export function InfiniteScrollList<T>({
    fetchData,
    renderItem,
    pageSize,
    emptyState,
    totalItemCount,
    showLoader = true,
}: InfiniteScrollListProps<T>) {
    const { items, loading, observerRef, hasMore } = useInfiniteScroll({
        fetchData,
        pageSize,
        totalItemCount,
    });

    return (
        <>
            {items.length === 0 && !loading && emptyState}
            {items.map((item) => renderItem(item))}
            {hasMore && <ObserverContainer ref={observerRef} />}
            {items.length > 0 && showLoader && loading && <Loader size="sm" alignItems="center" />}
        </>
    );
}
