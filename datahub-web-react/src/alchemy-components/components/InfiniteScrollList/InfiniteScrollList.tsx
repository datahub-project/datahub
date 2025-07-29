import React from 'react';

import { ObserverContainer } from '@components/components/InfiniteScrollList/components';
import { InfiniteScrollListProps } from '@components/components/InfiniteScrollList/types';
import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

export function InfiniteScrollList<T>({
    fetchData,
    renderItem,
    pageSize,
    emptyState,
    totalItemCount,
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
        </>
    );
}
