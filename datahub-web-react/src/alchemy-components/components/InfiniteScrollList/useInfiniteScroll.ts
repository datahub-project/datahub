import { useCallback, useEffect, useRef, useState } from 'react';

interface Props<T> {
    fetchData: (start: number, count: number) => Promise<T[]>;
    pageSize?: number;
    totalItemCount?: number;
}

export function useInfiniteScroll<T>({ fetchData, pageSize = 10, totalItemCount }: Props<T>) {
    const [items, setItems] = useState<T[]>([]);
    const [loading, setLoading] = useState(false);
    const [hasMore, setHasMore] = useState(true);
    const start = useRef(0);

    // Ref element to be observed by IntersectionObserver
    const observerRef = useRef<HTMLDivElement | null>(null);

    // Function to fetch the next data batch, invoked initially and when observer comes into view
    const loadMore = useCallback(() => {
        if (loading || !hasMore) return;

        setLoading(true);

        fetchData(start.current, pageSize)
            .then((newItems) => {
                // Append newly fetched items to current list
                setItems((prev) => [...prev, ...newItems]);
                // Advance the start index by the number of new items fetched
                start.current += pageSize;
                // Update hasMore depending on totalItemCount or inferred from batch size
                if (totalItemCount !== undefined) {
                    setHasMore(start.current < totalItemCount);
                } else {
                    setHasMore(newItems.length === pageSize);
                }
            })
            .finally(() => {
                setLoading(false);
            });
    }, [fetchData, loading, hasMore, pageSize, totalItemCount]);

    // Initial load
    useEffect(() => {
        if (items.length === 0 && hasMore && !loading) {
            loadMore();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [hasMore]);

    // Intersection Observer
    useEffect(() => {
        if (!observerRef.current || !hasMore) return undefined;

        const observer = new IntersectionObserver((entries) => {
            if (entries[0].isIntersecting) {
                loadMore();
            }
        });

        const currentObserverRef = observerRef.current;
        observer.observe(currentObserverRef);

        return () => {
            observer.unobserve(currentObserverRef);
            observer.disconnect();
        };
    }, [loadMore, hasMore]);

    return { items, loading, observerRef, hasMore };
}
