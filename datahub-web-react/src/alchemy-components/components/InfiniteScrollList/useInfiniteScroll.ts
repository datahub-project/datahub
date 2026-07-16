import { useCallback, useEffect, useRef, useState } from 'react';

interface Props<T> {
    fetchData: (start: number, count: number) => Promise<T[]>;
    pageSize?: number;
    totalItemCount?: number;
    resetTrigger?: string | number | boolean;
    getKey?: (item: T) => string | number;
}

export function useInfiniteScroll<T>({
    fetchData,
    pageSize = 10,
    totalItemCount,
    resetTrigger,
    getKey = (item: T) => (item as any).urn,
}: Props<T>) {
    const [items, setItems] = useState<T[]>([]);
    const [loading, setLoading] = useState(false);
    const [hasMore, setHasMore] = useState(true);
    const startIndex = useRef(0);

    // Ref element to be observed by IntersectionObserver
    const observerRef = useRef<HTMLDivElement | null>(null);

    // Ref for initial loading
    const initialLoadedRef = useRef(false);

    // Track prepended keys to avoid duplicates across resets
    const prependedKeysRef = useRef<Set<string | number>>(new Set());

    // Function to fetch the next batch of items, invoked when observer comes into view
    const loadMore = useCallback(() => {
        if (loading || !hasMore) return;

        setLoading(true);

        fetchData(startIndex.current, pageSize)
            .then((newItems) => {
                if (!Array.isArray(newItems)) return;

                setItems((prev) => {
                    // Append newly fetched items to updated list
                    const updated = [...prev, ...newItems];
                    startIndex.current = updated.length;

                    if (totalItemCount) {
                        setHasMore(updated.length < totalItemCount);
                    } else {
                        setHasMore(newItems.length === pageSize);
                    }

                    return updated;
                });
            })
            .finally(() => {
                setLoading(false);
            });
    }, [fetchData, loading, hasMore, pageSize, totalItemCount]);

    // Update items to show immediate feedback on the UI after operations

    // Add new item at the top
    const prependItem = useCallback(
        (newItem: T) => {
            if (newItem == null) return;
            const key = getKey(newItem);

            setItems((prev) => {
                if (prev.some((item) => getKey(item) === key) || prependedKeysRef.current.has(key)) {
                    return prev;
                }

                prependedKeysRef.current.add(key);
                return [newItem, ...prev];
            });
        },
        [getKey],
    );

    const removeItem = useCallback((shouldRemove: (item: T) => boolean) => {
        setItems((prev) => {
            const filtered = prev.filter((item) => !shouldRemove(item));
            startIndex.current = filtered.length;
            return filtered;
        });
    }, []);

    const updateItem = useCallback((updatedItem: T, shouldUpdate: (item: T) => boolean) => {
        setItems((prev) => prev.map((item) => (shouldUpdate(item) ? updatedItem : item)));
    }, []);

    // Reset items and startIndex when resetTrigger changes
    useEffect(() => {
        setItems([]);
        startIndex.current = 0;
    }, [resetTrigger]);

    // Initial load
    useEffect(() => {
        if (!initialLoadedRef.current) {
            initialLoadedRef.current = true;
            loadMore();
        }
    }, [loadMore]);

    // Intersection Observer
    useEffect(() => {
        if (!observerRef.current || !hasMore) return undefined;

        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting && !loading) {
                    loadMore();
                }
            },
            { threshold: 0.1 },
        );

        const currentObserverRef = observerRef.current;
        observer.observe(currentObserverRef);

        return () => {
            observer.unobserve(currentObserverRef);
            observer.disconnect();
        };
    }, [loadMore, hasMore, loading]);

    return { items, loading, observerRef, hasMore, prependItem, removeItem, updateItem };
}
