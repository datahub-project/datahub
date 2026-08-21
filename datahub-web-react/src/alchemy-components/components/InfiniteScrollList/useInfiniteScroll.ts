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

    // Refs so sequential await loadMore() calls (document-tree reveal) see fresh
    // gates/offsets without waiting for a React re-render of the callback closure.
    const loadingRef = useRef(false);
    const hasMoreRef = useRef(true);
    const fetchDataRef = useRef(fetchData);
    fetchDataRef.current = fetchData;

    // Ref element to be observed by IntersectionObserver
    const observerRef = useRef<HTMLDivElement | null>(null);

    // Ref for initial loading
    const initialLoadedRef = useRef(false);

    // Track prepended keys to avoid duplicates across resets
    const prependedKeysRef = useRef<Set<string | number>>(new Set());

    // Function to fetch the next batch of items, invoked when observer comes into view.
    // Returns the fetched page so callers (e.g. document-tree reveal) can wait on the batch.
    const loadMore = useCallback(async (): Promise<T[]> => {
        if (loadingRef.current || !hasMoreRef.current) return [];

        loadingRef.current = true;
        setLoading(true);

        try {
            const newItems = await fetchDataRef.current(startIndex.current, pageSize);
            if (!Array.isArray(newItems)) return [];

            startIndex.current += newItems.length;
            const nextHasMore = totalItemCount ? startIndex.current < totalItemCount : newItems.length === pageSize;
            hasMoreRef.current = nextHasMore;
            setHasMore(nextHasMore);

            setItems((prev) => [...prev, ...newItems]);

            return newItems;
        } finally {
            loadingRef.current = false;
            setLoading(false);
        }
    }, [pageSize, totalItemCount]);

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

    // Reset and reload when resetTrigger changes (e.g. document sidebar sort).
    useEffect(() => {
        setItems([]);
        startIndex.current = 0;
        hasMoreRef.current = true;
        setHasMore(true);
        loadingRef.current = false;
        setLoading(false);
        initialLoadedRef.current = false;
        prependedKeysRef.current = new Set();
    }, [resetTrigger]);

    // Initial load + reload after resetTrigger clears the list
    useEffect(() => {
        if (!initialLoadedRef.current) {
            initialLoadedRef.current = true;
            loadMore();
        }
    }, [loadMore, resetTrigger]);

    // Intersection Observer — re-bind when loading settles so a late-assigned
    // observerRef (tests / deferred mount) still gets observed.
    useEffect(() => {
        if (!observerRef.current || !hasMore) return undefined;

        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting && !loadingRef.current) {
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

    return { items, loading, observerRef, hasMore, loadMore, prependItem, removeItem, updateItem };
}
