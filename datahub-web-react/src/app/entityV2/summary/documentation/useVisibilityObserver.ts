import { DependencyList, useEffect, useRef, useState } from 'react';

export function useVisibilityObserver(
    maxViewHeight: number,
    dependencies: DependencyList = [], // Additional dependencies to watch
) {
    const elementRef = useRef<HTMLDivElement | null>(null);
    const [hasMore, setHasMore] = useState(false);

    useEffect(() => {
        const element = elementRef.current;
        if (!element) return undefined;

        const updateHasMore = () => {
            setHasMore(element.scrollHeight > maxViewHeight);
        };

        updateHasMore();

        // ResizeObserver to detect size changes (e.g., when images load)
        const resizeObserver = new ResizeObserver(updateHasMore);
        resizeObserver.observe(element);

        const intersectionObserver = new IntersectionObserver(
            (entries) => {
                entries.forEach((entry) => {
                    if (entry.isIntersecting) {
                        updateHasMore();
                    }
                });
            },
            { threshold: 0.01 },
        );
        intersectionObserver.observe(element);

        // Clean up observers
        return () => {
            intersectionObserver.disconnect();
            resizeObserver.disconnect();
        };

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [maxViewHeight, ...dependencies]);

    return { elementRef, hasMore };
}
