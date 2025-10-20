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

        setHasMore(element.scrollHeight > maxViewHeight);

        const observer = new IntersectionObserver(
            (entries) => {
                entries.forEach((entry) => {
                    if (entry.isIntersecting) {
                        setHasMore(element.scrollHeight > maxViewHeight);
                    }
                });
            },
            { threshold: 0.01 },
        );

        observer.observe(element);
        return () => observer.disconnect();

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [maxViewHeight, ...dependencies]);

    return { elementRef, hasMore };
}
