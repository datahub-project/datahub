/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
