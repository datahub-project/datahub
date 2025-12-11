/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useRef } from 'react';

type Props = {
    skip?: boolean;
    options?: IntersectionObserverInit;
    onIntersect: () => void;
};

const NOOP = () => {};

const useIntersect = ({ skip = false, options = {}, onIntersect }: Props) => {
    const observableRef = useRef<HTMLDivElement | null>(null);
    const { root, rootMargin, threshold } = options;

    useEffect(() => {
        if (skip) return NOOP;

        const observer = new window.IntersectionObserver(
            (entries) => {
                if (entries.some((entry) => entry.isIntersecting)) onIntersect();
            },
            { root, rootMargin, threshold },
        );

        if (observableRef.current) observer.observe(observableRef.current);

        return () => {
            observer.disconnect();
        };
    }, [onIntersect, root, rootMargin, skip, threshold]);

    return { observableRef };
};

export default useIntersect;
