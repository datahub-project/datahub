import { useEffect, useRef } from 'react';

type Props = {
    skip?: boolean;
    initialDelay?: number;
    options?: IntersectionObserverInit;
    onIntersect: () => void;
};

const NOOP = () => {};

const useIntersect = ({ skip = false, initialDelay = 0, options = {}, onIntersect }: Props) => {
    const observableRef = useRef<HTMLDivElement | null>(null);
    const { root, rootMargin, threshold } = options;

    useEffect(() => {
        if (skip) return NOOP;

        const observer = new window.IntersectionObserver(
            (entries) => {
                const intersectingEntry = entries.find((entry) => entry.isIntersecting);
                if (intersectingEntry) onIntersect();
            },
            { root, rootMargin, threshold },
        );

        const timer = window.setTimeout(() => {
            if (observableRef.current) observer.observe(observableRef.current);
        }, initialDelay);

        return () => {
            window.clearTimeout(timer);
            observer.disconnect();
        };
    }, [initialDelay, onIntersect, root, rootMargin, skip, threshold]);

    return { observableRef };
};

export default useIntersect;
