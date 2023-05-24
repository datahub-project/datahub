import { useEffect, useRef } from 'react';

type Props = {
    skip?: boolean;
    initialDelay?: number;
    onIntersect: () => void;
};

const useIntersect = ({ skip = false, initialDelay = 0, onIntersect }: Props) => {
    const observableRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        const observer = new window.IntersectionObserver((entries) => {
            const intersectingEntry = entries.find((entry) => entry.isIntersecting);
            if (!skip && intersectingEntry) onIntersect();
        });

        const timer = window.setTimeout(() => {
            if (!skip && observableRef.current) observer.observe(observableRef.current);
        }, initialDelay);

        return () => {
            window.clearTimeout(timer);
            observer.disconnect();
        };
    }, [initialDelay, onIntersect, skip]);

    return { observableRef };
};

export default useIntersect;
