import { useCallback, useState } from 'react';

export default function useMeasureIfTrancated() {
    const [isHorizontallyTruncated, setIsHorizontallyTruncated] = useState<boolean>(false);
    const [isVerticallyTruncated, setIsVerticallyTruncated] = useState<boolean>(false);

    const measuredRef = useCallback((node: HTMLDivElement | null) => {
        if (node !== null) {
            const resizeObserver = new ResizeObserver(() => {
                setIsHorizontallyTruncated(node.scrollWidth > node.clientWidth + 1);
                setIsVerticallyTruncated(node.scrollHeight > node.clientHeight + 1);
            });
            resizeObserver.observe(node);
        }
    }, []);

    return { measuredRef, isHorizontallyTruncated, isVerticallyTruncated };
}
