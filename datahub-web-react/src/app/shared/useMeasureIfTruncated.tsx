/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
