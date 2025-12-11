/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback, useEffect, useState } from 'react';

export function useWindowResize(callback: () => void): void {
    useEffect(() => {
        window.addEventListener('resize', callback);

        return () => {
            window.removeEventListener('resize', callback);
        };
    }, [callback]);
}

export default function useSidebarWidth(ratio = 0.3): number {
    const [width, setWidth] = useState(window.innerWidth * ratio);

    const resize = useCallback(() => {
        setWidth(window.innerWidth * ratio);
    }, [ratio]);
    useWindowResize(resize);

    return width;
}
