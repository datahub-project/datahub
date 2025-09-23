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
