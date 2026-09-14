import { useEffect, useState } from 'react';

/** Single Column Views breakpoint: below this the popover becomes a bottom sheet and the builder stacks. */
export const NARROW_VIEWPORT_MAX_PX = 768;

const QUERY = `(max-width: ${NARROW_VIEWPORT_MAX_PX}px)`;

export function useIsNarrowViewport(): boolean {
    const [isNarrow, setIsNarrow] = useState<boolean>(() =>
        typeof window !== 'undefined' && typeof window.matchMedia === 'function' ? window.matchMedia(QUERY).matches : false,
    );

    useEffect(() => {
        if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return undefined;
        const media = window.matchMedia(QUERY);
        const onChange = (e: MediaQueryListEvent) => setIsNarrow(e.matches);
        setIsNarrow(media.matches);
        if (typeof media.addEventListener === 'function') {
            media.addEventListener('change', onChange);
            return () => media.removeEventListener('change', onChange);
        }
        media.addListener(onChange);
        return () => media.removeListener(onChange);
    }, []);

    return isNarrow;
}
