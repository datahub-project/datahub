import { useEffect, useState } from 'react';

/**
 * Whether the element is laid out (not `display: none`, directly or through an ancestor).
 *
 * Filter menus are hidden rather than unmounted when their popover or submenu closes, and
 * commit their staged selection at that point. Viewport intersection alone is not enough: a menu
 * scrolled off-screen would count as closed. The observer is only the trigger; layout decides.
 */
export function useIsRendered(ref: React.RefObject<Element>) {
    const [isRendered, setIsRendered] = useState(false);

    useEffect(() => {
        const element = ref.current;
        if (!element) return undefined;

        const update = () => setIsRendered(element.getClientRects().length > 0);
        const observer = new IntersectionObserver(update);
        observer.observe(element);
        update();
        return () => observer.disconnect();
    }, [ref]);

    return isRendered;
}
