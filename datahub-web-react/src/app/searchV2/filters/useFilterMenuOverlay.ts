import '@app/searchV2/filters/filter-menu-overlay.less';

import { CSSProperties, useLayoutEffect, useRef, useState } from 'react';

const FILTER_MENU_OVERLAY_CLASS_NAME = 'filter-menu-overlay';
const VIEWPORT_MARGIN = 16;
// Below this the list would be too short to use, so let antd place the menu above instead.
const MIN_MENU_HEIGHT = 240;

/**
 * Sizes a filter menu to fit between its trigger and the bottom of the window. The menus list
 * every filter field, so without this they rarely fit below a trigger lower on the page, and antd
 * flips them up over the trigger, where they get clipped by the top of the window.
 */
export function useFilterMenuOverlay<T extends HTMLElement>(isOpen: boolean) {
    const triggerRef = useRef<T>(null);
    const [maxHeight, setMaxHeight] = useState<number>();

    useLayoutEffect(() => {
        if (!isOpen || !triggerRef.current) return;
        const { bottom } = triggerRef.current.getBoundingClientRect();
        setMaxHeight(Math.max(window.innerHeight - bottom - VIEWPORT_MARGIN, MIN_MENU_HEIGHT));
    }, [isOpen]);

    const overlayStyle = maxHeight ? ({ '--filter-menu-max-height': `${maxHeight}px` } as CSSProperties) : undefined;

    return { triggerRef, overlayClassName: FILTER_MENU_OVERLAY_CLASS_NAME, overlayStyle };
}
