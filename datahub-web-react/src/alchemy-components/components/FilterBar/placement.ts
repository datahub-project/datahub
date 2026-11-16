/** Where a filter bar menu opens relative to its trigger. */
export type MenuPlacement = 'bottom' | 'right';

export type ViewportBounds = {
    top: number;
    bottom: number;
    left: number;
    right: number;
};

export type TriggerRect = Pick<DOMRect, 'top' | 'bottom' | 'left' | 'right'>;

export type PopoverPlacement = {
    top?: number;
    bottom?: number;
    left: number;
    maxHeight: number;
};

/** Gap between the trigger and the menu. */
export const PANEL_OFFSET_PX = 4;
/** Gap between a menu and the element it is anchored beside. */
export const ANCHOR_GAP_PX = 8;
/** Gap between the menu and the viewport edge. */
export const VIEWPORT_MARGIN_PX = 12;
/** Floor so a nearly-offscreen trigger still gets a usable menu. */
const MIN_MENU_HEIGHT_PX = 160;
/** Below this much room under the trigger, the menu opens above it when there is more room there. */
const PREFERRED_MENU_HEIGHT_PX = 320;

/** The viewport left of a box of `width` starting at `left`, shifted as little as needed to stay inside the viewport. */
export function clampLeftWithinViewport(left: number, width: number, viewport: ViewportBounds): number {
    const min = viewport.left + VIEWPORT_MARGIN_PX;
    const max = Math.max(min, viewport.right - VIEWPORT_MARGIN_PX - width);
    return Math.round(Math.min(max, Math.max(min, left)));
}

/** Opens below the trigger, or above it when there is little room below and more above. */
export function placeBelowOrAbove(rect: TriggerRect, viewport: ViewportBounds, width: number): PopoverPlacement {
    const left = clampLeftWithinViewport(rect.left, width, viewport);
    const spaceBelow = viewport.bottom - rect.bottom - VIEWPORT_MARGIN_PX;
    const spaceAbove = rect.top - viewport.top - VIEWPORT_MARGIN_PX;
    const above = spaceBelow < PREFERRED_MENU_HEIGHT_PX && spaceAbove > spaceBelow;
    const maxHeight = Math.max(MIN_MENU_HEIGHT_PX, Math.floor((above ? spaceAbove : spaceBelow) - PANEL_OFFSET_PX));
    return above
        ? { bottom: Math.round(viewport.bottom - rect.top + PANEL_OFFSET_PX), left, maxHeight }
        : { top: Math.round(rect.bottom + PANEL_OFFSET_PX), left, maxHeight };
}

/** The viewport top of a box of `height` starting at `top`, shifted as little as needed to stay inside the viewport. */
export function alignWithinViewport(top: number, height: number, viewport: ViewportBounds): number {
    const min = viewport.top + VIEWPORT_MARGIN_PX;
    const max = Math.max(min, viewport.bottom - VIEWPORT_MARGIN_PX - height);
    return Math.round(Math.min(max, Math.max(min, top)));
}

/** The viewport top of a box of `height` centred on `centre`, shifted as little as needed to stay inside the viewport. */
export function centreWithinViewport(centre: number, height: number, viewport: ViewportBounds): number {
    return alignWithinViewport(centre - height / 2, height, viewport);
}

/**
 * Opens beside the trigger, or beside `anchor` when given (e.g. the panel holding the trigger), to the right unless
 * only the left has room, vertically centred on the trigger and as tall as the viewport allows.
 */
export function placeBeside(
    rect: TriggerRect,
    viewport: ViewportBounds,
    panel: { width: number; height: number },
    anchor?: TriggerRect,
): PopoverPlacement {
    const maxHeight = Math.max(MIN_MENU_HEIGHT_PX, viewport.bottom - viewport.top - 2 * VIEWPORT_MARGIN_PX);
    const height = Math.min(panel.height, maxHeight);
    const top = centreWithinViewport((rect.top + rect.bottom) / 2, height, viewport);
    const side = anchor ?? rect;
    const gap = anchor ? ANCHOR_GAP_PX : PANEL_OFFSET_PX;
    const spaceRight = viewport.right - VIEWPORT_MARGIN_PX - side.right - gap;
    const spaceLeft = side.left - gap - VIEWPORT_MARGIN_PX - viewport.left;
    const right = spaceRight >= panel.width || spaceRight >= spaceLeft;
    const left = right ? side.right + gap : side.left - gap - panel.width;
    return { top, left: clampLeftWithinViewport(left, panel.width, viewport), maxHeight };
}
