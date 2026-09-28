/**
 * Pure helpers for HierarchicalBrowseSidebar collapsed-body / home placement.
 */

export type CollapsedBodyMode = 'icons' | 'searchOnly';

/** Icon rail when the page supplies icons; otherwise search-only expand control. */
export function resolveCollapsedBodyMode(hasCollapsedIcons: boolean): CollapsedBodyMode {
    return hasCollapsedIcons ? 'icons' : 'searchOnly';
}

/**
 * Home sits above the tree divider when there are no filters; with filters it
 * moves into the tree band under the divider.
 */
export function shouldPlaceHomeAboveDivider(options: { showFilters: boolean; hasHomeNav: boolean }): boolean {
    return !options.showFilters && options.hasHomeNav;
}
