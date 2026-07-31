/**
 * Pure chrome flags for hierarchical browse tree rows.
 *
 * Kept free of React so count / caret visibility can be unit-tested without
 * mounting the row (same idea as documentTreeGrouping / fileImportUtils).
 */

export type TreeRowChromeInput = {
    isCollapsed: boolean;
    hasChildren: boolean;
    isExpanded: boolean;
    /** Child count for the collapsed-row pill; undefined / 0 hides the pill. */
    count?: number;
    hasToggle: boolean;
};

export type TreeRowChromeFlags = {
    /** Show expand caret (parents only; hidden when sidebar is collapsed). */
    canExpand: boolean;
    /** Count pill — only while children are collapsed. */
    showCount: boolean;
    /** Right column: count, trailing actions, and/or reserved caret slot. */
    showRightChrome: boolean;
    /** Reserve far-right caret column so leaf/parent icons stay aligned. */
    reserveCaretSlot: boolean;
};

/**
 * Derives which chrome pieces HierarchicalBrowseTreeRow should render.
 *
 * Count hides when expanded (children are visible). Caret column is reserved
 * for every expanded-sidebar row so leaves and parents share one vertical line.
 */
export function getTreeRowChromeFlags({
    isCollapsed,
    hasChildren,
    isExpanded,
    count,
    hasToggle,
}: TreeRowChromeInput): TreeRowChromeFlags {
    const canExpand = !isCollapsed && hasChildren && hasToggle;
    const showCount = !isCollapsed && hasChildren && !isExpanded && count != null && count > 0;
    const reserveCaretSlot = !isCollapsed;
    // Always mount the right column when expanded so the caret slot keeps
    // leaf and parent icons on one vertical line.
    const showRightChrome = reserveCaretSlot;

    return {
        canExpand,
        showCount,
        showRightChrome,
        reserveCaretSlot,
    };
}

/** Leading padding for a tree row / section header at the given depth. */
export function getTreeRowPaddingLeft(level: number): number {
    return 8 + Math.max(0, level) * 16;
}
