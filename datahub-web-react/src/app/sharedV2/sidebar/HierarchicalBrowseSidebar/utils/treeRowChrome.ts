/**
 * Pure chrome flags for hierarchical browse tree rows.
 *
 * Kept free of React so count / expand visibility can be unit-tested without
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
    canExpand: boolean;
    /** Mount count when collapsed; `countReveal` on the row controls always vs hover. */
    showCount: boolean;
};

export function getTreeRowChromeFlags({
    isCollapsed,
    hasChildren,
    isExpanded,
    count,
    hasToggle,
}: TreeRowChromeInput): TreeRowChromeFlags {
    return {
        canExpand: !isCollapsed && hasChildren && hasToggle,
        showCount: !isCollapsed && hasChildren && !isExpanded && count != null && count > 0,
    };
}

/** Leading padding for a tree row / section header at the given depth. */
export function getTreeRowPaddingLeft(level: number): number {
    return 8 + Math.max(0, level) * 16;
}
