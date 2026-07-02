import { hasLazyIcon } from '@app/mfeframework/lazyIconNames';

// Default minimum cell width, in pixels. The grid packs as many columns as fit into the
// container at this width; the actual cell width is then stretched to fill the row so the
// grid always aligns to the container edges.
export const DEFAULT_MIN_CELL_SIZE = 42;

/**
 * Builds the list of icon names the picker should render, keeping any customer-pinned
 * icons visible even when they live outside the curated library.
 *
 * A pinned icon is preserved only when:
 *   1. It resolves to a real Phosphor icon (`hasLazyIcon`). This filters out empty strings,
 *      unknown names, or stored MUI names that have no mapping — we don't want cells that
 *      would render as the `AppWindow` fallback.
 *   2. It isn't already in the curated set (`curatedIcons` lookup). Without this a pinned
 *      icon that's coincidentally in the curated 163 would appear twice.
 *
 * When both conditions hold, the pinned icon is prepended so the user's current selection
 * is the first cell they see. Otherwise the curated library is returned unchanged.
 */
export function buildEffectiveDomainIconList(
    library: readonly string[],
    curatedIcons: Readonly<Record<string, unknown>>,
    pinnedIcons?: readonly string[],
): readonly string[] {
    if (!pinnedIcons?.length) return library;
    const extras = pinnedIcons.filter((n) => hasLazyIcon(n) && !curatedIcons[n]);
    if (!extras.length) return library;
    return [...extras, ...library];
}

/**
 * Case-insensitive substring search. Trims the search term so trailing spaces from
 * an IME/paste don't hide otherwise-matching icons. An empty (or whitespace-only)
 * term returns the input list unchanged.
 */
export function filterDomainIconsBySearch(icons: readonly string[], searchTerm: string): readonly string[] {
    const term = searchTerm.trim().toLowerCase();
    if (!term) return icons;
    return icons.filter((name) => name.toLowerCase().includes(term));
}

export type IconGridLayout = {
    columnCount: number;
    cellSize: number;
};

/**
 * Computes the column count and stretched cell size for the icon grid.
 *
 * The grid packs as many columns of at least `minCellSize` as fit into `containerWidth`,
 * then stretches each cell to fill the row exactly — this keeps the right edge flush and
 * avoids a jittery scrollbar when the modal resizes mid-drag. A zero-width container
 * (pre-first-measurement) falls back to a single column of `minCellSize` so the grid
 * renders something coherent while `ResizeObserver` is still warming up.
 */
export function computeIconGridLayout(
    containerWidth: number,
    minCellSize: number = DEFAULT_MIN_CELL_SIZE,
): IconGridLayout {
    const columnCount = Math.max(1, Math.floor(containerWidth / minCellSize));
    const cellSize = containerWidth > 0 ? containerWidth / columnCount : minCellSize;
    return { columnCount, cellSize };
}
