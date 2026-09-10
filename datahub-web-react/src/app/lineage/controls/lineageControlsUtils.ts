type LineageFilterState = {
    hideTransformations: boolean;
    showDataProcessInstances: boolean;
    showGhostEntities: boolean;
};

export function isLineageFilterActive({
    hideTransformations,
    showDataProcessInstances,
    showGhostEntities,
}: LineageFilterState): boolean {
    return hideTransformations || !showDataProcessInstances || showGhostEntities;
}

// Wraps an index around the bounds of a list so that stepping past either end cycles to the other.
// Returns NaN for an empty list, matching modulo-by-zero so callers index into `undefined` as before.
export function getWrappedIndex(index: number, delta: number, length: number): number {
    return (index + delta + length) % length;
}
