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
