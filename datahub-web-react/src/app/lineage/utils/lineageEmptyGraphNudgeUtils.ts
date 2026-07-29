import { LineageDirection } from '@types';

const FETCH_STATUS_COMPLETE = 'COMPLETE';

export type AdjacencyList = Record<LineageDirection, Map<string, Set<string>>>;

export type LineageEmptyGraphNudgeNodes = {
    get(urn: string): { fetchStatus?: Record<LineageDirection, string> } | undefined;
};

type RootFetchStatus = Record<LineageDirection, string> | undefined;

export type LineageEmptyGraphNudgeReason = 'timeFilter' | 'hiddenEdges';

export type LineageEmptyGraphNudgeState = {
    show: boolean;
    reasons: LineageEmptyGraphNudgeReason[];
};

export function isLineageGraphLoaded(fetchStatus: RootFetchStatus): boolean {
    if (!fetchStatus) {
        return false;
    }
    return (
        fetchStatus[LineageDirection.Upstream] === FETCH_STATUS_COMPLETE &&
        fetchStatus[LineageDirection.Downstream] === FETCH_STATUS_COMPLETE
    );
}

export function isLineageGraphEmpty(rootUrn: string, adjacencyList: AdjacencyList): boolean {
    const upstreamNeighbors = adjacencyList[LineageDirection.Upstream].get(rootUrn);
    const downstreamNeighbors = adjacencyList[LineageDirection.Downstream].get(rootUrn);
    return (upstreamNeighbors?.size ?? 0) === 0 && (downstreamNeighbors?.size ?? 0) === 0;
}

export function getLineageCountTotal(
    upstreamTotal: number | undefined | null,
    downstreamTotal: number | undefined | null,
): number {
    return (upstreamTotal ?? 0) + (downstreamTotal ?? 0);
}

export function getLineageEmptyGraphNudgeState({
    isLoaded,
    isEmpty,
    isTimeFilterActive,
    showGhostEntities,
    allTimeTotal,
    filteredTotal,
}: {
    isLoaded: boolean;
    isEmpty: boolean;
    isTimeFilterActive: boolean;
    showGhostEntities: boolean;
    allTimeTotal: number;
    filteredTotal: number;
}): LineageEmptyGraphNudgeState {
    if (!isLoaded || !isEmpty || allTimeTotal <= 0) {
        return { show: false, reasons: [] };
    }

    const reasons: LineageEmptyGraphNudgeReason[] = [];

    if (isTimeFilterActive && filteredTotal < allTimeTotal) {
        reasons.push('timeFilter');
    }

    if (!showGhostEntities && filteredTotal === allTimeTotal) {
        reasons.push('hiddenEdges');
    }

    if (!reasons.length && isTimeFilterActive) {
        reasons.push('timeFilter');
    }

    return {
        show: reasons.length > 0,
        reasons,
    };
}
