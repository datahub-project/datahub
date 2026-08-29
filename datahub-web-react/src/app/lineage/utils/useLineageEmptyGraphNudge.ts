import { useMemo } from 'react';

import {
    AdjacencyList,
    LineageEmptyGraphNudgeNodes,
    LineageEmptyGraphNudgeState,
    getLineageCountTotal,
    getLineageEmptyGraphNudgeState,
    isLineageGraphEmpty,
    isLineageGraphLoaded,
} from '@app/lineage/utils/lineageEmptyGraphNudgeUtils';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';

import { useGetSearchAcrossLineageCountsQuery } from '@graphql/lineage.generated';

type UseLineageEmptyGraphNudgeParams = {
    rootUrn: string;
    adjacencyList: AdjacencyList;
    nodes: LineageEmptyGraphNudgeNodes;
    showGhostEntities: boolean;
};

export default function useLineageEmptyGraphNudge({
    rootUrn,
    adjacencyList,
    nodes,
    showGhostEntities,
}: UseLineageEmptyGraphNudgeParams): LineageEmptyGraphNudgeState & { loading: boolean } {
    const { startTimeMillis, endTimeMillis } = useGetLineageTimeParams();

    const rootFetchStatus = nodes.get(rootUrn)?.fetchStatus;
    const isLoaded = isLineageGraphLoaded(rootFetchStatus);
    const isEmpty = isLineageGraphEmpty(rootUrn, adjacencyList);
    const shouldFetchCounts = isLoaded && isEmpty;
    const isTimeFilterActive = !!(startTimeMillis || endTimeMillis);

    const { data: allTimeCounts, loading: allTimeLoading } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn: rootUrn },
        skip: !shouldFetchCounts,
        fetchPolicy: 'cache-first',
    });

    const { data: filteredCounts, loading: filteredLoading } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn: rootUrn, startTimeMillis, endTimeMillis },
        skip: !shouldFetchCounts,
        fetchPolicy: 'cache-first',
    });

    const allTimeTotal = getLineageCountTotal(allTimeCounts?.upstreams?.total, allTimeCounts?.downstreams?.total);
    const filteredTotal = getLineageCountTotal(filteredCounts?.upstreams?.total, filteredCounts?.downstreams?.total);

    const nudgeState = useMemo(
        () =>
            getLineageEmptyGraphNudgeState({
                isLoaded,
                isEmpty,
                isTimeFilterActive,
                showGhostEntities,
                allTimeTotal,
                filteredTotal,
            }),
        [allTimeTotal, filteredTotal, isEmpty, isLoaded, isTimeFilterActive, showGhostEntities],
    );

    return {
        ...nudgeState,
        loading: shouldFetchCounts && (allTimeLoading || filteredLoading),
    };
}
