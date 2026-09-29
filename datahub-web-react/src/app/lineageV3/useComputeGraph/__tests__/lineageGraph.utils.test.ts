import {
    FetchStatus,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    NodeContext,
    addToAdjacencyList,
} from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { hideOrphanedNodes, isLineageKnown } from '@app/lineageV3/useComputeGraph/lineageGraph.utils';

import { EntityType, LineageDirection } from '@types';

const DP = 'urn:li:dataProduct:DP';
const A = 'urn:li:dataset:A';
const B = 'urn:li:dataset:B';
const C = 'urn:li:dataset:C';
const FILTER = 'lineage-filter:A';

function entityNode(urn: string, fetched: boolean): LineageEntity {
    return {
        id: urn,
        urn,
        type: EntityType.Dataset,
        entity: fetched ? ({ urn, type: EntityType.Dataset, exists: true } as FetchedEntityV2) : undefined,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}

function setUp(fetched: Record<string, boolean>, edges: [string, string][]) {
    const nodes: NodeContext['nodes'] = new Map(
        Object.entries(fetched).map(([urn, isFetched]) => [urn, entityNode(urn, isFetched)]),
    );
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    edges.forEach(([upstream, downstream]) =>
        addToAdjacencyList(adjacencyList, LineageDirection.Downstream, upstream, downstream),
    );
    return { nodes, adjacencyList };
}

const ids = (shown: { id: string }[]) => shown.map((node) => node.id);
const pick = (nodes: NodeContext['nodes'], urns: string[]): LineageEntity[] =>
    urns.map((urn) => nodes.get(urn)).filter((node): node is LineageEntity => !!node);

describe('hideOrphanedNodes', () => {
    it('hides nothing while no home member has lineage, so the home box stays visible', () => {
        const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [C]: true }, []);

        const result = hideOrphanedNodes(pick(nodes, [DP, A, C]), DP, new Set([A, C]), nodes, adjacencyList);

        expect(ids(result)).toEqual([DP, A, C]);
    });

    it('hides fetched orphans once a home member has lineage, even if that member is not fetched yet', () => {
        const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: false, [B]: true, [C]: true }, [[B, A]]);

        const result = hideOrphanedNodes(pick(nodes, [DP, A, C]), DP, new Set([A, C]), nodes, adjacencyList);

        expect(ids(result)).toEqual([DP, A]);
    });

    it('keeps a fetched member while its lineage is being updated, e.g. a manual edit swapping edges', () => {
        const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [B]: true, [C]: true }, [[A, B]]);
        const editing = nodes.get(C);
        if (editing) editing.fetchStatus[LineageDirection.Upstream] = FetchStatus.LOADING;

        const result = hideOrphanedNodes(pick(nodes, [DP, A, B, C]), DP, new Set([A, B, C]), nodes, adjacencyList);

        expect(ids(result)).toEqual([DP, A, B, C]);
    });

    it('always keeps the root and filter nodes', () => {
        const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [B]: true }, [[A, B]]);
        const filterNode = {
            id: FILTER,
            type: LINEAGE_FILTER_TYPE,
            direction: LineageDirection.Downstream,
            parent: A,
        } as LineageFilter;

        const result = hideOrphanedNodes(
            [...pick(nodes, [DP, A, B]), filterNode],
            DP,
            new Set([A, B]),
            nodes,
            adjacencyList,
        );

        expect(ids(result)).toEqual([DP, A, B, FILTER]);
    });
});

describe('isLineageKnown', () => {
    it('is true once the node is fetched and no lineage update is in flight', () => {
        expect(isLineageKnown(entityNode(A, true))).toBe(true);
    });

    it('is false for a missing or unfetched node', () => {
        expect(isLineageKnown(undefined)).toBe(false);
        expect(isLineageKnown(entityNode(A, false))).toBe(false);
    });

    it('is false while lineage in either direction is loading', () => {
        const upstreamLoading = entityNode(A, true);
        upstreamLoading.fetchStatus[LineageDirection.Upstream] = FetchStatus.LOADING;
        const downstreamLoading = entityNode(A, true);
        downstreamLoading.fetchStatus[LineageDirection.Downstream] = FetchStatus.LOADING;

        expect(isLineageKnown(upstreamLoading)).toBe(false);
        expect(isLineageKnown(downstreamLoading)).toBe(false);
    });
});
