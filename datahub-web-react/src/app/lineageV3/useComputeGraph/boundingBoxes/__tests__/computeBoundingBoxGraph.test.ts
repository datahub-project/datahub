import { FetchStatus, LineageEntity, NodeContext, addToAdjacencyList, createEdgeId } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import computeBoundingBoxGraph from '@app/lineageV3/useComputeGraph/boundingBoxes/computeBoundingBoxGraph';

import { EntityType, LineageDirection } from '@types';

const DP = 'urn:li:dataProduct:DP';
const A = 'urn:li:dataset:A';
const B = 'urn:li:dataset:B';
const C = 'urn:li:dataset:C';
const D = 'urn:li:dataset:D';
const N = 'urn:li:dataset:N';

function node(urn: string, type: EntityType, fetched: boolean, isMember: boolean): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        entity: fetched ? ({ urn, type, exists: true } as FetchedEntityV2) : undefined,
        boundingBoxes: isMember ? [{ urn: DP, isOutputPort: false }] : undefined,
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

type Options = { collapsed?: string[]; unresolvedNeighbors?: string[] };

/**
 * Returns the members rendered for the home box DP; `fetched` lists members whose bulk lineage fetch
 * has completed. `unresolvedNeighbors` are non-member nodes whose membership hasn't been fetched yet.
 */
function renderedUrns(
    members: string[],
    fetched: string[],
    lineage: [string, string][],
    { collapsed = [], unresolvedNeighbors = [] }: Options = {},
): string[] {
    const nodes: NodeContext['nodes'] = new Map([
        [DP, node(DP, EntityType.DataProduct, true, false)],
        ...members.map((urn): [string, LineageEntity] => [
            urn,
            node(urn, EntityType.Dataset, fetched.includes(urn), true),
        ]),
        ...unresolvedNeighbors.map((urn): [string, LineageEntity] => [
            urn,
            node(urn, EntityType.Dataset, false, false),
        ]),
    ]);
    collapsed.forEach((urn) => {
        const member = nodes.get(urn);
        if (member) member.isExpanded = { [LineageDirection.Upstream]: false, [LineageDirection.Downstream]: false };
    });
    const edges: NodeContext['edges'] = new Map();
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    lineage.forEach(([upstream, downstream]) => {
        edges.set(createEdgeId(upstream, downstream), { isDisplayed: true });
        addToAdjacencyList(adjacencyList, LineageDirection.Downstream, upstream, downstream);
    });

    const { flowNodes } = computeBoundingBoxGraph(
        DP,
        {
            nodes,
            edges,
            adjacencyList,
            rootType: EntityType.DataProduct,
            hideTransformations: false,
            showDataProcessInstances: false,
            showGhostEntities: false,
            outputPortsOnly: false,
            boundingBoxEntities: new Map(),
        },
        false,
    );
    return flowNodes.map((flowNode) => (flowNode.data as LineageEntity).urn).filter((urn) => urn !== DP);
}

describe('computeBoundingBoxGraph orphaned members', () => {
    it('hides fetched members with no lineage', () => {
        expect(renderedUrns([A, B, C], [A, B, C], [[A, B]]).sort()).toEqual([A, B]);
    });

    it('keeps members whose lineage has not been fetched yet', () => {
        expect(renderedUrns([A, B, C], [A, B], [[A, B]]).sort()).toEqual([A, B, C]);
    });

    it('does not hide orphans while the rest of the page is loading and no member is connected yet', () => {
        expect(renderedUrns([A, C], [C], []).sort()).toEqual([A, C]);
    });

    it('keeps connected members whose edges are hidden by collapsing', () => {
        expect(
            renderedUrns(
                [A, B, C, D],
                [A, B, C, D],
                [
                    [A, B],
                    [C, D],
                ],
                { collapsed: [A, B] },
            ).sort(),
        ).toEqual([A, B, C, D]);
    });

    it('keeps members whose only lineage is to neighbors with unresolved membership', () => {
        expect(
            renderedUrns(
                [A, B, C],
                [A, B, C],
                [
                    [A, B],
                    [C, N],
                ],
                { unresolvedNeighbors: [N] },
            ).sort(),
        ).toEqual([A, B, C]);
    });

    it('shows all members when none of them has lineage, so the home box stays visible', () => {
        expect(renderedUrns([A, B, C], [A, B, C], []).sort()).toEqual([A, B, C]);
    });
});
