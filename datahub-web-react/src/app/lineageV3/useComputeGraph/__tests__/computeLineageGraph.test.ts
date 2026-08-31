import {
    FetchStatus,
    GraphStoreFields,
    LINEAGE_FILTER_TYPE,
    LineageEdge,
    LineageEntity,
    LineageToggles,
    NodeContext,
    createLineageFilterNodeId,
} from '@app/lineageV3/common';
import computeLineageGraph from '@app/lineageV3/useComputeGraph/computeLineageGraph';

import { EntityType, LineageDirection } from '@types';

type GraphContext = Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>;

function createNode(urn: string, type: EntityType = EntityType.Dataset, limit?: number): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map(), limit },
        },
    };
}

const createEdge = (isDisplayed = true): LineageEdge => ({ isDisplayed, isManual: false });

const TOGGLES: Pick<NodeContext, LineageToggles> = {
    hideTransformations: false,
    showDataProcessInstances: false,
    showGhostEntities: false,
};

/**
 * Builds a simple graph: `upstream -> root -> downstream`.
 */
function buildLinearContext(overrides: Partial<Pick<NodeContext, LineageToggles>> = {}): GraphContext {
    const nodes = new Map<string, LineageEntity>([
        ['root', createNode('root')],
        ['upstream', createNode('upstream')],
        ['downstream', createNode('downstream')],
    ]);
    const edges = new Map<string, LineageEdge>([
        ['upstream-:-root', createEdge()],
        ['root-:-downstream', createEdge()],
    ]);
    const adjacencyList = {
        [LineageDirection.Upstream]: new Map([
            ['root', new Set(['upstream'])],
            ['downstream', new Set(['root'])],
        ]),
        [LineageDirection.Downstream]: new Map([
            ['root', new Set(['downstream'])],
            ['upstream', new Set(['root'])],
        ]),
    };
    return { nodes, edges, adjacencyList, rootType: EntityType.Dataset, ...TOGGLES, ...overrides };
}

describe('computeLineageGraph', () => {
    it('returns the root and displays nodes in both directions', () => {
        const { roots, displayedNodes } = computeLineageGraph('root', buildLinearContext(), false);

        expect(roots.map((n) => n.urn)).toEqual(['root']);
        const displayedUrns = displayedNodes.map((n) => n.id).sort();
        expect(displayedUrns).toEqual(['downstream', 'root', 'upstream']);
    });

    it('signs order indices by direction, with the root at zero', () => {
        const { orderIndices } = computeLineageGraph('root', buildLinearContext(), false);

        expect(orderIndices.root).toBe(0);
        expect(orderIndices.downstream).toBeGreaterThan(0);
        expect(orderIndices.upstream).toBeLessThan(0);
    });

    it('returns no roots and no displayed nodes when the root is absent from the graph', () => {
        const { roots, displayedNodes } = computeLineageGraph('missing', buildLinearContext(), false);

        expect(roots).toEqual([]);
        expect(displayedNodes).toEqual([]);
    });

    it('removes nodes matched by nodeFilter before display', () => {
        const { displayedNodes, newGraphStore } = computeLineageGraph('root', buildLinearContext(), false, {
            nodeFilter: (node) => node.urn !== 'downstream',
        });

        expect(newGraphStore.nodes.has('downstream')).toBe(false);
        expect(displayedNodes.some((n) => n.id === 'downstream')).toBe(false);
        expect(displayedNodes.some((n) => n.id === 'upstream')).toBe(true);
    });

    it('applies transformDisplayedNodes to the returned nodes', () => {
        const { displayedNodes } = computeLineageGraph('root', buildLinearContext(), false, {
            transformDisplayedNodes: (displayed) => displayed.filter((n) => n.id === 'root'),
        });

        expect(displayedNodes.map((n) => n.id)).toEqual(['root']);
    });

    it('hides transformational nodes from the graph store when hideTransformations is set', () => {
        const nodes = new Map<string, LineageEntity>([
            ['root', createNode('root')],
            ['transform', createNode('transform', EntityType.DataJob)],
            ['downstream', createNode('downstream')],
        ]);
        const edges = new Map<string, LineageEdge>([
            ['root-:-transform', createEdge()],
            ['transform-:-downstream', createEdge()],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map([
                ['transform', new Set(['root'])],
                ['downstream', new Set(['transform'])],
            ]),
            [LineageDirection.Downstream]: new Map([
                ['root', new Set(['transform'])],
                ['transform', new Set(['downstream'])],
            ]),
        };
        const context: GraphContext = {
            nodes,
            edges,
            adjacencyList,
            rootType: EntityType.Dataset,
            ...TOGGLES,
            hideTransformations: true,
        };

        const { newGraphStore, displayedNodes } = computeLineageGraph('root', context, false);

        expect(newGraphStore.nodes.has('transform')).toBe(false);
        expect(displayedNodes.some((n) => n.id === 'transform')).toBe(false);
        expect(displayedNodes.some((n) => n.id === 'downstream')).toBe(true);
    });

    describe('with more children than the pagination limit', () => {
        const CHILDREN = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5'];

        function buildFanOutContext(): GraphContext {
            const nodes = new Map<string, LineageEntity>([['root', createNode('root')]]);
            CHILDREN.forEach((urn) => nodes.set(urn, createNode(urn)));
            // hideNodes rebuilds the adjacency list from edges, so each child needs a backing edge
            const edges = new Map<string, LineageEdge>(CHILDREN.map((urn) => [`root-:-${urn}`, createEdge()]));
            return {
                nodes,
                edges,
                rootType: EntityType.Dataset,
                ...TOGGLES,
                adjacencyList: {
                    [LineageDirection.Downstream]: new Map([['root', new Set(CHILDREN)]]),
                    [LineageDirection.Upstream]: new Map(CHILDREN.map((urn) => [urn, new Set(['root'])])),
                },
            };
        }

        const filterId = createLineageFilterNodeId('root', LineageDirection.Downstream);

        it('includes a filter node by default and records pagination state', () => {
            const { displayedNodes, lineageFilters } = computeLineageGraph('root', buildFanOutContext(), false);

            expect(displayedNodes.some((n) => n.type === LINEAGE_FILTER_TYPE)).toBe(true);
            expect(lineageFilters.get(filterId)?.contents).toEqual(CHILDREN);
        });

        it('omits the filter node but keeps its state when createFilterNodes is false', () => {
            const { displayedNodes, lineageFilters } = computeLineageGraph('root', buildFanOutContext(), false, {
                createFilterNodes: false,
            });

            expect(displayedNodes.some((n) => n.type === LINEAGE_FILTER_TYPE)).toBe(false);
            expect(lineageFilters.get(filterId)).toBeDefined();
        });
    });
});
