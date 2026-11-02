import {
    FetchStatus,
    Filters,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageNode,
    NodeContext,
    createLineageFilterNodeId,
} from '@app/lineageV3/common';
import getDisplayedNodes from '@app/lineageV3/useComputeGraph/getDisplayedNodes';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { EntityType, LineageDirection } from '@types';

type Context = Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges' | 'rootType'>;

interface NodeOpts {
    type?: EntityType;
    entity?: any;
    inCycle?: boolean;
    expanded?: boolean;
    /** Direction of the node relative to the root; drives `getParents` traversal. */
    direction?: LineageDirection;
    downstream?: Partial<Filters>;
    upstream?: Partial<Filters>;
}

function createNode(urn: string, opts: NodeOpts = {}): LineageEntity {
    const { type = EntityType.Dataset, entity, inCycle, expanded = true, direction, downstream, upstream } = opts;
    return {
        id: urn,
        urn,
        type,
        entity,
        inCycle,
        direction,
        isExpanded: { [LineageDirection.Upstream]: expanded, [LineageDirection.Downstream]: expanded },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map(), ...upstream },
            [LineageDirection.Downstream]: { facetFilters: new Map(), ...downstream },
        },
    };
}

const emptyAdjacencyList = () => ({
    [LineageDirection.Upstream]: new Map<string, Set<string>>(),
    [LineageDirection.Downstream]: new Map<string, Set<string>>(),
});

/** Links `parent -> child` in the downstream direction (and the reverse upstream). */
function addEdge(adjacencyList: NodeContext['adjacencyList'], parent: string, child: string): void {
    if (!adjacencyList[LineageDirection.Downstream].has(parent)) {
        adjacencyList[LineageDirection.Downstream].set(parent, new Set());
    }
    adjacencyList[LineageDirection.Downstream].get(parent)?.add(child);
    if (!adjacencyList[LineageDirection.Upstream].has(child)) {
        adjacencyList[LineageDirection.Upstream].set(child, new Set());
    }
    adjacencyList[LineageDirection.Upstream].get(child)?.add(parent);
}

/** Builds a context from a node list and a set of downstream edges `[parent, child]`. */
function buildContext(nodeList: LineageEntity[], downstreamEdges: [string, string][] = []): Context {
    const nodes = new Map(nodeList.map((n) => [n.urn, n]));
    const adjacencyList = emptyAdjacencyList();
    downstreamEdges.forEach(([parent, child]) => addEdge(adjacencyList, parent, child));
    return { nodes, edges: new Map(), rootType: EntityType.Dataset, adjacencyList };
}

/** BFS-ordered node lists per direction, as `orderNodes` would produce. */
function ordered(
    context: Context,
    downstreamUrns: string[],
    upstreamUrns: string[] = [],
): Record<LineageDirection, LineageEntity[]> {
    const lookup = (urn: string) => context.nodes.get(urn) as LineageEntity;
    return {
        [LineageDirection.Downstream]: downstreamUrns.map(lookup),
        [LineageDirection.Upstream]: upstreamUrns.map(lookup),
    };
}

const childUrns = (displayed: LineageNode[], all: string[]) =>
    displayed.filter((n) => all.includes(n.id)).map((n) => n.id);

describe('getDisplayedNodes', () => {
    describe('pagination and filter nodes', () => {
        const ROOT = 'root';
        // More than LINEAGE_FILTER_PAGINATION (4), so a lineage filter is created for the root's children
        const CHILDREN = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5'];
        const LIMIT = 2;
        const filterId = createLineageFilterNodeId(ROOT, LineageDirection.Downstream);

        function build(): Context {
            const nodes = [
                createNode(ROOT, { downstream: { limit: LIMIT } }),
                ...CHILDREN.map((urn) => createNode(urn)),
            ];
            return buildContext(
                nodes,
                CHILDREN.map((urn) => [ROOT, urn]),
            );
        }

        it('keeps the first `limit` children and records full pagination state', () => {
            const context = build();
            const { displayedNodes, lineageFilters } = getDisplayedNodes(ROOT, ordered(context, CHILDREN), context);

            expect(childUrns(displayedNodes, CHILDREN)).toEqual(['c0', 'c1']);

            const filter = lineageFilters.get(filterId);
            expect(filter?.contents).toEqual(CHILDREN);
            expect(filter?.shown).toEqual(new Set(['c0', 'c1']));
        });

        it('renders a filter node by default', () => {
            const context = build();
            const { displayedNodes } = getDisplayedNodes(ROOT, ordered(context, CHILDREN), context);
            expect(displayedNodes.some((n) => n.type === LINEAGE_FILTER_TYPE)).toBe(true);
        });

        it('omits the filter node but still returns its state when createFilterNodes is false', () => {
            const context = build();
            const { displayedNodes, lineageFilters } = getDisplayedNodes(ROOT, ordered(context, CHILDREN), context, {
                createFilterNodes: false,
            });

            expect(displayedNodes.some((n) => n.type === LINEAGE_FILTER_TYPE)).toBe(false);
            expect(childUrns(displayedNodes, CHILDREN)).toEqual(['c0', 'c1']);
            expect(lineageFilters.get(filterId)?.shown).toEqual(new Set(['c0', 'c1']));
        });
    });

    describe('search filtering', () => {
        const CHILDREN = ['c0', 'c1', 'c2'];

        it('shows only children whose urn is in searchUrns', () => {
            const context = buildContext(
                [
                    createNode('root', { downstream: { searchUrns: new Set(['c1']) } }),
                    ...CHILDREN.map((u) => createNode(u)),
                ],
                CHILDREN.map((u) => ['root', u] as [string, string]),
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, CHILDREN), context);
            expect(childUrns(displayedNodes, CHILDREN)).toEqual(['c1']);
        });

        it('matches a schema field child via its parent urn in searchUrns', () => {
            // Search results reference the parent dataset, not the schema field itself
            const field = createNode('field', {
                type: EntityType.SchemaField,
                entity: { parent: { urn: 'parentDataset' } },
            });
            const context = buildContext(
                [createNode('root', { downstream: { searchUrns: new Set(['parentDataset']) } }), field],
                [['root', 'field']],
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['field']), context);
            expect(displayedNodes.some((n) => n.id === 'field')).toBe(true);
        });
    });

    describe('facet filtering', () => {
        it('filters children by platform', () => {
            const mysql = 'urn:li:dataPlatform:mysql';
            const snowflake = 'urn:li:dataPlatform:snowflake';
            const context = buildContext(
                [
                    createNode('root', {
                        downstream: { facetFilters: new Map([[PLATFORM_FILTER_NAME, new Set([mysql])]]) },
                    }),
                    createNode('c0', { entity: { platform: { urn: mysql } } }),
                    createNode('c1', { entity: { platform: { urn: snowflake } } }),
                ],
                [
                    ['root', 'c0'],
                    ['root', 'c1'],
                ],
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['c0', 'c1']), context);
            expect(childUrns(displayedNodes, ['c0', 'c1'])).toEqual(['c0']);
        });

        it('filters children by entity subtype', () => {
            const facetValue = `${EntityType.Dataset}${FILTER_DELIMITER}View`;
            const context = buildContext(
                [
                    createNode('root', {
                        downstream: { facetFilters: new Map([[ENTITY_SUB_TYPE_FILTER_NAME, new Set([facetValue])]]) },
                    }),
                    createNode('c0', { entity: { subtype: 'View' } }),
                    createNode('c1', { entity: { subtype: 'Table' } }),
                ],
                [
                    ['root', 'c0'],
                    ['root', 'c1'],
                ],
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['c0', 'c1']), context);
            expect(childUrns(displayedNodes, ['c0', 'c1'])).toEqual(['c0']);
        });
    });

    describe('reachability', () => {
        it('only displays nodes reachable from the root urn', () => {
            // Two disconnected components; only root -> reachable should be traversed
            const context = buildContext(
                [createNode('root'), createNode('reachable'), createNode('island0'), createNode('island1')],
                [
                    ['root', 'reachable'],
                    ['island0', 'island1'],
                ],
            );

            // island nodes are ordered but unreachable from root
            const { displayedNodes } = getDisplayedNodes(
                'root',
                ordered(context, ['reachable', 'island0', 'island1']),
                context,
            );

            expect(displayedNodes.map((n) => n.id).sort()).toEqual(['reachable', 'root']);
        });

        it('does not traverse past a node whose filters set display to false', () => {
            const context = buildContext(
                [createNode('root'), createNode('a', { downstream: { display: false } }), createNode('b')],
                [
                    ['root', 'a'],
                    ['a', 'b'],
                ],
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['a', 'b']), context);
            expect(displayedNodes.map((n) => n.id).sort()).toEqual(['a', 'root']);
        });
    });

    describe('ordering', () => {
        it('returns children in the priority order given by orderedNodes', () => {
            const context = buildContext(
                [createNode('root'), createNode('c0'), createNode('c1'), createNode('c2')],
                // Adjacency insertion order differs from the intended priority order
                [
                    ['root', 'c2'],
                    ['root', 'c0'],
                    ['root', 'c1'],
                ],
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['c0', 'c1', 'c2']), context);
            expect(childUrns(displayedNodes, ['c0', 'c1', 'c2'])).toEqual(['c0', 'c1', 'c2']);
        });

        it('returns the root first, then upstream nodes, then downstream nodes', () => {
            const context = buildContext(
                [createNode('root'), createNode('up'), createNode('down')],
                [
                    ['root', 'down'],
                    ['up', 'root'],
                ],
            );

            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['down'], ['up']), context);
            expect(displayedNodes.map((n) => n.id)).toEqual(['root', 'up', 'down']);
        });
    });

    describe('transformational nodes', () => {
        // root -> t (DataJob, transformational) -> c0, c1
        function build(rootOpts: NodeOpts = {}): Context {
            return buildContext(
                [
                    createNode('root', rootOpts),
                    createNode('t', { type: EntityType.DataJob, direction: LineageDirection.Downstream }),
                    createNode('c0', { direction: LineageDirection.Downstream }),
                    createNode('c1', { direction: LineageDirection.Downstream }),
                ],
                [
                    ['root', 't'],
                    ['t', 'c0'],
                    ['t', 'c1'],
                ],
            );
        }

        it('filters the non-transformational children, then adds the transformational node back', () => {
            const context = build();
            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['c0', 'c1']), context);

            // Transformational `t` is not itself filterable, but is re-inserted on the path to its leaves
            expect(displayedNodes.map((n) => n.id)).toEqual(['root', 't', 'c0', 'c1']);
        });

        it('keeps the transformational node when at least one leaf survives filtering', () => {
            const context = build({ downstream: { searchUrns: new Set(['c0']) } });
            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['c0', 'c1']), context);

            expect(displayedNodes.map((n) => n.id)).toEqual(['root', 't', 'c0']);
        });

        it('drops the transformational node when all of its leaves are filtered out', () => {
            const context = build({ downstream: { searchUrns: new Set(['no-match']) } });
            const { displayedNodes } = getDisplayedNodes('root', ordered(context, ['c0', 'c1']), context);

            expect(displayedNodes.map((n) => n.id)).toEqual(['root']);
        });

        it('records the transformational leaves as parents of the shown children', () => {
            const context = build();
            const { parents } = getDisplayedNodes('root', ordered(context, ['c0', 'c1']), context);

            expect(parents.get('c0')).toEqual(new Set(['root']));
            expect(parents.get('c1')).toEqual(new Set(['root']));
        });
    });
});
