import { TENTATIVE_EDGE_NAME } from '@app/lineageV3/LineageEdge/TentativeEdge';
import {
    ColumnRef,
    ENTITY_LEVEL_FIELD,
    FineGrainedLineage,
    NodeContext,
    createColumnRef,
    createEdgeId,
    createEntityRef,
    createLineageFilterNodeId,
    resolveColumnHighlightSource,
    setDefault,
} from '@app/lineageV3/common';
import { computeSingleColumnHighlights, getEntityRef } from '@app/lineageV3/useColumnHighlighting';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.utils';

import { EntityType, LineageDirection } from '@types';

const UPSTREAM = 'urn:li:dataset:upstream';
const DOWNSTREAM = 'urn:li:dataset:downstream';
const DOWNSTREAM_OF_DOWNSTREAM = 'urn:li:dataset:downstreamOfDownstream';
const DP_A = 'urn:li:dataProduct:A';
const DP_B = 'urn:li:dataProduct:B';
const FIELD = 'id';

const upstreamRef = createColumnRef(UPSTREAM, FIELD);
const downstreamRef = createColumnRef(DOWNSTREAM, FIELD);

interface AssetOverrides {
    numUpstream?: number;
    numDownstream?: number;
    lineageCountsFetched?: boolean;
}

function node(urn: string, direction?: LineageDirection, asset?: AssetOverrides) {
    // Counts default to fetched, so a node without overrides emits no tentative edges
    const lineageAssets = asset && new Map([[FIELD, { name: FIELD, lineageCountsFetched: true, ...asset }]]);
    return { id: urn, urn, type: EntityType.Dataset, direction, entity: { lineageAssets } } as any;
}

/** Builds fine grained lineage from `[upstreamColumn, downstreamColumn]` pairs. */
function lineageFromEdges(edges: [ColumnRef, ColumnRef][]): FineGrainedLineage {
    const lineage: FineGrainedLineage = { downstream: new Map(), upstream: new Map() };
    edges.forEach(([from, to]) => {
        setDefault(lineage.downstream, from, new Map()).set(to, null);
        setDefault(lineage.upstream, to, new Map()).set(from, null);
    });
    return lineage;
}

function fineGrainedLineage(): FineGrainedLineage {
    return lineageFromEdges([[upstreamRef, downstreamRef]]);
}

interface Overrides {
    fineGrainedLineage?: FineGrainedLineage;
    nodes?: NodeContext['nodes'];
    displayedNodeIds?: Set<string>;
    showFilterNodes?: boolean;
    /** The hovered or selected ref; the upstream column by default. */
    source?: ColumnRef;
    adjacencyList?: NodeContext['adjacencyList'];
}

/** Builds an entity-level adjacency list from `[upstreamUrn, downstreamUrn]` pairs. */
function adjacencyFromEdges(edges: [string, string][]): NodeContext['adjacencyList'] {
    const list = { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() };
    edges.forEach(([from, to]) => {
        setDefault(list[LineageDirection.Downstream], from, new Set()).add(to);
        setDefault(list[LineageDirection.Upstream], to, new Set()).add(from);
    });
    return list;
}

function run(nodeIdsByUrn: Map<string, string[]>, overrides: Overrides = {}) {
    return computeSingleColumnHighlights(
        overrides.source ?? upstreamRef,
        {
            fineGrainedLineage: overrides.fineGrainedLineage ?? fineGrainedLineage(),
            nodes:
                overrides.nodes ??
                new Map([
                    [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream)],
                    [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
                ]),
            adjacencyList: overrides.adjacencyList ?? {
                [LineageDirection.Upstream]: new Map(),
                [LineageDirection.Downstream]: new Map(),
            },
            displayedNodeIds: overrides.displayedNodeIds ?? new Set([UPSTREAM, DOWNSTREAM]),
            nodeIdsByUrn,
            rootUrn: DP_A,
            rootType: EntityType.DataProduct,
            showFilterNodes: overrides.showFilterNodes ?? false,
        },
        'red',
    );
}

describe('column edges attach to rendered node ids', () => {
    it('uses urns directly when node ids are urns', () => {
        const { columnEdges } = run(new Map());

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0].source).toEqual(UPSTREAM);
        expect(edges[0].target).toEqual(DOWNSTREAM);
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].targetHandle).toEqual(downstreamRef);
    });

    it('attaches to data-product-qualified member node ids', () => {
        const upstreamId = createMemberNodeId(DP_A, UPSTREAM);
        const downstreamId = createMemberNodeId(DP_A, DOWNSTREAM);
        const { columnEdges } = run(
            new Map([
                [UPSTREAM, [upstreamId]],
                [DOWNSTREAM, [downstreamId]],
            ]),
        );

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0].source).toEqual(upstreamId);
        expect(edges[0].target).toEqual(downstreamId);
        // Handles stay urn-based, as Column.tsx builds them from the entity urn
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].targetHandle).toEqual(downstreamRef);
    });

    it('emits one edge per rendered copy of an entity in multiple data products', () => {
        const downstreamA = createMemberNodeId(DP_A, DOWNSTREAM);
        const downstreamB = createMemberNodeId(DP_B, DOWNSTREAM);
        const upstreamId = createMemberNodeId(DP_A, UPSTREAM);
        const { columnEdges } = run(
            new Map([
                [UPSTREAM, [upstreamId]],
                [DOWNSTREAM, [downstreamA, downstreamB]],
            ]),
        );

        const targets = Array.from(columnEdges.values()).map((edge) => edge.target);
        expect(targets.sort()).toEqual([downstreamA, downstreamB].sort());
    });
});

// Numerator of the column lineage controls, compared against the counts fetched for each column
describe('related columns shown on the graph', () => {
    const QUERY = 'urn:li:query:transform';
    const MISSING = 'urn:li:dataset:missing';
    const OTHER_FIELD = 'name';
    const queryRef = createColumnRef(QUERY, FIELD);
    const missingRef = createColumnRef(MISSING, FIELD);

    it('counts both directions for the column under the cursor', () => {
        const { shownRelatedColumns } = run(new Map());

        expect(shownRelatedColumns.get(upstreamRef)).toEqual({
            [LineageDirection.Downstream]: 1,
            [LineageDirection.Upstream]: 0,
        });
    });

    it('counts related columns only on the side the traversal reached them from', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, downstreamRef],
                [downstreamRef, createColumnRef(DOWNSTREAM_OF_DOWNSTREAM, FIELD)],
            ]),
            nodes: new Map([
                [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream)],
                [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
                [DOWNSTREAM_OF_DOWNSTREAM, node(DOWNSTREAM_OF_DOWNSTREAM, LineageDirection.Upstream)],
            ]),
            displayedNodeIds: new Set([UPSTREAM, DOWNSTREAM, DOWNSTREAM_OF_DOWNSTREAM]),
        });

        // Nothing is known about what is upstream of the downstream column, as we never looked
        expect(shownRelatedColumns.get(downstreamRef)).toEqual({ [LineageDirection.Downstream]: 1 });
    });

    it('leaves out columns on nodes that are not displayed', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, missingRef],
                [missingRef, downstreamRef],
            ]),
        });

        expect(shownRelatedColumns.has(missingRef)).toBe(false);
        expect(shownRelatedColumns.has(downstreamRef)).toBe(true);
    });

    it('counts each related column once, no matter how many paths reach it', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, downstreamRef],
                [upstreamRef, createColumnRef(DOWNSTREAM, OTHER_FIELD)],
                [upstreamRef, queryRef],
                [queryRef, downstreamRef],
            ]),
            displayedNodeIds: new Set([UPSTREAM, QUERY, DOWNSTREAM]),
        });

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(2);
    });

    it('counts through transformations, which are not rendered as columns of their own', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, queryRef],
                [queryRef, downstreamRef],
            ]),
            displayedNodeIds: new Set([UPSTREAM, QUERY, DOWNSTREAM]),
        });

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(1);
    });

    it('counts through nodes missing from the graph', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, missingRef],
                [missingRef, downstreamRef],
            ]),
        });

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(1);
    });
});

// How hidden column lineage is shown when lineage filter nodes are rendered, in place of the
// column lineage controls
describe('column edges to lineage filter nodes', () => {
    const upstreamFilterNode = createLineageFilterNodeId(UPSTREAM, LineageDirection.Upstream);
    const downstreamFilterNode = createLineageFilterNodeId(UPSTREAM, LineageDirection.Downstream);

    /** Edges other than the upstream column -> downstream column edge every case emits. */
    function filterNodeEdges(asset: AssetOverrides, showFilterNodes = true) {
        const { columnEdges } = run(new Map(), {
            nodes: new Map([
                [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream, asset)],
                // Counts fetched, so only the column under test emits filter node edges
                [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream, {})],
            ]),
            showFilterNodes,
        });
        return Array.from(columnEdges.values()).filter((edge) => edge.targetHandle !== downstreamRef);
    }

    it('emits a tentative edge in each direction while counts are unfetched', () => {
        const edges = filterNodeEdges({ lineageCountsFetched: false });

        expect(edges).toHaveLength(2);
        expect(edges.every((edge) => edge.type === TENTATIVE_EDGE_NAME)).toBe(true);
        // Edges point away from the column, so the filter node is the target only downstream
        expect(edges.map((edge) => [edge.source, edge.target])).toEqual(
            expect.arrayContaining([
                [UPSTREAM, downstreamFilterNode],
                [upstreamFilterNode, UPSTREAM],
            ]),
        );
    });

    it('emits a solid edge once counts show more lineage than is on the graph', () => {
        const edges = filterNodeEdges({ numDownstream: 2 });

        expect(edges).toHaveLength(1);
        expect(edges[0].type).toEqual('default');
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].target).toEqual(downstreamFilterNode);
    });

    it('emits no edge once counts show all lineage is on the graph', () => {
        expect(filterNodeEdges({ numDownstream: 1 })).toHaveLength(0);
    });

    it('emits no edge when filter nodes are not rendered, as the controls show the counts', () => {
        expect(filterNodeEdges({ lineageCountsFetched: false }, false)).toHaveLength(0);
    });
});

describe('column edges through query nodes', () => {
    const QUERY = 'urn:li:query:q1';
    const queryRef = createColumnRef(QUERY, 'op1');

    function throughQueryLineage(): FineGrainedLineage {
        return lineageFromEdges([
            [upstreamRef, queryRef],
            [queryRef, downstreamRef],
        ]);
    }

    it('routes segments through a query node displayed on the graph', () => {
        const { columnEdges } = run(new Map(), {
            fineGrainedLineage: throughQueryLineage(),
            displayedNodeIds: new Set([UPSTREAM, DOWNSTREAM, QUERY]),
        });

        const edges = Array.from(columnEdges.values());
        expect(edges.map((edge) => [edge.source, edge.target])).toEqual([
            [UPSTREAM, QUERY],
            [QUERY, DOWNSTREAM],
        ]);
    });

    it('draws the edge directly when the query node is not on the graph', () => {
        // e.g. when a fine-grained edge's query differs from its table edge's, so the graph
        // never draws a node for it
        const { columnEdges } = run(new Map(), { fineGrainedLineage: throughQueryLineage() });

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0].source).toEqual(UPSTREAM);
        expect(edges[0].target).toEqual(DOWNSTREAM);
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].targetHandle).toEqual(downstreamRef);
    });
});

// Entities that read columns without columns of their own, e.g. metrics, take part in column
// lineage as entity refs: highlighted and drawn to as a whole node
describe('entity-level column lineage', () => {
    const METRIC = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,total)';
    const metricRef = createEntityRef(METRIC);

    function toMetricLineage(): FineGrainedLineage {
        return lineageFromEdges([[upstreamRef, metricRef]]);
    }

    function runToMetric(overrides: Overrides = {}) {
        return run(new Map(), {
            fineGrainedLineage: toMetricLineage(),
            nodes: new Map([
                [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream, { lineageCountsFetched: false })],
                [METRIC, { id: METRIC, urn: METRIC, type: EntityType.Metric, entity: {} } as any],
            ]),
            displayedNodeIds: new Set([UPSTREAM, METRIC]),
            ...overrides,
        });
    }

    it('draws the edge from the column to the entity node itself when the column is highlighted', () => {
        const { columnEdges, highlightedColumns } = runToMetric();

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0]).toMatchObject({ source: UPSTREAM, target: METRIC, sourceHandle: upstreamRef });
        // No handle: the edge attaches to the node's own handle rather than to a column's
        expect(edges[0].targetHandle).toBeUndefined();
        expect(highlightedColumns.get(METRIC)).toEqual(new Set([ENTITY_LEVEL_FIELD]));
    });

    it('draws the same edge when the entity is highlighted, and highlights the column it reads', () => {
        const { columnEdges, highlightedColumns } = runToMetric({ source: metricRef });

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0]).toMatchObject({ source: UPSTREAM, target: METRIC, sourceHandle: upstreamRef });
        expect(highlightedColumns.get(UPSTREAM)).toEqual(new Set([FIELD]));
    });

    it('counts the entity as a related column, as the counts fetched for the column include metrics', () => {
        const { shownRelatedColumns } = runToMetric();

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(1);
    });

    it('emits no column lineage controls or filter node edges for the entity, which has no column', () => {
        const { shownRelatedColumns, columnEdges } = runToMetric({ source: metricRef, showFilterNodes: true });

        expect(shownRelatedColumns.has(metricRef)).toBe(false);
        const filterNodeEdges = Array.from(columnEdges.values()).filter(
            (edge) => edge.source.startsWith('lf:') || edge.target.startsWith('lf:'),
        );
        // Only the upstream column's own filter node edge, from its unfetched counts
        expect(filterNodeEdges.map((edge) => edge.sourceHandle ?? edge.targetHandle)).toEqual([upstreamRef]);
    });
});

describe('entity lineage between column-like entities', () => {
    const METRIC = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,total)';
    const DERIVED = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,derived)';
    const SECOND_DERIVED = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,secondDerived)';
    const metricRef = createEntityRef(METRIC);

    function metricNode(urn: string) {
        return { id: urn, urn, type: EntityType.Metric, entity: {} } as any;
    }

    /** Column -> metric fine grained lineage, plus entity lineage onward from that metric. */
    function runFromColumn(entityEdges: [string, string][], extraNodes: [string, any][] = []) {
        const urns = [UPSTREAM, METRIC, ...entityEdges.flat()];
        return run(new Map(), {
            fineGrainedLineage: lineageFromEdges([[upstreamRef, metricRef]]),
            nodes: new Map([
                [UPSTREAM, node(UPSTREAM, undefined, { lineageCountsFetched: true })],
                [METRIC, metricNode(METRIC)],
                [DERIVED, metricNode(DERIVED)],
                [SECOND_DERIVED, metricNode(SECOND_DERIVED)],
                ...extraNodes,
            ]),
            displayedNodeIds: new Set(urns),
            adjacencyList: adjacencyFromEdges(entityEdges),
        });
    }

    it('highlights the entity edge onward from the metric the column feeds', () => {
        const { columnHighlightedEdges, highlightedColumns } = runFromColumn([[METRIC, DERIVED]]);

        expect(Array.from(columnHighlightedEdges.keys())).toEqual([createEdgeId(METRIC, DERIVED)]);
        // The stroke matches the column edges it continues, so hover and select colors agree
        expect(columnHighlightedEdges.get(createEdgeId(METRIC, DERIVED))).toEqual('red');
        expect(highlightedColumns.get(DERIVED)).toEqual(new Set([ENTITY_LEVEL_FIELD]));
    });

    it('walks a chain of metrics, as each is a hop in the same column lineage', () => {
        const { columnHighlightedEdges } = runFromColumn([
            [METRIC, DERIVED],
            [DERIVED, SECOND_DERIVED],
        ]);

        expect(new Set(columnHighlightedEdges.keys())).toEqual(
            new Set([createEdgeId(METRIC, DERIVED), createEdgeId(DERIVED, SECOND_DERIVED)]),
        );
    });

    it('highlights upstream entity edges when the hovered column is fed by a metric', () => {
        // The mirror case, which metric -> column lineage will produce: walking upstream from the
        // column reaches the metric, and carries on up the entity edge to the metric it derives from
        const { columnHighlightedEdges, highlightedColumns } = run(new Map(), {
            source: downstreamRef,
            fineGrainedLineage: lineageFromEdges([[metricRef, downstreamRef]]),
            nodes: new Map([
                [DOWNSTREAM, node(DOWNSTREAM, undefined, { lineageCountsFetched: true })],
                [METRIC, metricNode(METRIC)],
                [DERIVED, metricNode(DERIVED)],
            ]),
            displayedNodeIds: new Set([DOWNSTREAM, METRIC, DERIVED]),
            adjacencyList: adjacencyFromEdges([[DERIVED, METRIC]]),
        });

        expect(Array.from(columnHighlightedEdges.keys())).toEqual([createEdgeId(DERIVED, METRIC)]);
        expect(highlightedColumns.get(DERIVED)).toEqual(new Set([ENTITY_LEVEL_FIELD]));
    });

    it('leaves the other upstreams of a metric the column feeds alone', () => {
        // Traversal stays in the direction it is going, as it does for columns: a sibling input to
        // a downstream metric is not part of this column's lineage
        const { columnHighlightedEdges } = runFromColumn([[DERIVED, METRIC]]);

        expect(columnHighlightedEdges.size).toEqual(0);
    });

    it('stops at entities that are not column-like, e.g. a dataset a metric feeds', () => {
        const { columnHighlightedEdges, highlightedColumns } = runFromColumn(
            [[METRIC, DOWNSTREAM_OF_DOWNSTREAM]],
            [[DOWNSTREAM_OF_DOWNSTREAM, node(DOWNSTREAM_OF_DOWNSTREAM)]],
        );

        expect(columnHighlightedEdges.size).toEqual(0);
        expect(highlightedColumns.has(DOWNSTREAM_OF_DOWNSTREAM)).toBe(false);
    });

    it('leaves a metric with no column lineage untouched', () => {
        const { highlightedColumns, cllHighlightedNodes, columnEdges, columnHighlightedEdges } = run(new Map(), {
            source: metricRef,
            fineGrainedLineage: lineageFromEdges([]),
            nodes: new Map([[METRIC, metricNode(METRIC)]]),
            displayedNodeIds: new Set([METRIC]),
        });

        expect(highlightedColumns.size).toEqual(0);
        expect(cllHighlightedNodes.size).toEqual(0);
        expect(columnEdges.size).toEqual(0);
        expect(columnHighlightedEdges.size).toEqual(0);
    });

    it('adds no column edges of its own, so the drawn entity edge is the one highlighted', () => {
        const { columnEdges } = runFromColumn([[METRIC, DERIVED]]);

        // Only the column -> metric edge; the metric -> metric hop reuses the entity edge
        expect(Array.from(columnEdges.values()).map((edge) => edge.target)).toEqual([METRIC]);
    });
});

describe('getEntityRef', () => {
    const METRIC = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,total)';
    const DERIVED = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,derived)';

    const nodes = new Map<string, any>([
        [UPSTREAM, node(UPSTREAM)],
        [METRIC, { id: METRIC, urn: METRIC, type: EntityType.Metric, entity: {} }],
        [DERIVED, { id: DERIVED, urn: DERIVED, type: EntityType.Metric, entity: {} }],
    ]);

    it('returns the entity ref for a node that takes part in column lineage as a whole', () => {
        const lineage = lineageFromEdges([[upstreamRef, createEntityRef(METRIC)]]);
        expect(getEntityRef(METRIC, lineage, nodes)).toEqual(createEntityRef(METRIC));
    });

    it('returns the entity ref for a column-like entity with no fine grained lineage of its own', () => {
        // A derived metric reaches column lineage only through the metric it derives from, which is
        // an entity edge, so it is absent from the fine grained lineage map
        const lineage = lineageFromEdges([[upstreamRef, createEntityRef(METRIC)]]);
        expect(getEntityRef(DERIVED, lineage, nodes)).toEqual(createEntityRef(DERIVED));
    });

    it('returns null for a node whose column lineage is all through its columns, or no node', () => {
        expect(getEntityRef(UPSTREAM, fineGrainedLineage(), nodes)).toBeNull();
        expect(getEntityRef(null, fineGrainedLineage(), nodes)).toBeNull();
    });
});

describe('resolveColumnHighlightSource', () => {
    const METRIC = 'urn:li:metric:(urn:li:dataPlatform:snowflake,db,total)';
    const metricRef = createEntityRef(METRIC);

    it('keeps a selected column highlighted whatever node is hovered or selected', () => {
        expect(resolveColumnHighlightSource(upstreamRef, downstreamRef, metricRef, metricRef)).toEqual({
            ref: upstreamRef,
            isSelected: true,
        });
    });

    it('prefers a selected entity to hovers, as it does a selected column', () => {
        expect(resolveColumnHighlightSource(null, downstreamRef, metricRef, null)).toEqual({
            ref: metricRef,
            isSelected: true,
        });
    });

    it('prefers a hovered column to a hovered entity', () => {
        expect(resolveColumnHighlightSource(null, downstreamRef, null, metricRef)).toEqual({
            ref: downstreamRef,
            isSelected: false,
        });
    });

    it('falls back to the hovered entity, and to nothing', () => {
        expect(resolveColumnHighlightSource(null, null, null, metricRef)).toEqual({
            ref: metricRef,
            isSelected: false,
        });
        expect(resolveColumnHighlightSource(null, null, null, null)).toBeNull();
    });
});
