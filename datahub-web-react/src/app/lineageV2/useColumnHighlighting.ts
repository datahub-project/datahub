import { useContext, useEffect, useMemo } from 'react';
import { Edge, MarkerType, useReactFlow } from 'reactflow';
import { LineageDirection } from '@types';
import {
    ColumnRef,
    createLineageFilterNodeId,
    FineGrainedLineage,
    FineGrainedLineageMap,
    FineGrainedOperationRef,
    HighlightedColumns,
    HOVER_COLOR,
    LineageNodesContext,
    NodeContext,
    parseColumnRef,
    SELECT_COLOR,
    setDefault,
    setDifference,
} from './common';

export default function useColumnHighlighting(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
): {
    cllHighlightedNodes: Map<string, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
} {
    const { setEdges } = useReactFlow();
    const { nodes, nodeVersion } = useContext(LineageNodesContext);

    const { cllHighlightedNodes, highlightedColumns, columnEdges } = useMemo(() => {
        return processColumnHighlights(selectedColumn, hoveredColumn, fineGrainedLineage, nodes);
    }, [selectedColumn, hoveredColumn, nodes, fineGrainedLineage]);

    useEffect(() => {
        // TODO: Figure out how to only add edges once columns are rendered?
        setEdges((oldEdges) => {
            const currentEdgeIds = new Set(columnEdges.keys());
            const oldEdgeIds = new Set(oldEdges.map((edge) => edge.id));
            const addIds = setDifference(currentEdgeIds, oldEdgeIds);
            return [
                ...oldEdges
                    .filter((edge) => !edge.data?.isColumnEdge || currentEdgeIds.has(edge.id))
                    .map((edge) => columnEdges.get(edge.id) || edge),
                ...addIds.map((id) => columnEdges.get(id) as Edge),
            ];
        });
    }, [nodeVersion, columnEdges, setEdges]);

    return { cllHighlightedNodes, highlightedColumns };
}

function processColumnHighlights(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
    nodes: NodeContext['nodes'],
) {
    if (selectedColumn) {
        return computeSingleColumnHighlights(selectedColumn, fineGrainedLineage, nodes, SELECT_COLOR);
    }
    return computeSingleColumnHighlights(hoveredColumn, fineGrainedLineage, nodes, HOVER_COLOR);
}

function computeSingleColumnHighlights(
    column: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
    nodes: NodeContext['nodes'],
    stroke: string,
): {
    cllHighlightedNodes: Map<string, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
    columnEdges: Map<string, Edge>;
} {
    const cllHighlightedNodes = new Map<string, Set<FineGrainedOperationRef> | null>();
    const highlightedColumns = new Map<string, Set<string>>();
    const columnEdges = new Map<string, Edge>();

    if (column === null) {
        return { cllHighlightedNodes, highlightedColumns, columnEdges };
    }

    const [urn, field] = parseColumnRef(column);
    cllHighlightedNodes.set(urn, null);
    highlightedColumns.set(urn, new Set([field]));

    const lineages: Array<[LineageDirection, FineGrainedLineageMap]> = [
        [LineageDirection.Downstream, fineGrainedLineage.downstream],
        [LineageDirection.Upstream, fineGrainedLineage.upstream],
    ];
    lineages.forEach(([direction, fgl]) => {
        function addEdge(ref: ColumnRef, childRef: ColumnRef) {
            const fromRef = direction === LineageDirection.Downstream ? ref : childRef;
            const toRef = direction === LineageDirection.Downstream ? childRef : ref;
            const [fromUrn, fromField] = parseColumnRef(fromRef);
            const [toUrn, toField] = parseColumnRef(toRef);
            const id = `${fromRef}-${toRef}`;
            columnEdges.set(id, {
                id,
                source: fromUrn,
                target: toUrn,
                sourceHandle: fromField ? fromRef : undefined,
                targetHandle: toField ? toRef : undefined,
                type: 'default',
                markerEnd: { type: MarkerType.ArrowClosed },
                style: { stroke, strokeWidth: 1.25 },
                data: { isColumnEdge: true }, // Used to hide column edges
            });
        }

        const seen = new Set<ColumnRef>();
        const toVisit = [column];
        while (toVisit.length) {
            const ref = toVisit.pop();
            if (ref === undefined) {
                break;
            }
            const filterNodeEdge = addEdgeToLineageFilterNode(ref, direction, fgl, nodes);
            if (filterNodeEdge) {
                addEdge(ref, filterNodeEdge);
            }

            fgl.get(ref)?.forEach((fineGrainedOperationRef, childRef) => {
                const [childUrn, childField] = parseColumnRef(childRef);
                if (!seen.has(childRef)) {
                    seen.add(childRef);
                    toVisit.push(childRef);
                }

                const queryRefsOnChild = setDefault(cllHighlightedNodes, childUrn, null);
                if (fineGrainedOperationRef) {
                    if (queryRefsOnChild === null) {
                        cllHighlightedNodes.set(childUrn, new Set());
                    }
                    cllHighlightedNodes.get(childUrn)?.add(fineGrainedOperationRef);
                }
                setDefault(highlightedColumns, childUrn, new Set()).add(childField);
                addEdge(ref, childRef);
            });
        }
    });

    return { cllHighlightedNodes, highlightedColumns, columnEdges };
}

function addEdgeToLineageFilterNode(
    ref: ColumnRef,
    direction: LineageDirection,
    fgl: FineGrainedLineageMap,
    nodes: NodeContext['nodes'],
): ColumnRef | null {
    const [urn, field] = parseColumnRef(ref);
    const entity = nodes.get(urn)?.entity;
    const lineageAsset = entity?.lineageAssets?.find((asset) => asset.name === field);
    const cachedNumRelated =
        direction === LineageDirection.Downstream ? lineageAsset?.numDownstream : lineageAsset?.numUpstream;

    if ((cachedNumRelated || 0) <= (fgl.get(ref)?.size || 0)) return null;
    return createLineageFilterNodeId(urn, direction);
}
