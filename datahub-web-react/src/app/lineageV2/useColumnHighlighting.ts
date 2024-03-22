import { useEffect, useMemo } from 'react';
import { Edge, MarkerType, Node, useReactFlow } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import { LINEAGE_COLORS } from '../entityV2/shared/constants';
import {
    COLUMN_QUERY_ID_PREFIX,
    ColumnHighlight,
    ColumnRef,
    createColumnRef,
    FetchStatus,
    FineGrainedLineage,
    HighlightedColumns,
    LineageEntity,
    parseColumnQueryRef,
    parseColumnRef,
    setDefault,
    setDifference,
} from './common';
import { LINEAGE_NODE_WIDTH } from './LineageEntityNode/useDisplayedColumns';
import {
    LINEAGE_TRANSFORMATION_NODE_NAME,
    TRANSFORMATION_NODE_SIZE,
} from './LineageTransformationNode/LineageTransformationNode';

export default function useColumnHighlighting(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    indirect: FineGrainedLineage,
    direct: FineGrainedLineage,
): HighlightedColumns {
    const { getNodes, setNodes, setEdges } = useReactFlow();

    const { highlightedColumns, queryNodes, columnEdges } = useMemo(() => {
        const nodes = getNodes();
        const nodePositions = new Map<string, [number, number]>();
        nodes.forEach((node) => {
            nodePositions.set(node.data.id, [node.position.x, node.position.y]);
        });
        return processColumnHighlights(selectedColumn, hoveredColumn, indirect, direct, nodePositions);
    }, [getNodes, selectedColumn, hoveredColumn, indirect, direct]);

    useEffect(() => {
        setNodes((oldNodes) => {
            const currentNodeIds = new Set(
                Array.from(queryNodes.keys()).map((queryRef) => parseColumnRef(queryRef)[0]),
            );
            const oldNodeIds = new Set(oldNodes.map((node) => node.id));
            const addIds = setDifference(currentNodeIds, oldNodeIds);
            return [
                ...oldNodes.filter(
                    (node) => !node.id.startsWith(COLUMN_QUERY_ID_PREFIX) || currentNodeIds.has(node.id),
                ),
                ...addIds.map((id) => queryNodes.get(createColumnRef(id, '')) as Node),
            ];
        });
    }, [queryNodes, setNodes, getNodes]);

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
    }, [columnEdges, setEdges]);

    return highlightedColumns;
}

function processColumnHighlights(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
    fineGrainedLineageDirect: FineGrainedLineage,
    nodePositions: Map<string, [number, number]>,
) {
    if (selectedColumn) {
        return computeSingleColumnHighlights(
            selectedColumn,
            fineGrainedLineage,
            nodePositions,
            LINEAGE_COLORS.PURPLE_3,
            true,
        );
    }
    return computeSingleColumnHighlights(
        hoveredColumn,
        fineGrainedLineageDirect,
        nodePositions,
        LINEAGE_COLORS.BLUE_2,
        false,
    );
}

function computeSingleColumnHighlights(
    column: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
    nodePositions: Map<string, [number, number]>,
    stroke: string,
    fromSelect: boolean, // TODO: Consider a cleaner way of implementing this...
): { highlightedColumns: HighlightedColumns; queryNodes: Map<string, Node>; columnEdges: Map<string, Edge> } {
    const highlightedColumns = new Map<string, Map<string, ColumnHighlight>>();
    const queryNodes = new Map<string, Node>();
    const columnEdges = new Map<string, Edge>();
    const seenQueryRefs = new Set<ColumnRef>();

    if (column === null) {
        return { highlightedColumns, queryNodes, columnEdges };
    }

    const [urn, field] = parseColumnRef(column);
    highlightedColumns.set(urn, new Map([[field, { fromSelect }]]));

    const lineages = {
        [LineageDirection.Downstream]: fineGrainedLineage.forward,
        [LineageDirection.Upstream]: fineGrainedLineage.backward,
    };
    Object.entries(lineages).forEach(([direction, fgl]) => {
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
            if (ref.startsWith(COLUMN_QUERY_ID_PREFIX)) {
                seenQueryRefs.add(ref);
            }
            fgl.get(ref)?.forEach((childRef) => {
                const [childUrn, childField] = parseColumnRef(childRef);
                if (!seen.has(childRef)) {
                    seen.add(childRef);
                    toVisit.push(childRef);
                }

                setDefault(highlightedColumns, childUrn, new Map()).set(childField, { fromSelect });
                addEdge(ref, childRef);
            });
        }
    });

    seenQueryRefs.forEach((queryRef) => {
        queryNodes.set(
            queryRef,
            createColumnQueryNode(queryRef, fineGrainedLineage, highlightedColumns, nodePositions),
        );
    });

    return { highlightedColumns, queryNodes, columnEdges };
}

function createColumnQueryNode(
    queryRef: ColumnRef,
    fineGrainedLineage: FineGrainedLineage,
    highlightedColumns: HighlightedColumns,
    nodePositions: Map<string, [number, number]>,
): Node<LineageEntity> {
    const upstreamPos = Array.from(fineGrainedLineage.backward.get(queryRef) || [])
        .map(parseColumnRef)
        .filter(([childUrn, field]) => highlightedColumns.get(childUrn)?.has(field))
        .map(([childUrn]) => nodePositions.get(childUrn))
        .filter((pos): pos is [number, number] => pos !== undefined);
    const downstreamPos = Array.from(fineGrainedLineage.forward.get(queryRef) || [])
        .map(parseColumnRef)
        .filter(([childUrn, field]) => highlightedColumns.get(childUrn)?.has(field))
        .map(([childUrn]) => nodePositions.get(childUrn))
        .filter((pos): pos is [number, number] => pos !== undefined);

    const maxUpstreamX = Math.max(...upstreamPos.map(([x]) => x));
    const minDownstreamX = Math.min(...downstreamPos.map(([x]) => x));

    const [sumY, total] = [...upstreamPos, ...downstreamPos].reduce(
        ([y, t], childPos) => {
            if (childPos === undefined) {
                return [y, t];
            }
            return [y + childPos[1], t + 1];
        },
        [0, 0],
    );

    const urn = parseColumnQueryRef(queryRef);
    const [id] = parseColumnRef(queryRef);
    return {
        id,
        type: LINEAGE_TRANSFORMATION_NODE_NAME,
        data: {
            id,
            urn,
            type: EntityType.Query,
            parents: new Set(),
            nonTransformationalParents: new Set(),
            fetchStatus: {
                [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
                [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
            },
        },
        position: {
            x: (maxUpstreamX + minDownstreamX) / 2 + LINEAGE_NODE_WIDTH / 2 - TRANSFORMATION_NODE_SIZE / 2,
            y: 120 + (total ? sumY / total : 0),
        },
    };
}
