import React, { useContext, useEffect, useMemo, useState } from 'react';
import { useReactFlow } from 'reactflow';

import { ColumnRef, LineageDisplayContext, LineageNodesContext } from './common';
import LineageSidebar from './LineageSidebar';
import LineageVisualization from './LineageVisualization';
import useColumnHighlighting from './useColumnHighlighting';
import useProcessData from './useProcessData';
import { EntityType, LineageDirection } from '../../types.generated';
import useBulkEntityLineage from './useBulkEntityLineage';
import { LINEAGE_FILTER_NODE_NAME } from './LineageFilterNode/LineageFilterNode';
import useNodeHighlighting from './useNodeHighlighting';
import useLineageNodePreview from './useLineageNodePreview';

type Props = {
    urn: string;
    type: EntityType;
    loaded: boolean;
};

export default function LineageDisplay({ urn, type, loaded }: Props) {
    const { getNode, getEdge, setNodes, setEdges, fitView } = useReactFlow();

    const [selectedColumn, setSelectedColumn] = useState<ColumnRef | null>(null);
    const [hoveredColumn, setHoveredColumn] = useState<ColumnRef | null>(null);
    const [hoveredNode, setHoveredNode] = useState<string | null>(null);

    const { fineGrainedLineage, flowNodes, flowEdges, childMaps } = useProcessData(urn, type);
    const shownUrns = useMemo(
        () => flowNodes.filter((node) => node.type !== LINEAGE_FILTER_NODE_NAME).map((node) => node.id),
        [flowNodes],
    );
    useBulkEntityLineage(shownUrns, LineageDirection.Upstream);
    useBulkEntityLineage(shownUrns, LineageDirection.Downstream);
    useBulkEntityLineage(shownUrns, null);
    useLineageNodePreview(shownUrns);

    const { highlightedNodes, highlightedEdges } = useNodeHighlighting(hoveredNode, childMaps);

    const highlightedColumns = useColumnHighlighting(
        selectedColumn,
        hoveredColumn,
        fineGrainedLineage.indirect,
        fineGrainedLineage.direct,
    );

    useEffect(() => {
        const initialNodeMap = new Map(flowNodes.map((node) => [node.id, node]));
        const nodesToAdd = flowNodes.filter((node) => !getNode(node.id));
        const layersToRedraw = new Set<number>(
            nodesToAdd.map((node) => node?.layer).filter((layer): layer is number => !!layer),
        );
        const nodesToRedraw = flowNodes.filter((node) => node.layer && layersToRedraw.has(node.layer));
        setNodes((oldNodes) => [
            ...oldNodes
                .filter((n) => initialNodeMap.has(n.id))
                .map((n) => ({
                    ...n,
                    data: initialNodeMap.get(n.id)?.data || n.data,
                })),
            ...nodesToAdd,
            ...nodesToRedraw,
        ]);
    }, [flowNodes, getNode, setNodes, fitView]);

    useEffect(() => {
        const edgesToAdd = flowEdges.filter((edge) => !getEdge(edge.id));
        setEdges((oldEdges) => [...oldEdges, ...edgesToAdd]);
    }, [flowEdges, getEdge, setEdges]);

    useFitView(loaded);

    return (
        <LineageDisplayContext.Provider
            value={{
                hoveredNode,
                setHoveredNode,
                selectedColumn,
                setSelectedColumn,
                hoveredColumn,
                setHoveredColumn,
                highlightedNodes,
                highlightedColumns,
                highlightedEdges,
                childMaps,
                fineGrainedLineage: fineGrainedLineage.direct,
                columnQueryData: fineGrainedLineage.columnQueryData,
                numNodes: flowNodes.length,
            }}
        >
            <LineageVisualization initialNodes={flowNodes} initialEdges={flowEdges} />
            <LineageSidebar />
        </LineageDisplayContext.Provider>
    );
}

function useFitView(loaded: boolean) {
    const { fitView } = useReactFlow();
    const { nodeVersion, displayVersion } = useContext(LineageNodesContext);

    useEffect(() => {
        if (!loaded) return () => {};
        const timeout = setTimeout(() => fitView({ duration: 1000 }), 100);
        return () => {
            clearTimeout(timeout);
        };
    }, [loaded, nodeVersion, fitView]);

    useEffect(() => {
        if (!loaded) return () => {};
        const [, nodes] = displayVersion;
        const timeout = setTimeout(
            () =>
                fitView({
                    duration: 1000,
                    nodes: nodes.map((urn) => ({ id: urn })),
                    maxZoom: 2,
                }),
            100,
        );
        return () => {
            clearTimeout(timeout);
        };
    }, [loaded, displayVersion, fitView]);
}
