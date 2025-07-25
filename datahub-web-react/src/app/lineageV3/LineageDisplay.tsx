import React, { useContext, useEffect, useMemo, useState } from 'react';
import { useReactFlow } from 'reactflow';

import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import LineageSidebar from '@app/lineageV3/LineageSidebar';
import LineageVisualization from '@app/lineageV3/LineageVisualization';
import { ColumnRef, LineageDisplayContext, LineageNodesContext } from '@app/lineageV3/common';
import useBulkEntityLineage from '@app/lineageV3/queries/useBulkEntityLineage';
import useColumnHighlighting from '@app/lineageV3/useColumnHighlighting';
import { getNodePriority } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import useComputeGraph from '@app/lineageV3/useComputeGraph/useComputeGraph';
import useNodeHighlighting from '@app/lineageV3/useNodeHighlighting';

type Props = {
    initialized: boolean;
};

export default function LineageDisplay({ initialized }: Props) {
    const { getEdge, setNodes, setEdges } = useReactFlow();

    const [selectedColumn, setSelectedColumn] = useState<ColumnRef | null>(null);
    const [hoveredColumn, setHoveredColumn] = useState<ColumnRef | null>(null);
    const [hoveredNode, setHoveredNode] = useState<string | null>(null);
    const [displayedMenuNode, setDisplayedMenuNode] = useState<string | null>(null);

    const { fineGrainedLineage, flowNodes, flowEdges, resetPositions } = useComputeGraph();
    const shownUrns = useMemo(
        () => flowNodes.filter((node) => node.type !== LINEAGE_FILTER_NODE_NAME).map((node) => node.id),
        [flowNodes],
    );
    const refetchUrn = useBulkEntityLineage(shownUrns);

    const { highlightedNodes, highlightedEdges } = useNodeHighlighting(hoveredNode);

    const { cllHighlightedNodes, highlightedColumns } = useColumnHighlighting(
        selectedColumn,
        hoveredColumn,
        fineGrainedLineage.indirect,
        shownUrns,
    );

    useEffect(() => {
        const newNodeMap = new Map(flowNodes.map((node) => [node.id, node]));
        setNodes((oldNodes) => {
            const oldNodeIds = new Set(oldNodes.map((n) => n.id));
            const nodesToAdd = flowNodes.filter((n) => !oldNodeIds.has(n.id));
            const nodesToResetPosition = resetPositions ? flowNodes : nodesToAdd;
            nodesToResetPosition.forEach((n) => {
                // eslint-disable-next-line no-param-reassign
                n.data.dragged = false;
            });
            return [
                ...oldNodes
                    .filter((n) => newNodeMap.has(n.id))
                    .map((n) => ({
                        ...n,
                        position: (!n.data.dragged && newNodeMap.get(n.id)?.position) || n.position,
                        data: newNodeMap.get(n.id)?.data ?? n.data,
                        selectable: newNodeMap.get(n.id)?.selectable ?? n.selectable,
                    })),
                ...nodesToAdd.map((n) => ({ ...n, data: { ...n.data, dragged: false } })),
            ].sort((a, b) => getNodePriority(b) - getNodePriority(a));
        });
    }, [flowNodes, setNodes, resetPositions]);

    useEffect(() => setEdges(flowEdges), [flowEdges, getEdge, setEdges]);

    useFitView(initialized);

    return (
        <LineageDisplayContext.Provider
            value={{
                hoveredNode,
                setHoveredNode,
                displayedMenuNode,
                setDisplayedMenuNode,
                selectedColumn,
                setSelectedColumn,
                hoveredColumn,
                setHoveredColumn,
                highlightedNodes,
                cllHighlightedNodes,
                highlightedColumns,
                highlightedEdges,
                fineGrainedLineage: fineGrainedLineage.indirect,
                fineGrainedOperations: fineGrainedLineage.fineGrainedOperations,
                shownUrns,
                refetchUrn,
            }}
        >
            <LineageVisualization initialNodes={flowNodes} initialEdges={flowEdges} />
            <LineageSidebar />
        </LineageDisplayContext.Provider>
    );
}

function useFitView(loaded: boolean) {
    const { fitView } = useReactFlow();
    const { displayVersion } = useContext(LineageNodesContext);
    const [, displayVersionNodes] = displayVersion;

    useEffect(() => {
        if (!loaded) return () => {};
        const timeout = setTimeout(() => fitView({ duration: 1000, maxZoom: 2, padding: 0.5 }), 1000);
        return () => {
            clearTimeout(timeout);
        };
    }, [loaded, fitView]);

    useEffect(() => {
        if (!loaded || !displayVersionNodes.length) return () => {};
        const timeout = setTimeout(
            () =>
                fitView({
                    duration: 1000,
                    nodes: displayVersionNodes.map((urn) => ({ id: urn })),
                    maxZoom: 1,
                    padding: 0.2,
                }),
            100,
        );
        return () => {
            clearTimeout(timeout);
        };
    }, [loaded, displayVersionNodes, fitView]);
}
