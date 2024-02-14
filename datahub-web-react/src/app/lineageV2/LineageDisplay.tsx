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
};

export default function LineageDisplay({ urn, type }: Props) {
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

    useFitView();

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
            }}
        >
            <LineageVisualization initialNodes={flowNodes} initialEdges={flowEdges} />
            <LineageSidebar />
        </LineageDisplayContext.Provider>
    );
}

function useFitView() {
    const { fitView } = useReactFlow();
    const { nodeVersion, displayVersion } = useContext(LineageNodesContext);

    useEffect(() => {
        const timeout = setTimeout(() => fitView({ duration: 1000 }), 100);
        return () => {
            clearTimeout(timeout);
        };
    }, [fitView, nodeVersion]);

    useEffect(() => {
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
    }, [fitView, displayVersion]);
}
