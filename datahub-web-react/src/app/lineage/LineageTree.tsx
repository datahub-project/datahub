import React, { useContext, useEffect, useMemo, useState } from 'react';
import { TransformMatrix } from '@vx/zoom/lib/types';

import { NodeData, Direction, EntitySelectParams, TreeProps, EntityAndType, FetchedEntity } from './types';
import LineageTreeNodeAndEdgeRenderer from './LineageTreeNodeAndEdgeRenderer';
import layoutTree from './utils/layoutTree';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import useSortColumnsBySelectedField from './utils/useSortColumnsBySelectedField';
import { populateColumnsByUrn } from './utils/columnLineageUtils';

type LineageTreeProps = {
    upstreamData: NodeData;
    downstreamData: NodeData;
    zoom: {
        transformMatrix: TransformMatrix;
    };
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (data: EntityAndType) => void;
    selectedEntity?: EntitySelectParams;
    hoveredEntity?: EntitySelectParams;
    setHoveredEntity: (EntitySelectParams) => void;
    margin: TreeProps['margin'];
    direction: Direction;
    canvasHeight: number;
    setIsDraggingNode: (isDraggingNode: boolean) => void;
    draggedNodes: Record<string, { x: number; y: number }>;
    setDraggedNodes: (draggedNodes: Record<string, { x: number; y: number }>) => void;
    fetchedEntities: { [x: string]: FetchedEntity };
};

export default function LineageTree({
    upstreamData,
    downstreamData,
    zoom,
    margin,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    hoveredEntity,
    setHoveredEntity,
    direction,
    canvasHeight,
    setIsDraggingNode,
    draggedNodes,
    setDraggedNodes,
    fetchedEntities,
}: LineageTreeProps) {
    const [xCanvasScale, setXCanvasScale] = useState(1);
    const {
        expandTitles,
        showColumns,
        collapsedColumnsNodes,
        fineGrainedMap,
        visibleColumnsByUrn,
        columnsByUrn,
        setColumnsByUrn,
    } = useContext(LineageExplorerContext);

    useEffect(() => {
        populateColumnsByUrn(columnsByUrn, fetchedEntities, setColumnsByUrn);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [fetchedEntities]);

    useSortColumnsBySelectedField(fetchedEntities);

    useEffect(() => {
        setXCanvasScale(1);
    }, [upstreamData.urn]);

    let dragState: { urn: string; x: number; y: number } | undefined;

    const { nodesToRender, edgesToRender, nodesByUrn, layers } = useMemo(
        () =>
            layoutTree(
                upstreamData,
                downstreamData,
                draggedNodes,
                canvasHeight,
                expandTitles,
                showColumns,
                collapsedColumnsNodes,
                fineGrainedMap,
                visibleColumnsByUrn,
                columnsByUrn,
            ),
        [
            upstreamData,
            downstreamData,
            draggedNodes,
            canvasHeight,
            expandTitles,
            showColumns,
            collapsedColumnsNodes,
            fineGrainedMap,
            visibleColumnsByUrn,
            columnsByUrn,
        ],
    );

    const dragContinue = (event: MouseEvent) => {
        if (!dragState || !dragState.urn) {
            return;
        }

        const realY =
            (event.clientX - (dragState.x || 0)) * (1 / zoom.transformMatrix.scaleY) + nodesByUrn[dragState.urn].y;
        const realX =
            (event.clientY - (dragState.y || 0)) * (1 / zoom.transformMatrix.scaleX) + nodesByUrn[dragState.urn].x;
        setDraggedNodes({
            ...draggedNodes,
            [dragState?.urn]: { x: realX, y: realY },
        });
    };

    const stopDragging = () => {
        setIsDraggingNode(false);
        window.removeEventListener('mousemove', dragContinue, false);
        window.removeEventListener('mouseup', stopDragging, false);
    };

    const onDrag = ({ urn }, event: React.MouseEvent) => {
        const { clientX, clientY } = event;
        dragState = { urn, x: clientX, y: clientY };
        setIsDraggingNode(true);

        window.addEventListener('mousemove', dragContinue, false);
        window.addEventListener('mouseup', stopDragging, false);
    };

    useEffect(() => {
        // as our tree height grows, we need to expand our canvas so the nodes do not become increasingly squished together
        if (layers > xCanvasScale) {
            setXCanvasScale(layers);
        }
    }, [layers, xCanvasScale, setXCanvasScale]);

    return (
        <LineageTreeNodeAndEdgeRenderer
            data={downstreamData}
            onDrag={onDrag}
            nodesToRender={nodesToRender}
            edgesToRender={edgesToRender}
            nodesByUrn={nodesByUrn}
            zoom={zoom}
            margin={margin}
            onEntityClick={onEntityClick}
            onEntityCenter={onEntityCenter}
            onLineageExpand={onLineageExpand}
            selectedEntity={selectedEntity}
            hoveredEntity={hoveredEntity}
            setHoveredEntity={setHoveredEntity}
            direction={direction}
        />
    );
}
