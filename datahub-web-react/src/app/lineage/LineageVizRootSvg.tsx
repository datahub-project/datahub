import { ProvidedZoom, TransformMatrix } from '@visx/zoom/lib/types';
import React, { SVGProps, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components/macro';

import { useEntityRegistry } from '../useEntityRegistry';
import LineageTree from './LineageTree';
import { EntityAndType, FetchedEntity, EntitySelectParams, Direction, UpdatedLineages } from './types';
import constructTree from './utils/constructTree';

type Props = {
    margin: { top: number; right: number; bottom: number; left: number };
    entityAndType?: EntityAndType | null;
    fetchedEntities: Map<string, FetchedEntity>;
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (data: EntityAndType) => void;
    selectedEntity?: EntitySelectParams;
    zoom: ProvidedZoom<any> & {
        transformMatrix: TransformMatrix;
        isDragging: boolean;
    };
    width: number;
    height: number;
};

const RootSvg = styled.svg<{ isDragging: boolean } & SVGProps<SVGSVGElement>>`
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'grab')};
    @keyframes spin {
        0% {
            transform: rotate(0deg);
        }
        100% {
            transform: rotate(359deg);
        }
    }
    .lineageExpandLoading {
        transform-box: fill-box;
        transform-origin: 50% 50%;
        animation: spin 2s linear infinite;
    }
`;

export default function LineageVizRootSvg({
    zoom,
    margin,
    entityAndType,
    fetchedEntities,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    width,
    height,
}: Props) {
    const [draggedNodes, setDraggedNodes] = useState<Record<string, { x: number; y: number }>>({});
    const [hoveredEntity, setHoveredEntity] = useState<EntitySelectParams | undefined>(undefined);
    const [isDraggingNode, setIsDraggingNode] = useState(false);
    const [updatedLineages, setUpdatedLineages] = useState<UpdatedLineages>({});

    const entityRegistry = useEntityRegistry();

    const downstreamData = useMemo(
        () => constructTree(entityAndType, fetchedEntities, Direction.Downstream, entityRegistry, updatedLineages),
        [entityAndType, fetchedEntities, entityRegistry, updatedLineages],
    );

    const upstreamData = useMemo(
        () => constructTree(entityAndType, fetchedEntities, Direction.Upstream, entityRegistry, updatedLineages),
        [entityAndType, fetchedEntities, entityRegistry, updatedLineages],
    );

    // we want to clear all the dragged nodes after recentering
    useEffect(() => {
        setDraggedNodes({});
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityAndType?.entity?.urn]);

    return (
        <>
            <RootSvg
                width={width}
                height={height}
                onMouseDown={zoom.dragStart}
                onMouseUp={zoom.dragEnd}
                onMouseMove={(e) => {
                    if (!isDraggingNode) {
                        zoom.dragMove(e);
                    }
                }}
                onTouchStart={zoom.dragStart}
                onTouchMove={(e) => {
                    if (!isDraggingNode) {
                        zoom.dragMove(e);
                    }
                }}
                onTouchEnd={zoom.dragEnd}
                isDragging={zoom.isDragging}
            >
                <defs>
                    <marker
                        id="triangle-downstream"
                        viewBox="0 0 10 10"
                        refX="10"
                        refY="5"
                        markerUnits="strokeWidth"
                        markerWidth="10"
                        markerHeight="10"
                        orient="auto"
                    >
                        <path d="M 0 0 L 10 5 L 0 10 z" fill="#BFBFBF" />
                    </marker>
                    <marker
                        id="triangle-upstream"
                        viewBox="0 0 10 10"
                        refX="0"
                        refY="5"
                        markerUnits="strokeWidth"
                        markerWidth="10"
                        markerHeight="10"
                        orient="auto"
                    >
                        <path d="M 0 5 L 10 10 L 10 0 L 0 5 z" fill="#BFBFBF" />
                    </marker>
                    <marker
                        id="triangle-downstream-highlighted"
                        viewBox="0 0 10 10"
                        refX="10"
                        refY="5"
                        markerUnits="strokeWidth"
                        markerWidth="10"
                        markerHeight="10"
                        orient="auto"
                    >
                        <path d="M 0 0 L 10 5 L 0 10 z" fill="#1890FF" />
                    </marker>
                    <marker
                        id="triangle-upstream-highlighted"
                        viewBox="0 0 10 10"
                        refX="0"
                        refY="5"
                        markerUnits="strokeWidth"
                        markerWidth="10"
                        markerHeight="10"
                        orient="auto"
                    >
                        <path d="M 0 5 L 10 10 L 10 0 L 0 5 z" fill="#1890FF" />
                    </marker>
                    <linearGradient id="gradient-Downstream" x1="1" x2="0" y1="0" y2="0">
                        <stop offset="0%" stopColor="#1890FF" />
                        <stop offset="100%" stopColor="#1890FF" stopOpacity="0" />
                    </linearGradient>
                    <linearGradient id="gradient-Upstream" x1="0" x2="1" y1="0" y2="0">
                        <stop offset="0%" stopColor="#1890FF" />
                        <stop offset="100%" stopColor="#1890FF" stopOpacity="0" />
                    </linearGradient>
                    <filter id="shadow1">
                        <feDropShadow
                            dx="0"
                            dy="0"
                            stdDeviation="4"
                            floodColor="rgba(72, 106, 108, 0.15)"
                            floodOpacity="1"
                        />
                    </filter>
                    <filter id="shadow1-selected">
                        <feDropShadow
                            dx="0"
                            dy="0"
                            stdDeviation="6"
                            floodColor="rgba(24, 144, 255, .15)"
                            floodOpacity="1"
                        />
                    </filter>
                </defs>
                <rect width={width} height={height} fill="#fafafa" />
                <LineageTree
                    upstreamData={upstreamData}
                    downstreamData={downstreamData}
                    zoom={zoom}
                    onEntityClick={onEntityClick}
                    onEntityCenter={onEntityCenter}
                    onLineageExpand={onLineageExpand}
                    margin={margin}
                    selectedEntity={selectedEntity}
                    hoveredEntity={hoveredEntity}
                    setHoveredEntity={setHoveredEntity}
                    canvasHeight={height}
                    setIsDraggingNode={setIsDraggingNode}
                    draggedNodes={draggedNodes}
                    setDraggedNodes={setDraggedNodes}
                    fetchedEntities={fetchedEntities}
                    setUpdatedLineages={setUpdatedLineages}
                />
            </RootSvg>
        </>
    );
}
