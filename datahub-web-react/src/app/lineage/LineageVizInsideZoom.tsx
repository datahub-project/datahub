import React, { SVGProps, useEffect, useMemo } from 'react';
import { hierarchy } from '@vx/hierarchy';
import { PlusOutlined, MinusOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button } from 'antd';
import { ProvidedZoom, TransformMatrix } from '@vx/zoom/lib/types';

import LineageTree from './LineageTree';
import constructTree from './utils/constructTree';
import { Direction, EntityAndType, EntitySelectParams, FetchedEntity } from './types';
import { useEntityRegistry } from '../useEntityRegistry';

const ZoomContainer = styled.div`
    position: relative;
`;

const ZoomControls = styled.div`
    position: absolute;
    top: 20px;
    right: 20px;
`;

const ZoomButton = styled(Button)`
    display: block;
    margin-bottom: 12px;
`;

const RootSvg = styled.svg<{ isDragging: boolean } & SVGProps<SVGSVGElement>>`
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'grab')};
`;

type Props = {
    margin: { top: number; right: number; bottom: number; left: number };
    entityAndType?: EntityAndType | null;
    fetchedEntities: { [x: string]: FetchedEntity };
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    zoom: ProvidedZoom & {
        transformMatrix: TransformMatrix;
        isDragging: boolean;
    };
    width: number;
    height: number;
};

export default function LineageVizInsideZoom({
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
    const entityRegistry = useEntityRegistry();
    const yMax = height - margin?.top - margin?.bottom;
    const xMax = (width - margin?.left - margin?.right) / 2;

    console.log('started constructing trees');

    const downstreamData = useMemo(
        () => hierarchy(constructTree(entityAndType, fetchedEntities, Direction.Downstream, entityRegistry)),
        [entityAndType, fetchedEntities, entityRegistry],
    );
    const upstreamData = useMemo(
        () => hierarchy(constructTree(entityAndType, fetchedEntities, Direction.Upstream, entityRegistry)),
        [entityAndType, fetchedEntities, entityRegistry],
    );

    console.log('finished constructing trees');

    useEffect(() => {
        zoom.setTransformMatrix({ ...zoom.transformMatrix, translateY: 0, translateX: width / 2 });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityAndType?.entity?.urn]);
    return (
        <ZoomContainer>
            <ZoomControls>
                <ZoomButton onClick={() => zoom.scale({ scaleX: 1.2, scaleY: 1.2 })}>
                    <PlusOutlined />
                </ZoomButton>
                <Button onClick={() => zoom.scale({ scaleX: 0.8, scaleY: 0.8 })}>
                    <MinusOutlined />
                </Button>
            </ZoomControls>
            <RootSvg
                width={width}
                height={height}
                onMouseDown={zoom.dragStart}
                onMouseUp={zoom.dragEnd}
                onMouseMove={zoom.dragMove}
                onTouchStart={zoom.dragStart}
                onTouchMove={zoom.dragMove}
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
                    <linearGradient id="gradient-Downstream" x1="1" x2="0" y1="0" y2="0">
                        <stop offset="0%" stopColor="#BFBFBF" />
                        <stop offset="100%" stopColor="#BFBFBF" stopOpacity="0" />
                    </linearGradient>
                    <linearGradient id="gradient-Upstream" x1="0" x2="1" y1="0" y2="0">
                        <stop offset="0%" stopColor="#BFBFBF" />
                        <stop offset="100%" stopColor="#BFBFBF" stopOpacity="0" />
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
                    data={upstreamData}
                    zoom={zoom}
                    onEntityClick={onEntityClick}
                    onEntityCenter={onEntityCenter}
                    onLineageExpand={onLineageExpand}
                    canvasHeight={yMax}
                    canvasWidth={xMax}
                    margin={margin}
                    selectedEntity={selectedEntity}
                    direction={Direction.Upstream}
                />
                <LineageTree
                    data={downstreamData}
                    zoom={zoom}
                    onEntityClick={onEntityClick}
                    onEntityCenter={onEntityCenter}
                    onLineageExpand={onLineageExpand}
                    canvasHeight={yMax}
                    canvasWidth={xMax}
                    margin={margin}
                    selectedEntity={selectedEntity}
                    direction={Direction.Downstream}
                />
            </RootSvg>
        </ZoomContainer>
    );
}
