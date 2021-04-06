import { hierarchy } from '@vx/hierarchy';
import React, { SVGProps, useMemo } from 'react';
import { useWindowSize } from '@react-hook/window-size';
import { Zoom } from '@vx/zoom';
import styled from 'styled-components';
import { Button, Card } from 'antd';
import { PlusOutlined, MinusOutlined } from '@ant-design/icons';

import { Direction, TreeProps } from './types';
import constructTree from './utils/constructTree';
import LineageTree from './LineageTree';

export const defaultMargin = { top: 10, left: 280, right: 280, bottom: 10 };

const ZoomCard = styled(Card)`
    position: absolute;
    box-shadow: 4px 4px 4px -1px grey;
    top: 145px;
    right: 20px;
`;

const ZoomButton = styled(Button)`
    display: block;
    margin-bottom: 12px;
`;

const RootSvg = styled.svg<{ isDragging: boolean } & SVGProps<SVGSVGElement>>`
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'grab')};
`;

export default function LineageViz({
    margin = defaultMargin,
    dataset,
    fetchedEntities,
    onEntityClick,
    onLineageExpand,
    selectedEntity,
}: TreeProps) {
    const downstreamData = useMemo(() => hierarchy(constructTree(dataset, fetchedEntities, Direction.Downstream)), [
        dataset,
        fetchedEntities,
    ]);
    const upstreamData = useMemo(() => hierarchy(constructTree(dataset, fetchedEntities, Direction.Upstream)), [
        dataset,
        fetchedEntities,
    ]);

    const [windowWidth, windowHeight] = useWindowSize();

    const height = windowHeight - 133;
    const width = windowWidth;
    const yMax = height - margin.top - margin.bottom;
    const xMax = (width - margin.left - margin.right) / 2;
    const initialTransform = {
        scaleX: 2 / 3,
        scaleY: 2 / 3,
        translateX: width / 2,
        translateY: 0,
        skewX: 0,
        skewY: 0,
    };
    return (
        <Zoom
            width={width}
            height={height}
            scaleXMin={1 / 8}
            scaleXMax={2}
            scaleYMin={1 / 8}
            scaleYMax={2}
            transformMatrix={initialTransform}
        >
            {(zoom) => (
                <>
                    <ZoomCard size="small">
                        <ZoomButton onClick={() => zoom.scale({ scaleX: 1.2, scaleY: 1.2 })}>
                            <PlusOutlined />
                        </ZoomButton>
                        <Button onClick={() => zoom.scale({ scaleX: 0.8, scaleY: 0.8 })}>
                            <MinusOutlined />
                        </Button>
                    </ZoomCard>
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
                                <path d="M 0 0 L 10 5 L 0 10 z" fill="#000" />
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
                                <path d="M 0 5 L 10 10 L 10 0 L 0 5 z" fill="#000" />
                            </marker>
                        </defs>
                        <rect width={width} height={height} fill="#f6f8fa" />
                        <LineageTree
                            data={upstreamData}
                            zoom={zoom}
                            onEntityClick={onEntityClick}
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
                            onLineageExpand={onLineageExpand}
                            canvasHeight={yMax}
                            canvasWidth={xMax}
                            margin={margin}
                            selectedEntity={selectedEntity}
                            direction={Direction.Downstream}
                        />
                    </RootSvg>
                </>
            )}
        </Zoom>
    );
}
