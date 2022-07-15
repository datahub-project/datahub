import React, { SVGProps, useEffect, useMemo, useState } from 'react';
import { PlusOutlined, MinusOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button, Switch, Tooltip } from 'antd';
import { ProvidedZoom, TransformMatrix } from '@vx/zoom/lib/types';
import { useHistory, useLocation } from 'react-router-dom';

import LineageTree from './LineageTree';
import constructTree from './utils/constructTree';
import { Direction, EntityAndType, EntitySelectParams, FetchedEntity } from './types';
import { useEntityRegistry } from '../useEntityRegistry';
import { ANTD_GRAY } from '../entity/shared/constants';
import { LineageExplorerContext } from './utils/LineageExplorerContext';
import { useIsSeparateSiblingsMode } from '../entity/shared/siblingUtils';
import { navigateToLineageUrl } from './utils/navigateToLineageUrl';

const ZoomContainer = styled.div`
    position: relative;
`;

const ZoomControls = styled.div`
    position: absolute;
    top: 20px;
    right: 20px;
`;

const DisplayControls = styled.div`
    padding: 8px;
    position: absolute;
    bottom: 30px;
    right: 20px;
    background-color: white;
    border: 1px solid ${ANTD_GRAY[4.5]};
    border-radius: 5px;
    box-shadow: 0px 0px 4px 0px #0000001a;
`;

const ControlsSwitch = styled(Switch)`
    margin-top: 12px;
    margin-right: 8px;
`;

const ZoomButton = styled(Button)`
    display: block;
    margin-bottom: 12px;
`;

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

const ControlLabel = styled.span`
    vertical-align: sub;
`;

type Props = {
    margin: { top: number; right: number; bottom: number; left: number };
    entityAndType?: EntityAndType | null;
    fetchedEntities: { [x: string]: FetchedEntity };
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (data: EntityAndType) => void;
    selectedEntity?: EntitySelectParams;
    zoom: ProvidedZoom & {
        transformMatrix: TransformMatrix;
        isDragging: boolean;
    };
    width: number;
    height: number;
};

const HelpIcon = styled(QuestionCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    padding-left: 4px;
`;

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
    const [draggedNodes, setDraggedNodes] = useState<Record<string, { x: number; y: number }>>({});
    const history = useHistory();
    const location = useLocation();

    const [hoveredEntity, setHoveredEntity] = useState<EntitySelectParams | undefined>(undefined);
    const [isDraggingNode, setIsDraggingNode] = useState(false);
    const [showExpandedTitles, setShowExpandedTitles] = useState(false);
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const entityRegistry = useEntityRegistry();

    const downstreamData = useMemo(
        () => constructTree(entityAndType, fetchedEntities, Direction.Downstream, entityRegistry),
        [entityAndType, fetchedEntities, entityRegistry],
    );

    const upstreamData = useMemo(
        () => constructTree(entityAndType, fetchedEntities, Direction.Upstream, entityRegistry),
        [entityAndType, fetchedEntities, entityRegistry],
    );

    useEffect(() => {
        zoom.setTransformMatrix({ ...zoom.transformMatrix, translateY: 0, translateX: width / 2 });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityAndType?.entity?.urn]);

    // we want to clear all the dragged nodes after recentering
    useEffect(() => {
        setDraggedNodes({});
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [entityAndType?.entity?.urn]);

    return (
        <LineageExplorerContext.Provider value={{ expandTitles: showExpandedTitles }}>
            <ZoomContainer>
                <ZoomControls>
                    <ZoomButton onClick={() => zoom.scale({ scaleX: 1.2, scaleY: 1.2 })}>
                        <PlusOutlined />
                    </ZoomButton>
                    <Button onClick={() => zoom.scale({ scaleX: 0.8, scaleY: 0.8 })}>
                        <MinusOutlined />
                    </Button>
                </ZoomControls>
                <DisplayControls>
                    <div>Controls</div>
                    <div>
                        <ControlsSwitch
                            checked={showExpandedTitles}
                            onChange={(checked) => setShowExpandedTitles(checked)}
                        />{' '}
                        <ControlLabel>Show Full Titles</ControlLabel>
                    </div>
                    <div>
                        <ControlsSwitch
                            data-testid="compress-lineage-toggle"
                            checked={!isHideSiblingMode}
                            onChange={(checked) => {
                                navigateToLineageUrl({
                                    location,
                                    history,
                                    isLineageMode: true,
                                    isHideSiblingMode: !checked,
                                });
                            }}
                        />{' '}
                        <ControlLabel>
                            Compress Lineage{' '}
                            <Tooltip title="Collapses related entities into a single lineage node" placement="topRight">
                                <HelpIcon />
                            </Tooltip>
                        </ControlLabel>
                    </div>
                </DisplayControls>
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
                        data={upstreamData}
                        zoom={zoom}
                        onEntityClick={onEntityClick}
                        onEntityCenter={onEntityCenter}
                        onLineageExpand={onLineageExpand}
                        margin={margin}
                        selectedEntity={selectedEntity}
                        hoveredEntity={hoveredEntity}
                        setHoveredEntity={setHoveredEntity}
                        direction={Direction.Upstream}
                        canvasHeight={height}
                        setIsDraggingNode={setIsDraggingNode}
                        draggedNodes={draggedNodes}
                        setDraggedNodes={setDraggedNodes}
                    />
                    <LineageTree
                        data={downstreamData}
                        zoom={zoom}
                        onEntityClick={onEntityClick}
                        onEntityCenter={onEntityCenter}
                        onLineageExpand={onLineageExpand}
                        margin={margin}
                        selectedEntity={selectedEntity}
                        hoveredEntity={hoveredEntity}
                        setHoveredEntity={setHoveredEntity}
                        direction={Direction.Downstream}
                        canvasHeight={height}
                        setIsDraggingNode={setIsDraggingNode}
                        draggedNodes={draggedNodes}
                        setDraggedNodes={setDraggedNodes}
                    />
                </RootSvg>
            </ZoomContainer>
        </LineageExplorerContext.Provider>
    );
}
