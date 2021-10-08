import React from 'react';
import { Group } from '@vx/group';
import { LinkHorizontal } from '@vx/shape';
import { TransformMatrix } from '@vx/zoom/lib/types';

import { NodeData, Direction, EntitySelectParams, TreeProps, VizNode, VizEdge } from './types';
import LineageEntityNode from './LineageEntityNode';
import { ANTD_GRAY } from '../entity/shared/constants';

type Props = {
    data: NodeData;
    zoom: {
        transformMatrix: TransformMatrix;
    };
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    hoveredEntity?: EntitySelectParams;
    setHoveredEntity: (EntitySelectParams) => void;
    margin: TreeProps['margin'];
    direction: Direction;
    nodesToRender: VizNode[];
    edgesToRender: VizEdge[];
    nodesByUrn: Record<string, VizNode>;
};

function transformToString(transform: {
    scaleX: number;
    scaleY: number;
    translateX: number;
    translateY: number;
    skewX: number;
    skewY: number;
}): string {
    return `matrix(${transform.scaleX}, ${transform.skewX}, ${transform.skewY}, ${transform.scaleY}, ${transform.translateX}, ${transform.translateY})`;
}

export default function LineageTreeNodeAndEdgeRenderer({
    data,
    zoom,
    margin,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    hoveredEntity,
    setHoveredEntity,
    direction,
    nodesToRender,
    edgesToRender,
    nodesByUrn,
}: Props) {
    return (
        <Group transform={transformToString(zoom.transformMatrix)} top={margin?.top} left={margin?.left}>
            {edgesToRender.map((link) => {
                return (
                    <LinkHorizontal
                        data={link}
                        stroke={
                            link.source.data.urn === hoveredEntity?.urn || link.target.data.urn === hoveredEntity?.urn
                                ? '#1890FF'
                                : ANTD_GRAY[6]
                        }
                        strokeWidth="1"
                        fill="none"
                        key={`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`}
                        data-testid={`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`}
                        markerEnd="url(#triangle-downstream)"
                        markerStart="url(#triangle-upstream)"
                    />
                );
            })}
            {nodesToRender.map((node) => {
                const isSelected = node.data.urn === selectedEntity?.urn;
                const isHovered = node.data.urn === hoveredEntity?.urn;
                return (
                    <LineageEntityNode
                        key={`node-${node.data.urn}-${direction}`}
                        node={node}
                        isSelected={isSelected}
                        isHovered={isHovered}
                        onHover={(select: EntitySelectParams) => setHoveredEntity(select)}
                        onEntityClick={onEntityClick}
                        onEntityCenter={onEntityCenter}
                        onExpandClick={onLineageExpand}
                        direction={direction}
                        isCenterNode={data.urn === node.data.urn}
                        nodesToRenderByUrn={nodesByUrn}
                    />
                );
            })}
        </Group>
    );
}
