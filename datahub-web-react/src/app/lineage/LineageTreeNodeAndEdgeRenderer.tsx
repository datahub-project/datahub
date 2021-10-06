import { HierarchyPointNode } from '@vx/hierarchy/lib/types';
import React, { useEffect, useMemo, useState } from 'react';
import { Group } from '@vx/group';
import { LinkHorizontal } from '@vx/shape';
import { TransformMatrix } from '@vx/zoom/lib/types';

import { NodeData, Direction, EntitySelectParams, TreeProps } from './types';
import LineageEntityNode from './LineageEntityNode';
import adjustVXTreeLayout from './utils/adjustVXTreeLayout';
import { ANTD_GRAY } from '../entity/shared/constants';

type Props = {
    tree: HierarchyPointNode<NodeData>;
    zoom: {
        transformMatrix: TransformMatrix;
    };
    canvasHeight: number;
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    margin: TreeProps['margin'];
    direction: Direction;
    debouncedSetYCanvasScale: (number) => void;
    yCanvasScale: number;
    xCanvasScale: number;
};

function findMin(arr) {
    if (!arr) return Infinity;
    if (arr.length < 2) return Infinity;
    arr.sort((a, b) => {
        return a - b;
    });

    let min = arr[1] - arr[0];

    const n = arr.length;

    for (let i = 0; i < n - 1; i++) {
        const m = arr[i + 1] - arr[i];
        if ((m < min && m > 0) || min === 0) {
            min = m;
        }
    }
    if (min === 0) return Infinity;

    return min; // minimum difference.
}

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
    tree,
    zoom,
    margin,
    canvasHeight,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    direction,
    debouncedSetYCanvasScale,
    yCanvasScale,
    xCanvasScale,
}: Props) {
    const [hoveredEntity, setHoveredEntity] = useState<EntitySelectParams | undefined>(undefined);

    const { nodesToRender, edgesToRender, nodesByUrn } = useMemo(() => {
        return adjustVXTreeLayout({ tree, direction });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [tree, direction, xCanvasScale, yCanvasScale]);

    useEffect(() => {
        const nodesByDepth: { [x: number]: { x: number; y: number; data: Omit<NodeData, 'children'> }[] } = {};
        nodesToRender.forEach((descendent) => {
            // we need to track clustering of nodes so we can expand the canvas horizontally
            nodesByDepth[descendent.y] = [...(nodesByDepth[descendent.y] || []), descendent];
        });

        Object.keys(nodesByDepth).forEach((depth) => {
            if (findMin(nodesByDepth[depth]?.map((entity) => entity.x)) < 130) {
                debouncedSetYCanvasScale(yCanvasScale * 1.025);
            }
        });
    }, [nodesToRender, debouncedSetYCanvasScale, yCanvasScale, xCanvasScale]);

    // the layout does not always center the root node. To reverse this affect, we need to determine how far off
    // the root node is from center and re-adjust from there
    const alteredTransform = { ...zoom.transformMatrix };
    alteredTransform.translateY -= (tree.x - canvasHeight / 2 - 125) * alteredTransform.scaleX;

    const renderedEdges = new Set();
    const renderedNodes = new Set();
    return (
        <Group transform={transformToString(alteredTransform)} top={margin?.top} left={margin?.left}>
            {edgesToRender.map((link) => {
                if (renderedEdges.has(`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`)) {
                    return null;
                }
                renderedEdges.add(`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`);
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
                if (renderedNodes.has(`node-${node.data.urn}-${direction}`)) {
                    return null;
                }
                renderedNodes.add(`node-${node.data.urn}-${direction}`);
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
                        isCenterNode={tree.data.urn === node.data.urn}
                        nodesToRenderByUrn={nodesByUrn}
                    />
                );
            })}
        </Group>
    );
}
