import { HierarchyNode, HierarchyPointNode } from '@vx/hierarchy/lib/types';
import React, { useCallback, useEffect, useState } from 'react';
import { Group } from '@vx/group';
import { LinkHorizontal } from '@vx/shape';
import debounce from 'lodash.debounce';
import { Tree } from '@vx/hierarchy';

import { NodeData, Direction, EntitySelectParams, TreeProps } from './types';
import LineageEntityNode from './LineageEntityNode';

type LineageTreeProps = {
    data: HierarchyNode<NodeData>;
    zoom: {
        transformMatrix: {
            scaleX: number;
            scaleY: number;
            translateX: number;
            translateY: number;
            skewX: number;
            skewY: number;
        };
    };
    canvasHeight: number;
    canvasWidth: number;
    onEntityClick: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    margin: TreeProps['margin'];
    direction: Direction;
};

function findMin(arr) {
    if (!arr) return Infinity;
    if (arr.length < 2) return Infinity;
    arr.sort((a, b) => {
        return a - b;
    });

    let min = arr[1] - arr[0];

    const n = arr.length;

    for (let i = 0; i < n; i++) {
        const m = arr[i + 1] - arr[i];
        if (m < min && min > 0) {
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

export default function LineageTree({
    data,
    zoom,
    margin,
    canvasWidth,
    canvasHeight,
    onEntityClick,
    onLineageExpand,
    selectedEntity,
    direction,
}: LineageTreeProps) {
    const [xCanvasScale, setXCanvasScale] = useState(1);
    const [yCanvasScale, setYCanvasScale] = useState(1);
    const [hoveredEntity, setHoveredEntity] = useState<EntitySelectParams | undefined>(undefined);

    // Need to disable exhaustive-deps because react has trouble introspecting the debounce call's dependencies
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const debouncedSetYCanvasScale = useCallback(
        debounce((newValue) => {
            setYCanvasScale(newValue);
        }, 10),
        [setYCanvasScale],
    );

    useEffect(() => {
        // as our tree height grows, we need to expand our canvas so the nodes do not become increasingly squished together
        if (data.height > xCanvasScale) {
            setXCanvasScale(data.height);
        }
    }, [data.height, xCanvasScale, setXCanvasScale]);

    // The <Tree /> component takes in the data we've prepared and lays out each node by providing it an x & y coordinate.
    // However, we need to make a few adjustments to the layout before rendering
    // TODO(gabe-lyons): Abstract the interior of <Tree />'s render into its own FC to further optimize
    return (
        <Tree<NodeData> root={data} size={[yCanvasScale * canvasHeight, xCanvasScale * canvasWidth]}>
            {(tree) => {
                const nodeByUrn: { [x: string]: HierarchyPointNode<NodeData> } = {};
                const nodesByDepth: { [x: number]: HierarchyPointNode<NodeData>[] } = {};

                // adjust node's positions
                tree.descendants().forEach((descendent) => {
                    // first, we need to flip the position of nodes who are going upstream
                    // eslint-disable-next-line  no-param-reassign
                    if (direction === Direction.Upstream) {
                        // eslint-disable-next-line  no-param-reassign
                        descendent.y *= -1;
                    }

                    // second, duplicate nodes will be created if dependencies are in a dag rather than a tree.
                    // this is expected, however the <Tree /> component will try to lay them out independently.
                    // We need to force them to be laid out in the same place so each copy's edges begin at the same source.
                    if (descendent.data.urn && !nodeByUrn[descendent.data.urn]) {
                        nodeByUrn[descendent.data.urn] = descendent;
                    } else if (descendent.data.urn) {
                        const existing = nodeByUrn[descendent.data.urn];
                        // eslint-disable-next-line  no-param-reassign
                        descendent.x = existing.x;
                        // eslint-disable-next-line  no-param-reassign
                        descendent.y = existing.y;
                    }

                    // third, we need to track clustering of nodes so we can expand the canvas horizontally
                    nodesByDepth[descendent.depth] = [...(nodesByDepth[descendent.depth] || []), descendent];
                });

                Object.keys(nodesByDepth).forEach((depth) => {
                    if (findMin(nodesByDepth[depth]?.map((entity) => entity.x)) < 90) {
                        debouncedSetYCanvasScale(yCanvasScale * 1.03);
                    }
                });

                // the layout does not always center the root node. To reverse this affect, we need to determine how far off
                // the root node is from center and re-adjust from there
                const alteredTransform = { ...zoom.transformMatrix };
                alteredTransform.translateY -= (tree.x - canvasHeight / 2 - 125) * alteredTransform.scaleX;

                const renderedEdges = new Set();
                const renderedNodes = new Set();
                return (
                    <Group transform={transformToString(alteredTransform)} top={margin?.top} left={margin?.left}>
                        {tree.links().map((link) => {
                            if (
                                renderedEdges.has(`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`)
                            ) {
                                return null;
                            }
                            renderedEdges.add(`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`);
                            return (
                                <LinkHorizontal
                                    data={link}
                                    stroke="black"
                                    strokeWidth="1"
                                    fill="none"
                                    key={`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`}
                                    data-testid={`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`}
                                />
                            );
                        })}
                        {tree.descendants().map((node) => {
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
                                    onExpandClick={onLineageExpand}
                                    direction={direction}
                                />
                            );
                        })}
                    </Group>
                );
            }}
        </Tree>
    );
}
