import React, { useContext } from 'react';
import { Group } from '@vx/group';
import { curveBasis } from '@vx/curve';
import { LinePath } from '@vx/shape';
import { TransformMatrix } from '@vx/zoom/lib/types';

import { NodeData, Direction, EntitySelectParams, TreeProps, VizNode, VizEdge, EntityAndType } from './types';
import LineageEntityNode from './LineageEntityNode';
import { ANTD_GRAY } from '../entity/shared/constants';
import { LineageExplorerContext } from './utils/LineageExplorerContext';

type Props = {
    data: NodeData;
    zoom: {
        transformMatrix: TransformMatrix;
    };
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (data: EntityAndType) => void;
    selectedEntity?: EntitySelectParams;
    hoveredEntity?: EntitySelectParams;
    setHoveredEntity: (EntitySelectParams) => void;
    onDrag: (params: EntitySelectParams, event: React.MouseEvent) => void;
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
    onDrag,
    direction,
    nodesToRender,
    edgesToRender,
    nodesByUrn,
}: Props) {
    const { highlightedEdges } = useContext(LineageExplorerContext);
    const isLinkHighlighted = (link) =>
        link.source.data.urn === hoveredEntity?.urn ||
        link.target.data.urn === hoveredEntity?.urn ||
        highlightedEdges.find(
            (edge) =>
                edge.sourceUrn === link.source.data.urn &&
                edge.sourceField === link.sourceField &&
                edge.targetUrn === link.target.data.urn &&
                edge.targetField === link.targetField,
        );
    return (
        <Group transform={transformToString(zoom.transformMatrix)} top={margin?.top} left={margin?.left}>
            {[
                // we want to render non-highlighted links first since svg does not support the
                // concept of a z-index
                ...edgesToRender.filter((link) => !isLinkHighlighted(link)),
                ...edgesToRender.filter(isLinkHighlighted),
            ].map((link) => {
                const isHighlighted = isLinkHighlighted(link);

                return (
                    <Group
                        key={`edge-${link.source.data.urn}${link.sourceField && `-${link.sourceField}`}-${
                            link.target.data.urn
                        }${link.targetField && `-${link.targetField}`}-${direction}`}
                    >
                        <LinePath
                            // we rotated the svg 90 degrees so we need to switch x & y for the last mile
                            x={(d) => d.y}
                            y={(d) => d.x}
                            curve={curveBasis}
                            data={link.curve}
                            stroke={isHighlighted ? '#1890FF' : ANTD_GRAY[6]}
                            strokeWidth="1"
                            markerEnd={`url(#triangle-downstream${isHighlighted ? '-highlighted' : ''})`}
                            markerStart={`url(#triangle-upstream${isHighlighted ? '-highlighted' : ''})`}
                            data-testid={`edge-${link.source.data.urn}-${link.target.data.urn}-${direction}`}
                        />
                    </Group>
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
                        onHover={(select?: EntitySelectParams) => setHoveredEntity(select)}
                        onEntityClick={onEntityClick}
                        onEntityCenter={onEntityCenter}
                        onExpandClick={onLineageExpand}
                        isCenterNode={data.urn === node.data.urn}
                        nodesToRenderByUrn={nodesByUrn}
                        onDrag={onDrag}
                    />
                );
            })}
        </Group>
    );
}
