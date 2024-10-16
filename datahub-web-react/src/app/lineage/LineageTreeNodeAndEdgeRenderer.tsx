import React, { useContext } from 'react';
import { Group } from '@visx/group';
import { TransformMatrix } from '@visx/zoom/lib/types';

import { NodeData, EntitySelectParams, TreeProps, VizNode, VizEdge, EntityAndType, UpdatedLineages } from './types';
import LineageEntityNode from './LineageEntityNode';
import LineageEntityEdge from './LineageEntityEdge';
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
    nodesToRender: VizNode[];
    edgesToRender: VizEdge[];
    nodesByUrn: Record<string, VizNode>;
    setUpdatedLineages: React.Dispatch<React.SetStateAction<UpdatedLineages>>;
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
    nodesToRender,
    edgesToRender,
    nodesByUrn,
    setUpdatedLineages,
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
            ].map((link, idx) => {
                const isHighlighted = isLinkHighlighted(link);
                const key = `edge-${idx}-${link.source.data.urn}${link.sourceField && `-${link.sourceField}`}-${
                    link.target.data.urn
                }${link.targetField && `-${link.targetField}`}-${link.target.direction}`;

                return <LineageEntityEdge edge={link} edgeKey={key} isHighlighted={!!isHighlighted} />;
            })}
            {nodesToRender.map((node, index) => {
                const isSelected = node.data.urn === selectedEntity?.urn;
                const isHovered = node.data.urn === hoveredEntity?.urn;
                const key = `node-${node.data.urn}-${node.direction}-${index}`;

                return (
                    <LineageEntityNode
                        key={key}
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
                        setUpdatedLineages={setUpdatedLineages}
                    />
                );
            })}
        </Group>
    );
}
