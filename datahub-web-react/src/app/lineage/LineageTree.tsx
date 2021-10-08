import React, { useEffect, useMemo, useState } from 'react';
import { TransformMatrix } from '@vx/zoom/lib/types';

import { NodeData, Direction, EntitySelectParams, TreeProps } from './types';
import LineageTreeNodeAndEdgeRenderer from './LineageTreeNodeAndEdgeRenderer';
import generateTree from './utils/generateTree';

type LineageTreeProps = {
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
};

export default function LineageTree({
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
}: LineageTreeProps) {
    const [xCanvasScale, setXCanvasScale] = useState(1);

    useEffect(() => {
        setXCanvasScale(1);
    }, [data.urn]);

    const { nodesToRender, edgesToRender, nodesByUrn, layers } = useMemo(
        () => generateTree(data, direction),
        [data, direction],
    );

    useEffect(() => {
        // as our tree height grows, we need to expand our canvas so the nodes do not become increasingly squished together
        if (layers > xCanvasScale) {
            setXCanvasScale(layers);
        }
    }, [layers, xCanvasScale, setXCanvasScale]);

    return (
        <LineageTreeNodeAndEdgeRenderer
            data={data}
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
