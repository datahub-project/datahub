import { HierarchyNode } from '@vx/hierarchy/lib/types';
import React, { useCallback, useEffect, useState } from 'react';
import debounce from 'lodash.debounce';
import { Tree } from '@vx/hierarchy';
import { TransformMatrix } from '@vx/zoom/lib/types';

import { NodeData, Direction, EntitySelectParams, TreeProps } from './types';
import LineageTreeNodeAndEdgeRenderer from './LineageTreeNodeAndEdgeRenderer';

type LineageTreeProps = {
    data: HierarchyNode<NodeData>;
    zoom: {
        transformMatrix: TransformMatrix;
    };
    canvasHeight: number;
    canvasWidth: number;
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    margin: TreeProps['margin'];
    direction: Direction;
};

export default function LineageTree({
    data,
    zoom,
    margin,
    canvasWidth,
    canvasHeight,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    direction,
}: LineageTreeProps) {
    const [xCanvasScale, setXCanvasScale] = useState(1);
    const [yCanvasScale, setYCanvasScale] = useState(1);

    useEffect(() => {
        setXCanvasScale(1);
        setYCanvasScale(1);
    }, [data.data.urn]);

    // Need to disable exhaustive-deps because react has trouble introspecting the debounce call's dependencies
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const debouncedSetYCanvasScale = useCallback(
        debounce((newValue) => {
            setYCanvasScale(newValue);
        }, 6),
        [setYCanvasScale],
    );

    useEffect(() => {
        // as our tree height grows, we need to expand our canvas so the nodes do not become increasingly squished together
        if (data.height > xCanvasScale) {
            setXCanvasScale(data.height);
        }
    }, [data.height, xCanvasScale, setXCanvasScale]);

    console.log('rendering tree');

    // The <Tree /> component takes in the data we've prepared and lays out each node by providing it an x & y coordinate.
    // However, we need to make a few adjustments to the layout before rendering
    // TODO(gabe-lyons): Abstract the interior of <Tree />'s render into its own FC to further optimize
    return (
        <Tree<NodeData> root={data} size={[yCanvasScale * canvasHeight, xCanvasScale * canvasWidth]}>
            {(tree) => (
                <LineageTreeNodeAndEdgeRenderer
                    tree={tree}
                    zoom={zoom}
                    margin={margin}
                    canvasHeight={canvasHeight}
                    onEntityClick={onEntityClick}
                    onEntityCenter={onEntityCenter}
                    onLineageExpand={onLineageExpand}
                    selectedEntity={selectedEntity}
                    direction={direction}
                    debouncedSetYCanvasScale={debouncedSetYCanvasScale}
                    yCanvasScale={yCanvasScale}
                    xCanvasScale={xCanvasScale}
                />
            )}
        </Tree>
    );
}
