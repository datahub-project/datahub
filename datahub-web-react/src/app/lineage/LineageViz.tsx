import React from 'react';
import { useWindowSize } from '@react-hook/window-size';
import { Zoom } from '@visx/zoom';

import { TreeProps } from './types';
import LineageVizInsideZoom from './LineageVizInsideZoom';

export const defaultMargin = { top: 10, left: 280, right: 280, bottom: 10 };

interface Props extends TreeProps {
    refetchCenterNode: () => void;
}

export default function LineageViz({
    margin = defaultMargin,
    entityAndType,
    fetchedEntities,
    onEntityClick,
    onEntityCenter,
    onLineageExpand,
    selectedEntity,
    fineGrainedMap,
    refetchCenterNode,
}: Props) {
    const [windowWidth, windowHeight] = useWindowSize();

    const height = windowHeight - 111;
    const width = windowWidth;
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
            initialTransformMatrix={initialTransform}
        >
            {(zoom) => (
                <LineageVizInsideZoom
                    fineGrainedMap={fineGrainedMap}
                    entityAndType={entityAndType}
                    width={width}
                    height={height}
                    margin={margin}
                    onEntityClick={onEntityClick}
                    onEntityCenter={onEntityCenter}
                    onLineageExpand={onLineageExpand}
                    selectedEntity={selectedEntity}
                    zoom={zoom}
                    fetchedEntities={fetchedEntities}
                    refetchCenterNode={refetchCenterNode}
                />
            )}
        </Zoom>
    );
}
