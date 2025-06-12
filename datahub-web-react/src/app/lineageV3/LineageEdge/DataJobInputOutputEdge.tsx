import { colors } from '@components';
import React, { useContext, useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import { EdgeLabelRenderer, EdgeProps, getSmoothStepPath } from 'reactflow';
import styled from 'styled-components';

import { DataJobInputOutputEdgeData, LINEAGE_HANDLE_OFFSET, LineageDisplayContext } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

export const DATA_JOB_INPUT_OUTPUT_EDGE_NAME = 'datajob-input-output';

const CENTER_X_OFFSET = 50;

const StyledPath = styled.path<{ isHighlighted: boolean; isColumnSelected: boolean; isManual?: boolean }>`
    ${({ isHighlighted }) => (isHighlighted ? `stroke: ${colors.gray[900]};` : '')};
    stroke-opacity: ${({ isColumnSelected }) => (isColumnSelected ? 0.5 : 1)};
    stroke-dasharray: ${({ isManual }) => (isManual ? '5,2' : 'none')};
`;

const InteractionPath = styled.path`
    stroke-width: 15;
    stroke-opacity: 0;
`;

const EdgeDetails = styled.div<{ labelX: number; labelY: number }>`
    position: absolute;
    transform: ${({ labelX, labelY }) => `translate(-50%, -50%) translate(${labelX}px, ${labelY}px);`};
    pointer-events: all;
`;

export function DataJobInputOutputEdge({
    id,
    data,
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    markerStart,
    markerEnd,
}: EdgeProps<DataJobInputOutputEdgeData>) {
    const { yOffset, direction, isManual, originalId } = data || { isManual: false, originalId: '' };

    const { selectedColumn, highlightedEdges } = useContext(LineageDisplayContext);

    const isHighlighted = useMemo(
        () => !selectedColumn && (highlightedEdges.has(id) || highlightedEdges.has(originalId)),
        [id, originalId, selectedColumn, highlightedEdges],
    );

    const intermediateX = direction === LineageDirection.Upstream ? targetX - 100 : sourceX + 100;
    const intermediateY = yOffset === undefined ? targetY : yOffset + LINEAGE_HANDLE_OFFSET;
    const [edgePathA] = getSmoothStepPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX: intermediateX,
        targetY: intermediateY,
        targetPosition,
        centerX: direction === LineageDirection.Upstream ? sourceX + CENTER_X_OFFSET : undefined,
    });
    const [edgePathB, labelX, labelY] = getSmoothStepPath({
        sourceX: intermediateX,
        sourceY: intermediateY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
        centerX: direction === LineageDirection.Downstream ? targetX - CENTER_X_OFFSET : undefined,
    });

    const [debouncedLabelPosition, setDebouncedLabelPosition] = useState({ labelX, labelY });
    useDebounce(() => setDebouncedLabelPosition({ labelX, labelY }), 10, [labelX, labelY]);

    return (
        <>
            <StyledPath
                id={id}
                d={edgePathA}
                fill="none"
                className="react-flow__edge-path"
                markerStart={markerStart}
                isHighlighted={isHighlighted}
                isColumnSelected={!!selectedColumn}
                isManual={isManual}
            />
            <StyledPath
                id={id}
                d={edgePathB}
                fill="none"
                className="react-flow__edge-path"
                markerEnd={markerEnd}
                isHighlighted={isHighlighted}
                isColumnSelected={!!selectedColumn}
                isManual={isManual}
            />
            <InteractionPath d={edgePathB} fill="none" className="react-flow__edge-interaction" />
            <EdgeLabelRenderer>
                {/* TODO: Add edge details to show edge information (on hover) */}
                <EdgeDetails labelX={debouncedLabelPosition.labelX} labelY={debouncedLabelPosition.labelY} />
            </EdgeLabelRenderer>
        </>
    );
}
