import React, { useContext } from 'react';
import { EdgeLabelRenderer, EdgeProps, getBezierPath } from 'reactflow';
import styled from 'styled-components';
import { LineageDisplayContext, LineageEdge } from '../common';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';

export const LINEAGE_TABLE_EDGE_NAME = 'table-table';

const StyledPath = styled.path<{ isHighlighted: boolean; isColumnSelected: boolean; isManual?: boolean }>`
    ${({ isHighlighted }) => (isHighlighted ? `stroke: ${LINEAGE_COLORS.BLUE_2};` : '')};
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

export function LineageTableEdge({
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
}: EdgeProps<LineageEdge>) {
    const { selectedColumn, highlightedEdges } = useContext(LineageDisplayContext);

    const [edgePath, labelX, labelY] = getBezierPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    return (
        <>
            <StyledPath
                id={id}
                d={edgePath}
                fill="none"
                className="react-flow__edge-path"
                markerEnd={markerEnd}
                markerStart={markerStart}
                isHighlighted={!selectedColumn && highlightedEdges.has(id)}
                isColumnSelected={!!selectedColumn}
                isManual={data?.isManual}
            />
            <InteractionPath d={edgePath} fill="none" className="react-flow__edge-interaction" />
            <EdgeLabelRenderer>
                {/* TODO: Add edge details to show edge information (on hover) */}
                <EdgeDetails labelX={labelX} labelY={labelY} />
            </EdgeLabelRenderer>
        </>
    );
}
