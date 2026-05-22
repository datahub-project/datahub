import { Tooltip } from 'antd';
import React from 'react';
import { EdgeLabelRenderer, EdgeProps, getSmoothStepPath } from 'reactflow';
import styled from 'styled-components';

export const AGGREGATED_LINEAGE_EDGE_NAME = 'aggregated-lineage';

/**
 * Data attached to an aggregated lineage edge between two bounding boxes (or between a
 * bounding box and a standalone neighbour node). The numbers come straight from the
 * server-side {@code AggregatedLineageResolver} buckets (Domain view) or from the
 * client-side per-DataProduct grouping (DataProduct view).
 */
export interface AggregatedLineageEdgeData {
    memberMatchCount: number;
    /** Distinct entities on the *neighbour* side that the source touches. Optional for clients
     *  that don't compute it (currently the DataProduct view rolls up client-side and only
     *  knows the asset count). */
    neighbourEntityCount?: number;
    degreeMin?: number;
    degreeMax?: number;
}

const StyledPath = styled.path`
    stroke: ${({ theme }) => theme.colors.borderHover};
    stroke-width: 2px;
    fill: none;
`;

const InteractionPath = styled.path`
    stroke-width: 15;
    stroke-opacity: 0;
    fill: none;
`;

const CountPill = styled.div<{ labelX: number; labelY: number }>`
    position: absolute;
    transform: ${({ labelX, labelY }) => `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`};

    background: ${({ theme }) => theme.colors.bgSurface};
    color: ${({ theme }) => theme.colors.text};
    border: 1px solid ${({ theme }) => theme.colors.border};
    border-radius: 12px;
    padding: 2px 8px;
    font-size: 12px;
    font-weight: 500;
    line-height: 1.2;
    white-space: nowrap;

    pointer-events: all;
`;

export function AggregatedLineageEdge({
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
}: EdgeProps<AggregatedLineageEdgeData>) {
    const [edgePath, labelX, labelY] = getSmoothStepPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    const count = data?.memberMatchCount ?? 0;
    const tooltip = buildTooltip(data);

    return (
        <>
            <StyledPath
                id={id}
                d={edgePath}
                className="react-flow__edge-path"
                markerStart={markerStart}
                markerEnd={markerEnd}
            />
            <InteractionPath d={edgePath} className="react-flow__edge-interaction" />
            <EdgeLabelRenderer>
                <Tooltip title={tooltip} placement="top">
                    <CountPill labelX={labelX} labelY={labelY}>
                        {count}
                    </CountPill>
                </Tooltip>
            </EdgeLabelRenderer>
        </>
    );
}

function buildTooltip(data?: AggregatedLineageEdgeData): string {
    if (!data) return '';
    const parts = [`${data.memberMatchCount} ${data.memberMatchCount === 1 ? 'asset' : 'assets'} contribute`];
    if (data.neighbourEntityCount !== undefined) {
        parts.push(
            `${data.neighbourEntityCount} ${data.neighbourEntityCount === 1 ? 'asset' : 'assets'} touched on the other side`,
        );
    }
    if (
        data.degreeMin !== undefined &&
        data.degreeMax !== undefined &&
        data.degreeMin !== Number.MAX_SAFE_INTEGER &&
        data.degreeMax !== Number.MIN_SAFE_INTEGER
    ) {
        if (data.degreeMin === data.degreeMax) {
            parts.push(`${data.degreeMin}-hop`);
        } else {
            parts.push(`${data.degreeMin}-${data.degreeMax} hops`);
        }
    }
    return parts.join(' · ');
}
