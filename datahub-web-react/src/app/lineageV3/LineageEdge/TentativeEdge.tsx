/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import { EdgeProps, getBezierPath } from 'reactflow';
import styled from 'styled-components';

export const TENTATIVE_EDGE_NAME = 'tentative';

const StyledPath = styled.path`
    stroke-dasharray: 3, 2;
    // animation: dash-move 300ms linear infinite;
    @keyframes dash-move {
        0% {
            stroke-dashoffset: 0;
        }
        100% {
            stroke-dashoffset: -10; /* negative value moves dashes forward */
        }
    }
`;

export default function TentativeEdge({
    id,
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    markerStart,
    markerEnd,
    style,
}: EdgeProps<void>) {
    const [edgePath] = getBezierPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    return (
        <StyledPath
            id={id}
            d={edgePath}
            fill="none"
            className="react-flow__edge-path"
            markerEnd={markerEnd}
            markerStart={markerStart}
            style={style}
        />
    );
}
