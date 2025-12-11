/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { MetricPointProps } from '@components/components/WhiskerChart/types';

const StyledRect = styled.rect`
    cursor: pointer;
`;

const RECT_WIDTH = 16;

export default function MetricPoint({
    pointX,
    topOfWhiskerBar,
    heightOfWhiskerBar,
    overHandler,
    leaveHandler,
}: MetricPointProps) {
    return (
        <StyledRect
            x={pointX - RECT_WIDTH / 2}
            y={topOfWhiskerBar}
            width={RECT_WIDTH}
            height={heightOfWhiskerBar}
            fill="transparent"
            onMouseOver={overHandler}
            onMouseLeave={leaveHandler}
        />
    );
}
