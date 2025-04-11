import React from 'react';
import styled from 'styled-components';
import { MetricPointProps } from '../types';

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
