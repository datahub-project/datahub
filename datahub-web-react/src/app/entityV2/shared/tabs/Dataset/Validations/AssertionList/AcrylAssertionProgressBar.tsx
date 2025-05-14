import React from 'react';
import styled from 'styled-components';

export interface AssertionProgressSummary {
    passing: number;
    failing: number;
    erroring: number;
}

interface Props {
    summary: AssertionProgressSummary;
}

// Styled Components
const StyledProgressContainer = styled.div`
    display: flex;
    height: 4px;
    width: 100%;
    border-radius: 20px,
    overflow: hidden;
    background-color: #e0e0e0;
`;

const StyledSegment = styled.div<{ width: number; color: string }>`
    width: ${({ width }) => `${width}%`};
    background-color: ${({ color }) => color};
    height: 100%;
    border-radius: 20px;
`;

export const AcrylAssertionProgressBar: React.FC<Props> = ({ summary }) => {
    const total = summary.passing + summary.failing + summary.erroring;
    const passingPercent = (summary.passing / total) * 100;
    const failingPercent = (summary.failing / total) * 100;
    const erroringPercent = (summary.erroring / total) * 100;

    return (
        <StyledProgressContainer>
            <StyledSegment width={passingPercent} color="#548239" />
            <StyledSegment width={failingPercent} color="#D23939" />
            <StyledSegment width={erroringPercent} color="#EEAE09" />
        </StyledProgressContainer>
    );
};
