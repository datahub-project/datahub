import React from 'react';
import styled, { useTheme } from 'styled-components';

export interface AssertionProgressSummary {
    passing: number;
    failing: number;
    erroring: number;
    initializing: number;
    notRunning: number;
}

interface Props {
    summary: AssertionProgressSummary;
}

// Styled Components
const StyledProgressContainer = styled.div`
    display: flex;
    height: 4px;
    width: 100%;
    border-radius: 20px;
    overflow: hidden;
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

const StyledSegment = styled.div<{ width: number; color: string }>`
    width: ${({ width }) => `${width}%`};
    background-color: ${({ color }) => color};
    height: 100%;
    border-radius: 20px;
`;

export const AcrylAssertionProgressBar: React.FC<Props> = ({ summary }) => {
    const theme = useTheme();
    const total = summary.passing + summary.failing + summary.erroring + summary.initializing + summary.notRunning;
    if (!total) {
        return <StyledProgressContainer />;
    }
    const passingPercent = (summary.passing / total) * 100;
    const failingPercent = (summary.failing / total) * 100;
    const erroringPercent = (summary.erroring / total) * 100;
    const initializingPercent = (summary.initializing / total) * 100;
    const notRunningPercent = (summary.notRunning / total) * 100;
    return (
        <StyledProgressContainer>
            <StyledSegment width={passingPercent} color={theme.colors.iconSuccess} />
            <StyledSegment width={failingPercent} color={theme.colors.iconError} />
            <StyledSegment width={erroringPercent} color={theme.colors.iconWarning} />
            <StyledSegment width={initializingPercent} color={theme.colors.iconInformation} />
            <StyledSegment width={notRunningPercent} color={theme.colors.iconDisabled} />
        </StyledProgressContainer>
    );
};
