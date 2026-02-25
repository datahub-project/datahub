import React from 'react';
import styled, { useTheme } from 'styled-components';

import { AssertionResultType, AssertionRunEvent } from '@src/types.generated';

const StyledAssertionResultDotContainer = styled.div`
    display: flex;
`;

type Props = {
    run?: AssertionRunEvent;
    disabled?: boolean;
    size?: number;
};

const AcrylAssertionListStatusDot = ({ run, disabled, size = 12 }: Props) => {
    const theme = useTheme();
    const status = (run?.result?.type as AssertionResultType) || 'gray';

    const statusColors = {
        success: {
            backgroundColor: theme.colors.iconSuccess,
            outerColor: theme.colors.bgSurfaceSuccess,
        },
        failure: {
            backgroundColor: theme.colors.iconError,
            outerColor: theme.colors.bgSurfaceError,
        },
        gray: {
            backgroundColor: theme.colors.border,
            outerColor: theme.colors.bgSurface,
        },
    };

    const { backgroundColor, outerColor } = statusColors[status.toLocaleLowerCase()] || statusColors.gray;

    const dotStyle = {
        backgroundColor,
        width: `${size}px`,
        height: `${size}px`,
        borderRadius: '50%',
        boxShadow: `0 0 0 3px ${outerColor}`,
        display: 'inline-block',
        opacity: disabled ? 0.5 : 1,
    };

    return (
        <StyledAssertionResultDotContainer className="assertion-result-dot" data-assertion-result-type={status}>
            <span style={dotStyle} />
        </StyledAssertionResultDotContainer>
    );
};

export default AcrylAssertionListStatusDot;
