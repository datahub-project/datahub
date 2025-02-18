import { AssertionResultType, AssertionRunEvent } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';

const StyledAssertionResultDotContainer = styled.div`
    display: flex;
`;

const statusColors = {
    success: {
        backgroundColor: '#248F5B',
        outerColor: '#F7FBF4',
    },
    failure: {
        backgroundColor: '#E54D1F',
        outerColor: '#FBF3EF',
    },
    gray: {
        backgroundColor: '#d9d9d9',
        outerColor: '#f5f5f5',
    },
};

type Props = {
    run?: AssertionRunEvent;
    disabled?: boolean;
    size?: number;
};

const AcrylAssertionListStatusDot = ({ run, disabled, size = 12 }: Props) => {
    const status = (run?.result?.type as AssertionResultType) || 'gray';
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
