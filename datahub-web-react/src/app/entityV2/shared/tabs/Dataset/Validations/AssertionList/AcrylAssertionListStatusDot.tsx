import React from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components';
import { AssertionStatus } from '@src/types.generated';

const StyledAssertionResultDotContainer = styled.div`
    display: flex;
`;

const statusColors: Record<AssertionStatus, { backgroundColor: string; outerColor: string }> = {
    [AssertionStatus.Passing]: {
        backgroundColor: '#248F5B',
        outerColor: '#F7FBF4',
    },
    [AssertionStatus.Failing]: {
        backgroundColor: '#E54D1F',
        outerColor: '#FBF3EF',
    },
    [AssertionStatus.Error]: {
        backgroundColor: colors.yellow[500],
        outerColor: colors.yellow[50],
    },
    [AssertionStatus.Init]: {
        backgroundColor: colors.blue[500],
        outerColor: colors.blue[50],
    },
};

const defaultStatusColors = {
    backgroundColor: '#d9d9d9',
    outerColor: '#f5f5f5',
};

type Props = {
    assertionStatus?: AssertionStatus | null;
    disabled?: boolean;
    size?: number;
};

const AcrylAssertionListStatusDot = ({ assertionStatus, disabled, size = 12 }: Props) => {
    const { backgroundColor, outerColor } = assertionStatus
        ? statusColors[assertionStatus] || defaultStatusColors
        : defaultStatusColors;

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
        <StyledAssertionResultDotContainer
            className="assertion-result-dot"
            data-assertion-result-type={assertionStatus}
        >
            <span style={dotStyle} />
        </StyledAssertionResultDotContainer>
    );
};

export default AcrylAssertionListStatusDot;
