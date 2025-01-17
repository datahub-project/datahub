import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import DeprecatedIcon from '../../../../../images/deprecated-status.svg?react';
import { REDESIGN_COLORS } from '../../constants';

// Styled Components
// Todo(Gabe): replace this with the @components button once it supports text buttons with hover states
const StyledButton = styled(Button)`
    padding: 4px;
    margin-top: -8px;
    color: ${REDESIGN_COLORS.LINK_GREY};
    :hover {
        color: ${REDESIGN_COLORS.LINK_GREY};
    }
`;

const FlexContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledDeprecatedIcon = styled(DeprecatedIcon)`
    color: inherit;
    path {
        fill: currentColor;
    }
    && {
        fill: currentColor;
    }
`;

type DeprecatedButtonProps = {
    onClick?: () => void;
    internalText?: string;
};

type MarkAsDeprecatedButtonContentsProps = {
    internalText?: string;
};

export const MarkAsDeprecatedButtonContents = ({ internalText }: MarkAsDeprecatedButtonContentsProps) => {
    return (
        <FlexContainer>
            <StyledDeprecatedIcon />
            {internalText || 'Mark as deprecated'}
        </FlexContainer>
    );
};

// Main Component
const MarkAsDeprecatedButton = ({ onClick, internalText }: DeprecatedButtonProps) => {
    return (
        <StyledButton onClick={onClick} type="text">
            <MarkAsDeprecatedButtonContents internalText={internalText} />
        </StyledButton>
    );
};

export default MarkAsDeprecatedButton;
